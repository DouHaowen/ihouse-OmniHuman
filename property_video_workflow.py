"""
房源实拍视频成片工作流。

这个模块只处理“已有实拍视频 + 手写解说文案”的包装成片，不参与现有
数字人/素材段生产链路。
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Callable, Optional

import requests

from video_composer import (
    SUBTITLE_TEMPLATE_STYLES,
    WHISPER_LANGUAGE_MAP,
    WhisperModel,
    _format_srt_timestamp,
    _get_audio_duration,
    _word_timestamps_for_audio,
)


PROPERTY_VIDEO_EXTENSIONS = {".mp4", ".mov", ".m4v", ".webm"}
OUTPUT_WIDTH = 1080
OUTPUT_HEIGHT = 1920
OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions"
PROPERTY_AUDIO_TOLERANCE_SECONDS = 3.0
PROPERTY_TIMELINE_AUDIO_TOLERANCE_SECONDS = 8.0
PROPERTY_SCRIPT_CALIBRATION_ATTEMPTS = 4
PROPERTY_MAX_OPENAI_TOKENS = 8192
PROPERTY_SUBTITLE_MAX_CHARS = 18
PROPERTY_SUBTITLE_MIN_SECONDS = 0.65
PROPERTY_TIMELINE_MIN_SEGMENT_SECONDS = 2.8
PROPERTY_SUBTITLE_SILENT_RE = re.compile(r"[\s，,。！？!?；;、：:（）()《》<>【】\[\]“”\"'‘’…—\-]+")


def _script_visible_length(text: str) -> int:
    return len(re.sub(r"\s+", "", text or ""))


def _target_script_length_for_duration(script_text: str, target_duration: float, actual_duration: float) -> tuple[int, int, int]:
    current_chars = max(_script_visible_length(script_text), 1)
    if actual_duration <= 0:
        estimated_chars = int(target_duration * 4.2)
    else:
        # Use the measured TTS speed for this exact voice/speed instead of a fixed
        # words-per-second guess. This makes long property videos much less likely
        # to under-expand after calibration.
        estimated_chars = int(round(current_chars * (target_duration / actual_duration)))
    lower = max(20, int(estimated_chars * 0.94))
    upper = max(lower + 10, int(estimated_chars * 1.06))
    return current_chars, lower, upper


def _spoken_text_length(text: str) -> int:
    return len(PROPERTY_SUBTITLE_SILENT_RE.sub("", text or ""))


def _run(cmd: list[str]) -> None:
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        message = (result.stderr or result.stdout or "ffmpeg failed").strip()
        lines = [line for line in message.splitlines() if line.strip()]
        important = [
            line
            for line in lines
            if any(token in line.lower() for token in ("error", "invalid", "failed", "unable", "no such", "not found", "cannot"))
        ]
        compact = "\n".join((important or lines[-8:])[-8:])
        raise RuntimeError(compact or message[:1000])


def _ffprobe_duration(path: Path) -> float:
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "quiet",
            "-show_entries",
            "format=duration",
            "-of",
            "csv=p=0",
            str(path),
        ],
        capture_output=True,
        text=True,
    )
    try:
        return max(0.0, float(result.stdout.strip()))
    except (TypeError, ValueError):
        return 0.0


def _split_script_for_subtitles(script_text: str) -> list[str]:
    text = re.sub(r"\s+", " ", (script_text or "").strip())
    if not text:
        return []
    sentences = [part.strip() for part in re.split(r"(?<=[。！？!?；;，,])\s*", text) if part.strip()]
    chunks: list[str] = []
    for sentence in sentences or [text]:
        if _spoken_text_length(sentence) <= PROPERTY_SUBTITLE_MAX_CHARS:
            chunks.append(sentence)
            continue
        pieces = [part.strip() for part in re.split(r"(?<=[，,、])\s*", sentence) if part.strip()]
        for piece in pieces or [sentence]:
            if _spoken_text_length(piece) <= PROPERTY_SUBTITLE_MAX_CHARS:
                chunks.append(piece)
                continue
            buffer = ""
            for char in piece:
                buffer += char
                if _spoken_text_length(buffer) >= PROPERTY_SUBTITLE_MAX_CHARS and char in "，,、的了和与及在是也都":
                    chunks.append(buffer.strip())
                    buffer = ""
            if buffer.strip():
                if chunks and _spoken_text_length(buffer) <= 3:
                    chunks[-1] = f"{chunks[-1]}{buffer.strip()}"
                else:
                    chunks.append(buffer.strip())
    return [chunk for chunk in chunks if chunk.strip()] or [text]


def _format_subtitle_chunk(chunk: str, max_line_chars: int = PROPERTY_SUBTITLE_MAX_CHARS) -> str:
    chunk = (chunk or "").strip()
    if _spoken_text_length(chunk) <= max_line_chars:
        return chunk
    return chunk


def _map_property_chunks_to_word_timeline(
    chunks: list[str],
    words: list[tuple[float, float, str]],
    audio_duration: float,
) -> list[tuple[float, float, str]]:
    if not chunks or not words:
        return []

    anchors: list[tuple[int, float]] = [(0, float(words[0][0]))]
    spoken_pos = 0
    for word_start, word_end, word_text in words:
        spoken = PROPERTY_SUBTITLE_SILENT_RE.sub("", str(word_text or ""))
        char_count = max(len(spoken), 1)
        start = float(word_start)
        end = float(word_end)
        for offset in range(char_count):
            anchors.append((spoken_pos + offset, start + (end - start) * offset / char_count))
        spoken_pos += char_count
    anchors.append((spoken_pos, float(words[-1][1])))
    if spoken_pos <= 0:
        return []

    total_script_spoken = sum(max(_spoken_text_length(chunk), 0) for chunk in chunks)
    if total_script_spoken <= 0:
        return []

    def time_at_script_pos(pos: int) -> float:
        mapped = pos / total_script_spoken * spoken_pos
        previous = anchors[0]
        for anchor in anchors:
            if anchor[0] >= mapped:
                next_anchor = anchor
                break
            previous = anchor
        else:
            next_anchor = anchors[-1]
        span = next_anchor[0] - previous[0]
        if span <= 0:
            return previous[1]
        ratio = (mapped - previous[0]) / span
        return previous[1] + ratio * (next_anchor[1] - previous[1])

    rows: list[tuple[float, float, str]] = []
    cursor = 0
    previous_end = 0.0
    for index, chunk in enumerate(chunks):
        chunk_len = max(_spoken_text_length(chunk), 1)
        start = time_at_script_pos(cursor)
        cursor += chunk_len
        end = time_at_script_pos(cursor)
        if index == len(chunks) - 1:
            end = max(end, float(words[-1][1]))
        start = max(0.0, min(start, audio_duration))
        end = max(start + PROPERTY_SUBTITLE_MIN_SECONDS, min(end, audio_duration + 0.02))
        if rows and start < previous_end:
            start = previous_end
        if rows and end <= start:
            end = min(audio_duration + 0.02, start + PROPERTY_SUBTITLE_MIN_SECONDS)
        rows.append((start, end, chunk))
        previous_end = end
    return rows


def _write_property_subtitles(script_text: str, audio_duration: float, output_path: Path, audio_path: Path | None = None, target_market: str = "cn") -> None:
    chunks = _split_script_for_subtitles(script_text)
    if not chunks:
        output_path.write_text("", encoding="utf-8")
        return
    timed_chunks: list[tuple[float, float, str]] = []
    if audio_path and audio_path.exists() and WhisperModel is not None:
        try:
            language = WHISPER_LANGUAGE_MAP.get(target_market or "cn", "zh")
            words = _word_timestamps_for_audio(audio_path, language)
            timed_chunks = _map_property_chunks_to_word_timeline(chunks, words, audio_duration)
        except Exception:
            timed_chunks = []

    if not timed_chunks:
        total_chars = sum(max(len(chunk), 1) for chunk in chunks)
        cursor = 0.0
        for index, chunk in enumerate(chunks, start=1):
            ratio = max(len(chunk), 1) / max(total_chars, 1)
            start = cursor
            end = audio_duration if index == len(chunks) else cursor + max(1.05, audio_duration * ratio)
            if end <= start:
                end = start + 0.8
            timed_chunks.append((start, end, chunk))
            cursor = end

    rows: list[str] = []
    previous_end = 0.0
    for index, (start, end, chunk) in enumerate(timed_chunks, start=1):
        start = max(previous_end, start)
        end = min(max(end, start + PROPERTY_SUBTITLE_MIN_SECONDS), audio_duration + 0.02)
        rows.extend(
            [
                str(index),
                f"{_format_srt_timestamp(start)} --> {_format_srt_timestamp(end)}",
                _format_subtitle_chunk(chunk),
                "",
            ]
        )
        previous_end = end
    output_path.write_text("\n".join(rows).strip() + "\n", encoding="utf-8")


def _write_property_timeline_subtitles(
    segments: list[dict],
    output_path: Path,
    target_market: str = "cn",
) -> None:
    rows: list[str] = []
    subtitle_index = 1
    previous_end = 0.0
    for segment in segments:
        script = str(segment.get("script") or "").strip()
        audio_path_value = str(segment.get("audio_path") or "")
        audio_path = Path(audio_path_value) if audio_path_value else None
        audio_duration = float(segment.get("audio_duration") or 0.0)
        offset = float(segment.get("audio_offset") or 0.0)
        if not script or audio_duration <= 0:
            continue
        chunks = _split_script_for_subtitles(script)
        timed_chunks: list[tuple[float, float, str]] = []
        if audio_path and audio_path.exists() and WhisperModel is not None:
            try:
                language = WHISPER_LANGUAGE_MAP.get(target_market or "cn", "zh")
                words = _word_timestamps_for_audio(audio_path, language)
                timed_chunks = _map_property_chunks_to_word_timeline(chunks, words, audio_duration)
            except Exception:
                timed_chunks = []
        if not timed_chunks:
            total_chars = sum(max(_spoken_text_length(chunk), 1) for chunk in chunks)
            cursor = 0.0
            for index, chunk in enumerate(chunks, start=1):
                ratio = max(_spoken_text_length(chunk), 1) / max(total_chars, 1)
                start = cursor
                end = audio_duration if index == len(chunks) else cursor + max(0.75, audio_duration * ratio)
                timed_chunks.append((start, end, chunk))
                cursor = end
        for start, end, chunk in timed_chunks:
            absolute_start = max(previous_end, offset + start)
            absolute_end = max(absolute_start + PROPERTY_SUBTITLE_MIN_SECONDS, offset + end)
            rows.extend(
                [
                    str(subtitle_index),
                    f"{_format_srt_timestamp(absolute_start)} --> {_format_srt_timestamp(absolute_end)}",
                    _format_subtitle_chunk(chunk),
                    "",
                ]
            )
            subtitle_index += 1
            previous_end = absolute_end
    output_path.write_text("\n".join(rows).strip() + ("\n" if rows else ""), encoding="utf-8")


def _write_property_timeline_subtitles_fallback(
    segments: list[dict],
    output_path: Path,
) -> None:
    rows: list[str] = []
    subtitle_index = 1
    previous_end = 0.0
    for segment in segments:
        script = str(segment.get("script") or "").strip()
        duration = float(segment.get("audio_duration") or segment.get("target_duration") or 0.0)
        offset = float(segment.get("audio_offset") or previous_end)
        if not script or duration <= 0:
            continue
        chunks = _split_script_for_subtitles(script)
        total_chars = sum(max(_spoken_text_length(chunk), 1) for chunk in chunks)
        cursor = 0.0
        for index, chunk in enumerate(chunks, start=1):
            ratio = max(_spoken_text_length(chunk), 1) / max(total_chars, 1)
            start = max(previous_end, offset + cursor)
            end = offset + (duration if index == len(chunks) else cursor + max(0.75, duration * ratio))
            end = max(start + PROPERTY_SUBTITLE_MIN_SECONDS, end)
            rows.extend(
                [
                    str(subtitle_index),
                    f"{_format_srt_timestamp(start)} --> {_format_srt_timestamp(end)}",
                    _format_subtitle_chunk(chunk),
                    "",
                ]
            )
            subtitle_index += 1
            previous_end = end
            cursor = max(0.0, end - offset)
    output_path.write_text("\n".join(rows).strip() + ("\n" if rows else ""), encoding="utf-8")


def _property_subtitle_filter(subtitle_path: Path) -> str:
    escaped = subtitle_path.as_posix().replace("\\", "/").replace(":", r"\:").replace("'", r"\'")
    base_style = SUBTITLE_TEMPLATE_STYLES.get("classic") or SUBTITLE_TEMPLATE_STYLES["classic"]
    style_config = {
        **base_style,
        "font": "Noto Sans CJK SC",
        "size": 16,
        "primary": "&H00FFFFFF",
        "outline": "&H0013202C",
        "back": "&H00000000",
        "border_style": 1,
        "outline_width": 2.2,
        "shadow": 0.9,
        "margin_v": 64,
        "margin_l": 112,
        "margin_r": 112,
    }
    style = (
        f"FontName={style_config['font']},"
        f"FontSize={style_config['size']},"
        "Bold=1,"
        "Spacing=0.4,"
        f"PrimaryColour={style_config['primary']},"
        f"OutlineColour={style_config['outline']},"
        f"BackColour={style_config['back']},"
        f"BorderStyle={style_config['border_style']},"
        f"Outline={style_config['outline_width']},"
        f"Shadow={style_config['shadow']},"
        f"Alignment={style_config['alignment']},"
        f"MarginV={style_config['margin_v']},"
        f"MarginL={style_config['margin_l']},"
        f"MarginR={style_config['margin_r']}"
    )
    return f"subtitles=filename='{escaped}':force_style='{style}'"


def _normalize_clip(input_path: Path, output_path: Path) -> None:
    filter_expr = (
        f"scale={OUTPUT_WIDTH}:{OUTPUT_HEIGHT}:force_original_aspect_ratio=decrease,"
        f"pad={OUTPUT_WIDTH}:{OUTPUT_HEIGHT}:(ow-iw)/2:(oh-ih)/2:color=black,"
        "setsar=1"
    )
    _run(
        [
            "ffmpeg",
            "-y",
            "-i",
            str(input_path),
            "-vf",
            filter_expr,
            "-an",
            "-r",
            "30",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "20",
            str(output_path),
        ]
    )


def _concat_clips(clip_paths: list[Path], output_path: Path) -> None:
    list_path = output_path.parent / "concat_list.txt"
    def concat_line(path: Path) -> str:
        escaped = path.as_posix().replace("'", "'\\''")
        return f"file '{escaped}'"

    list_path.write_text(
        "\n".join(concat_line(path) for path in clip_paths),
        encoding="utf-8",
    )
    _run(
        [
            "ffmpeg",
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(list_path),
            "-c",
            "copy",
            str(output_path),
        ]
    )


def _extract_json_object(text: str) -> dict:
    raw = (text or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?", "", raw).strip()
        raw = re.sub(r"```$", "", raw).strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        match = re.search(r"\{[\s\S]*\}", raw)
        if not match:
            raise
        return json.loads(match.group(0))


def _safe_expansion_paragraphs(target_market: str) -> list[str]:
    if target_market == "tw":
        return [
            "從實際看房的角度來看，可以先留意整體動線是否順暢。由入口進入後，每一個空間的銜接都會影響日常生活的便利性，包含回家放置物品、整理家務，以及家人之間的活動安排。",
            "接著可以觀察採光和通風。房屋不只是看格局，也要感受白天的自然光、窗邊的開放感，以及不同區域之間的空氣流動，這些細節會直接影響居住舒適度。",
            "收納也是看房時很值得確認的重點。無論是玄關、廚房、洗面空間，還是臥室周邊，如果能夠把日常用品有序收好，房子住起來就會更乾淨，也更容易維持生活品質。",
            "最後可以把整體居住感受連在一起看。這類房源適合慢慢走一遍，從進門、生活、休息到整理家務，每一步都能想像未來實際入住後的節奏。",
        ]
    if target_market == "jp":
        return [
            "実際の内見では、まず全体の生活動線を確認することが大切です。玄関から各スペースへ移動する流れ、荷物を置く場所、家事をしやすい配置など、毎日の暮らしやすさに直結します。",
            "次に、採光と風通しの印象も見ておきたいポイントです。窓まわりの明るさ、室内に入る自然光、各空間の開放感は、住んだ後の快適さを大きく左右します。",
            "収納の使いやすさも重要です。玄関、キッチン、洗面まわり、居室部分に必要な物を整理できる余地があるかを見ることで、実際の生活イメージがより具体的になります。",
            "最後に、家全体を一つの暮らしとして捉えると、この物件の使いやすさが見えてきます。日常の動き、休む時間、家事の流れを想像しながら見ると、より判断しやすくなります。",
        ]
    return [
        "从实际看房的角度来看，大家可以先关注整体动线是否顺畅。进入室内之后，每一个空间之间的衔接，都会影响未来日常生活的便利性，比如回家后的收纳、家务整理，以及家人之间的活动安排。",
        "接下来可以留意采光和通风。房子不只是看面积和格局，也要感受白天自然光进入室内后的状态，窗边区域是否明亮，各个空间之间是否有比较舒服的通透感，这些都会影响居住体验。",
        "收纳也是看房时非常值得确认的细节。无论是玄关、厨房、洗面空间，还是卧室周边，如果日常用品能够有序放置，房子住起来就会更加清爽，也更容易保持整洁。",
        "最后可以把整个空间连起来感受。看房时不要只看某一个局部，而是按照真实生活的顺序，从进门、做饭、洗漱、休息到日常整理，想象自己真正住进来之后的节奏。",
    ]


def _pad_script_to_minimum_length(script_text: str, target_min_chars: int, target_market: str) -> str:
    padded = (script_text or "").strip()
    if _script_visible_length(padded) >= target_min_chars:
        return padded
    paragraphs = _safe_expansion_paragraphs(target_market)
    index = 0
    while _script_visible_length(padded) < target_min_chars and index < 24:
        paragraph = paragraphs[index % len(paragraphs)]
        if paragraph not in padded:
            padded = f"{padded}\n{paragraph}".strip()
        else:
            padded = f"{padded}\n另外，建议结合现场视频的顺序，继续观察空间细节和生活动线，这样客户能更完整地理解这套房源的实际使用感。".strip()
        index += 1
    return padded


def _expand_property_script_with_openai(
    *,
    script_text: str,
    target_market: str,
    target_min_chars: int,
    target_max_chars: int,
    api_key: str,
    model: str,
) -> str:
    language = "繁體中文" if target_market == "tw" else ("日语" if target_market == "jp" else "简体中文")
    prompt = f"""
请把下面这份房源实拍解说文案扩写成“长版看房讲解稿”。

输出语言：{language}
当前有效字数：约 {_script_visible_length(script_text)} 字
扩写后必须不少于 {target_min_chars} 字，尽量不超过 {target_max_chars} 字。

规则：
1. 必须输出完整文案，不要输出大纲，不要解释。
2. 不要编造价格、面积、楼层、车站距离、收益率、朝向等没有出现在原文里的硬信息。
3. 可以围绕真实看房体验补充：空间动线、采光、通风、收纳、生活便利、居住感受、客户看房时该关注的细节。
4. 语气要像销售陪客户看房，连贯自然，适合直接配音。
5. 输出 JSON：{{"script": "扩写后的完整解说文案"}}

原文：
{script_text}
""".strip()
    response = requests.post(
        OPENAI_CHAT_COMPLETIONS_URL,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": "你只输出可解析 JSON。"},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.45,
            "max_tokens": PROPERTY_MAX_OPENAI_TOKENS,
            "response_format": {"type": "json_object"},
        },
        timeout=120,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"OpenAI 文案补写失败：{response.status_code} {response.text[:500]}")
    body = response.json()
    raw = body.get("choices", [{}])[0].get("message", {}).get("content", "")
    return (_extract_json_object(raw).get("script") or "").strip()


def _split_script_sentences(script_text: str) -> list[str]:
    text = re.sub(r"\s+", " ", (script_text or "").strip())
    if not text:
        return []
    return [part.strip() for part in re.split(r"(?<=[。！？!?；;])\s*", text) if part.strip()]


def _closing_sentence_for_market(target_market: str) -> str:
    if target_market == "tw":
        return "以上就是這套房源的實際看房介紹，如果您想進一步了解，可以再安排實地確認。"
    if target_market == "jp":
        return "以上が今回の物件紹介です。気になる点があれば、ぜひ現地で一緒に確認していきましょう。"
    return "以上就是这套房源的实际看房介绍，如果您想进一步了解，可以再安排实地确认。"


def _has_natural_closing(script_text: str) -> bool:
    tail = (script_text or "").strip()[-80:]
    closing_tokens = (
        "以上",
        "整体介绍",
        "房源介绍",
        "實際看房介紹",
        "物件紹介",
        "实地确认",
        "現地",
        "预约",
        "預約",
    )
    return any(token in tail for token in closing_tokens)


def _trim_script_to_max_length(script_text: str, target_max_chars: int, target_market: str = "cn", keep_closing: bool = True) -> str:
    sentences = _split_script_sentences(script_text)
    if not sentences:
        return (script_text or "")[:max(target_max_chars, 1)].strip()

    if not keep_closing or target_max_chars < 45:
        kept: list[str] = []
        for sentence in sentences:
            candidate = "".join(kept + [sentence]).strip()
            if _script_visible_length(candidate) <= target_max_chars:
                kept.append(sentence)
                continue
            break
        if kept:
            return "".join(kept).strip()
        first = sentences[0]
        pieces = [part.strip() for part in re.split(r"(?<=[，,、])\s*", first) if part.strip()]
        partial = ""
        for piece in pieces:
            candidate = f"{partial}{piece}".strip()
            if _script_visible_length(candidate) <= target_max_chars:
                partial = candidate
                continue
            break
        return partial or first[:max(target_max_chars, 1)].strip()

    closing = sentences[-1] if _has_natural_closing(sentences[-1]) else _closing_sentence_for_market(target_market)
    closing_len = _script_visible_length(closing)
    body_limit = max(20, target_max_chars - closing_len)
    kept: list[str] = []
    for sentence in sentences[:-1] if _has_natural_closing(sentences[-1]) else sentences:
        if _has_natural_closing(sentence):
            continue
        candidate = "".join(kept + [sentence]).strip()
        if _script_visible_length(candidate) <= body_limit:
            kept.append(sentence)
            continue
        break

    if kept:
        return f"{''.join(kept).strip()}{closing}".strip()

    # If the first sentence alone is too long, cut at a comma-like pause so we
    # still avoid chopping a word or phrase in the middle whenever possible.
    first = sentences[0]
    pieces = [part.strip() for part in re.split(r"(?<=[，,、])\s*", first) if part.strip()]
    partial = ""
    for piece in pieces:
        candidate = f"{partial}{piece}".strip()
        if _script_visible_length(candidate) <= target_max_chars:
            partial = candidate
            continue
        break
    if partial:
        return f"{partial}{closing}".strip()
    return closing if closing_len <= target_max_chars else first[:max(target_max_chars, 1)].strip()


def _compress_property_script_with_openai(
    *,
    script_text: str,
    target_duration: float,
    actual_duration: float,
    target_market: str,
    target_min_chars: int,
    target_max_chars: int,
    api_key: str,
    model: str,
) -> str:
    language = "繁體中文" if target_market == "tw" else ("日语" if target_market == "jp" else "简体中文")
    target_mid_chars = int((target_min_chars + target_max_chars) / 2)
    prompt = f"""
请把下面这份房源实拍连续解说文案压缩成更短、更紧凑的口播稿。

输出语言：{language}
目标视频时长：{target_duration:.2f} 秒
当前配音真实时长：{actual_duration:.2f} 秒，明显偏长
当前有效字数：约 {_script_visible_length(script_text)} 字
压缩后目标有效字数：约 {target_mid_chars} 字，必须落在 {target_min_chars}-{target_max_chars} 字之间。

硬性规则：
1. 必须输出完整文案，不要输出解释，不要输出大纲。
2. 这次必须明显压缩，不要只是润色；如果超过目标字数范围，这次任务就是失败。
3. 保留看房顺序和核心卖点，但删除重复寒暄、泛泛而谈、过度铺垫。
4. 不要编造价格、面积、车站距离、楼层、朝向、收益率等原文没有的硬信息。
5. 语气仍然像销售带客户看房，句子自然连贯，适合直接配音。
6. 输出 JSON：{{"script": "压缩后的完整解说文案"}}

原文：
{script_text}
""".strip()
    response = requests.post(
        OPENAI_CHAT_COMPLETIONS_URL,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": "你只输出可解析 JSON。"},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.25,
            "max_tokens": PROPERTY_MAX_OPENAI_TOKENS,
            "response_format": {"type": "json_object"},
        },
        timeout=120,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"OpenAI 文案压缩失败：{response.status_code} {response.text[:500]}")
    body = response.json()
    raw = body.get("choices", [{}])[0].get("message", {}).get("content", "")
    return (_extract_json_object(raw).get("script") or "").strip()


def _calibrate_property_script_with_openai(
    *,
    script_text: str,
    target_duration: float,
    actual_duration: float,
    target_market: str,
    attempt: int,
    keep_closing: bool = True,
) -> str:
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        return script_text
    direction = "扩写" if actual_duration < target_duration else "压缩"
    language = "繁體中文" if target_market == "tw" else ("日语" if target_market == "jp" else "简体中文")
    current_chars, target_min_chars, target_max_chars = _target_script_length_for_duration(
        script_text,
        target_duration,
        actual_duration,
    )
    target_mid_chars = int((target_min_chars + target_max_chars) / 2)
    prompt = f"""
你是 iHouse 的房源实拍视频解说文案时长校准助手。请只改写文案，不要输出解释。

目标视频总时长：{target_duration:.2f} 秒
当前配音真实时长：{actual_duration:.2f} 秒
当前需要：{direction}文案，让下一次 TTS 配音尽量接近目标视频总时长，允许误差 ±{PROPERTY_AUDIO_TOLERANCE_SECONDS:.0f} 秒。
当前文案有效字数：约 {current_chars} 字
校准后目标有效字数：约 {target_mid_chars} 字，必须落在 {target_min_chars}-{target_max_chars} 字之间。
输出语言：{language}
第 {attempt} 次校准。

要求：
1. 保留原文事实和销售表达重点，不要编造价格、面积、车站距离、楼层、朝向、收益率等新信息。
2. 如果需要扩写，只能围绕空间感、动线、采光、收纳、居住体验、看房顺序做自然补充。
3. 如果需要压缩，要保留最关键卖点，让语气仍然自然。
4. 这次校准的核心是“时长匹配”，不要只轻微润色；请按目标字数完整扩写或压缩。
5. 输出 JSON：{{"script": "校准后的完整解说文案"}}

当前文案：
{script_text}
""".strip()
    model = (os.getenv("OPENAI_TEXT_MODEL") or os.getenv("OPENAI_VISION_MODEL") or "gpt-4o-mini").strip()
    response = requests.post(
        OPENAI_CHAT_COMPLETIONS_URL,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": "你只输出可解析 JSON。"},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.35,
            "max_tokens": PROPERTY_MAX_OPENAI_TOKENS,
            "response_format": {"type": "json_object"},
        },
        timeout=120,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"OpenAI 文案时长校准失败：{response.status_code} {response.text[:500]}")
    body = response.json()
    raw = body.get("choices", [{}])[0].get("message", {}).get("content", "")
    calibrated = (_extract_json_object(raw).get("script") or "").strip()
    calibrated_chars = _script_visible_length(calibrated)
    if calibrated and actual_duration < target_duration and calibrated_chars < target_min_chars:
        expanded = _expand_property_script_with_openai(
            script_text=calibrated if calibrated_chars > current_chars else script_text,
            target_market=target_market,
            target_min_chars=target_min_chars,
            target_max_chars=target_max_chars,
            api_key=api_key,
            model=model,
        )
        if _script_visible_length(expanded) > calibrated_chars:
            calibrated = expanded
            calibrated_chars = _script_visible_length(calibrated)
        if calibrated_chars < target_min_chars:
            calibrated = _pad_script_to_minimum_length(calibrated, target_min_chars, target_market)
    if calibrated and actual_duration > target_duration:
        if calibrated_chars > target_max_chars or calibrated_chars >= current_chars * 0.94:
            compressed = _compress_property_script_with_openai(
                script_text=calibrated,
                target_duration=target_duration,
                actual_duration=actual_duration,
                target_market=target_market,
                target_min_chars=target_min_chars,
                target_max_chars=target_max_chars,
                api_key=api_key,
                model=model,
            )
            if _script_visible_length(compressed) < calibrated_chars:
                calibrated = compressed
                calibrated_chars = _script_visible_length(calibrated)
        if calibrated_chars > target_max_chars:
            calibrated = _trim_script_to_max_length(calibrated, target_max_chars, target_market, keep_closing=keep_closing)
    return calibrated or script_text


def _generate_calibrated_narration(
    *,
    script_text: str,
    audio_path: Path,
    target_duration: float,
    target_market: str,
    voice_id: str,
    voice_preset: dict,
    speed: float,
    generate_audio_fn: Callable[..., str],
    emit: Callable[[str, Optional[int]], None],
    tolerance_seconds: float = PROPERTY_AUDIO_TOLERANCE_SECONDS,
    keep_closing: bool = True,
) -> tuple[str, float, int]:
    current_script = script_text
    last_duration = 0.0
    for attempt in range(PROPERTY_SCRIPT_CALIBRATION_ATTEMPTS + 1):
        generate_audio_fn(
            current_script,
            str(audio_path),
            voice=voice_id,
            speed=speed,
            volume=float(voice_preset.get("default_volume", 1.0) or 1.0),
            language=voice_preset.get("language", ""),
        )
        last_duration = _get_audio_duration(audio_path)
        if last_duration <= 0:
            raise RuntimeError("配音生成完成，但无法读取音频时长")
        delta = last_duration - target_duration
        if abs(delta) <= tolerance_seconds:
            return current_script, last_duration, attempt
        if attempt >= PROPERTY_SCRIPT_CALIBRATION_ATTEMPTS:
            break
        action = "扩写" if delta < 0 else "压缩"
        current_chars, target_min_chars, target_max_chars = _target_script_length_for_duration(
            current_script,
            target_duration,
            last_duration,
        )
        emit(
            f"配音时长 {last_duration:.1f}s，与视频 {target_duration:.1f}s 相差 {abs(delta):.1f}s，正在 AI {action}文案校准"
            f"（当前约 {current_chars} 字，目标 {target_min_chars}-{target_max_chars} 字）...",
            1,
        )
        current_script = _calibrate_property_script_with_openai(
            script_text=current_script,
            target_duration=target_duration,
            actual_duration=last_duration,
            target_market=target_market,
            attempt=attempt + 1,
            keep_closing=keep_closing,
        )
    if last_duration > target_duration + tolerance_seconds:
        for emergency_attempt in range(2):
            current_chars = max(_script_visible_length(current_script), 1)
            # The normal calibration asks the model to rewrite. If it still runs
            # long, force a conservative sentence-level trim based on the measured
            # TTS speed, then synthesize again. This keeps one-take videos from
            # failing just because the model was too wordy.
            target_chars = max(20, int(current_chars * (target_duration / max(last_duration, 1.0)) * 0.96))
            current_script = _trim_script_to_max_length(current_script, target_chars, target_market, keep_closing=keep_closing)
            emit(
                f"配音仍偏长，正在按真实语速强制压缩文案后重配（第 {emergency_attempt + 1}/2 次，目标约 {target_chars} 字）...",
                1,
            )
            generate_audio_fn(
                current_script,
                str(audio_path),
                voice=voice_id,
                speed=speed,
                volume=float(voice_preset.get("default_volume", 1.0) or 1.0),
                language=voice_preset.get("language", ""),
            )
            last_duration = _get_audio_duration(audio_path)
            if last_duration <= 0:
                raise RuntimeError("配音生成完成，但无法读取音频时长")
            if abs(last_duration - target_duration) <= tolerance_seconds:
                return current_script, last_duration, PROPERTY_SCRIPT_CALIBRATION_ATTEMPTS + emergency_attempt + 1
            if last_duration < target_duration - tolerance_seconds:
                current_script = _calibrate_property_script_with_openai(
                    script_text=current_script,
                    target_duration=target_duration,
                    actual_duration=last_duration,
                    target_market=target_market,
                    attempt=PROPERTY_SCRIPT_CALIBRATION_ATTEMPTS + emergency_attempt + 2,
                    keep_closing=keep_closing,
                )
                generate_audio_fn(
                    current_script,
                    str(audio_path),
                    voice=voice_id,
                    speed=speed,
                    volume=float(voice_preset.get("default_volume", 1.0) or 1.0),
                    language=voice_preset.get("language", ""),
                )
                last_duration = _get_audio_duration(audio_path)
                if abs(last_duration - target_duration) <= tolerance_seconds:
                    return current_script, last_duration, PROPERTY_SCRIPT_CALIBRATION_ATTEMPTS + emergency_attempt + 1
    raise RuntimeError(
        f"文案配音时长仍未匹配视频：视频 {target_duration:.1f}s，配音 {last_duration:.1f}s。请在解说文案中手动{'增加' if last_duration < target_duration else '减少'}约 {abs(last_duration - target_duration):.0f} 秒内容后重试。"
    )


def _concat_audio_files(audio_paths: list[Path], output_path: Path) -> None:
    if not audio_paths:
        raise RuntimeError("没有可拼接的分段配音")
    cmd = ["ffmpeg", "-y"]
    for path in audio_paths:
        cmd.extend(["-i", str(path)])
    filter_inputs = "".join(f"[{index}:a]" for index in range(len(audio_paths)))
    cmd.extend(
        [
            "-filter_complex",
            f"{filter_inputs}concat=n={len(audio_paths)}:v=0:a=1[aout]",
            "-map",
            "[aout]",
            "-c:a",
            "libmp3lame",
            "-q:a",
            "3",
            str(output_path),
        ]
    )
    _run(cmd)


def _mix_timeline_audio_files(segments: list[dict], output_path: Path) -> None:
    playable_segments = [
        segment
        for segment in segments
        if segment.get("audio_path") and Path(str(segment.get("audio_path"))).exists()
    ]
    if not playable_segments:
        raise RuntimeError("没有可混合的一镜到底分段配音")
    cmd = ["ffmpeg", "-y"]
    for segment in playable_segments:
        cmd.extend(["-i", str(segment["audio_path"])])

    filters: list[str] = []
    labels: list[str] = []
    for index, segment in enumerate(playable_segments):
        delay_ms = max(0, int(round(float(segment.get("start") or 0.0) * 1000)))
        label = f"a{index}"
        filters.append(f"[{index}:a]adelay={delay_ms}:all=1[{label}]")
        labels.append(f"[{label}]")
    filters.append(f"{''.join(labels)}amix=inputs={len(labels)}:duration=longest:normalize=0[aout]")
    cmd.extend(
        [
            "-filter_complex",
            ";".join(filters),
            "-map",
            "[aout]",
            "-c:a",
            "libmp3lame",
            "-q:a",
            "3",
            str(output_path),
        ]
    )
    _run(cmd)


def _normalize_timeline_segments(timeline_segments: list[dict] | None, video_duration: float, fallback_script: str) -> list[dict]:
    normalized: list[dict] = []
    if not isinstance(timeline_segments, list):
        return []
    for index, item in enumerate(timeline_segments, start=1):
        if not isinstance(item, dict):
            continue
        script = str(item.get("script") or "").strip()
        if not script:
            continue
        try:
            start = float(item.get("start", 0) or 0)
            end = float(item.get("end", 0) or 0)
        except (TypeError, ValueError):
            continue
        start = max(0.0, min(video_duration, start))
        end = max(start, min(video_duration, end))
        if end - start < 1.0:
            continue
        normalized.append(
            {
                "index": int(item.get("index") or index),
                "start": round(start, 3),
                "end": round(end, 3),
                "target_duration": round(end - start, 3),
                "room_type": str(item.get("room_type") or item.get("room") or "空间"),
                "visual_summary": str(item.get("visual_summary") or ""),
                "script": script,
            }
        )
    if not normalized:
        return []
    normalized.sort(key=lambda item: (float(item["start"]), float(item["end"])))
    merged: list[dict] = []
    for segment in normalized:
        duration = float(segment["end"]) - float(segment["start"])
        if duration >= PROPERTY_TIMELINE_MIN_SEGMENT_SECONDS or not merged:
            merged.append(segment)
            continue
        previous = merged[-1]
        previous["end"] = segment["end"]
        previous["target_duration"] = round(float(previous["end"]) - float(previous["start"]), 3)
        if str(segment.get("room_type") or "") not in str(previous.get("room_type") or ""):
            previous["room_type"] = f"{previous.get('room_type') or '空间'} / {segment.get('room_type') or '空间'}"
        if segment.get("visual_summary"):
            previous["visual_summary"] = "；".join(part for part in [str(previous.get("visual_summary") or ""), str(segment.get("visual_summary") or "")] if part)
        previous["script"] = "\n".join(part for part in [str(previous.get("script") or "").strip(), str(segment.get("script") or "").strip()] if part)

    if len(merged) >= 2 and float(merged[0]["end"]) - float(merged[0]["start"]) < PROPERTY_TIMELINE_MIN_SEGMENT_SECONDS:
        first = merged.pop(0)
        merged[0]["start"] = first["start"]
        merged[0]["target_duration"] = round(float(merged[0]["end"]) - float(merged[0]["start"]), 3)
        if str(first.get("room_type") or "") not in str(merged[0].get("room_type") or ""):
            merged[0]["room_type"] = f"{first.get('room_type') or '空间'} / {merged[0].get('room_type') or '空间'}"
        if first.get("visual_summary"):
            merged[0]["visual_summary"] = "；".join(part for part in [str(first.get("visual_summary") or ""), str(merged[0].get("visual_summary") or "")] if part)
        merged[0]["script"] = "\n".join(part for part in [str(first.get("script") or "").strip(), str(merged[0].get("script") or "").strip()] if part)

    normalized = merged
    for index, segment in enumerate(normalized, start=1):
        segment["index"] = index
        segment["target_duration"] = round(float(segment["end"]) - float(segment["start"]), 3)
    if normalized[0]["start"] > 0.75:
        normalized[0]["start"] = 0.0
        normalized[0]["target_duration"] = round(float(normalized[0]["end"]) - float(normalized[0]["start"]), 3)
    if video_duration - float(normalized[-1]["end"]) > 0.75:
        normalized[-1]["end"] = round(video_duration, 3)
        normalized[-1]["target_duration"] = round(float(normalized[-1]["end"]) - float(normalized[-1]["start"]), 3)
    return normalized


def _generate_timeline_narration(
    *,
    timeline_segments: list[dict],
    audio_dir: Path,
    target_market: str,
    voice_id: str,
    voice_preset: dict,
    speed: float,
    generate_audio_fn: Callable[..., str],
    emit: Callable[[str, Optional[int]], None],
    target_total_duration: float | None = None,
) -> tuple[list[dict], Path, float, int]:
    working_segments = [dict(segment) for segment in timeline_segments]
    total_attempts = 0
    combined_audio = audio_dir / "narration.mp3"
    generated: list[dict] = []

    for pass_index in range(2):
        generated = []
        for index, segment in enumerate(working_segments, start=1):
            target_duration = float(segment.get("target_duration") or 0.0)
            if target_duration <= 0:
                continue
            segment_audio = audio_dir / f"timeline_segment_{index:02d}.mp3"
            target_audio_duration = max(1.0, target_duration - 0.25)
            tolerance = min(PROPERTY_AUDIO_TOLERANCE_SECONDS, max(0.75, target_audio_duration * 0.14))
            emit(
                f"正在生成一镜到底对齐配音 {index}/{len(working_segments)}：{segment.get('room_type') or '空间'}，目标 {target_audio_duration:.1f}s...",
                1,
            )
            script, audio_duration, attempts = _generate_calibrated_narration(
                script_text=str(segment.get("script") or ""),
                audio_path=segment_audio,
                target_duration=target_audio_duration,
                target_market=target_market,
                voice_id=voice_id,
                voice_preset=voice_preset,
                speed=speed,
                generate_audio_fn=generate_audio_fn,
                emit=emit,
                tolerance_seconds=tolerance,
                keep_closing=False,
            )
            enriched = {
                **segment,
                "script": script,
                "audio_path": str(segment_audio),
                "audio_duration": round(audio_duration, 3),
                "audio_offset": round(float(segment.get("start") or 0.0), 3),
                "audio_video_delta_seconds": round(audio_duration - target_duration, 3),
                "script_calibration_attempts": attempts,
            }
            generated.append(enriched)
            total_attempts += attempts
        _mix_timeline_audio_files(generated, combined_audio)
        combined_duration = _get_audio_duration(combined_audio)
        target = float(target_total_duration or 0.0)
        if not target or abs(combined_duration - target) <= PROPERTY_TIMELINE_AUDIO_TOLERANCE_SECONDS or pass_index >= 1:
            return generated, combined_audio, combined_duration, total_attempts

        delta = target - combined_duration
        adjustable = max(generated, key=lambda item: float(item.get("target_duration") or 0.0), default=None)
        if not adjustable:
            return generated, combined_audio, combined_duration, total_attempts
        segment_index = max(0, int(adjustable.get("index") or len(working_segments)) - 1)
        current_target = float(working_segments[segment_index].get("target_duration") or adjustable.get("target_duration") or 0.0)
        working_segments[segment_index]["target_duration"] = round(max(1.0, current_target + delta), 3)
        emit(
            f"分段配音合计 {combined_duration:.1f}s，与视频 {target:.1f}s 相差 {abs(delta):.1f}s，正在自动调整最长场景段后重配...",
            1,
        )

    return generated, combined_audio, _get_audio_duration(combined_audio), total_attempts


def _mux_voice_and_subtitles(
    video_path: Path,
    audio_path: Path,
    subtitle_path: Path,
    output_path: Path,
    bgm_path: Path | None = None,
    bgm_volume: float = 0.10,
    tolerance_seconds: float = PROPERTY_AUDIO_TOLERANCE_SECONDS,
) -> tuple[float, float]:
    audio_duration = _get_audio_duration(audio_path)
    video_duration = _ffprobe_duration(video_path)
    if audio_duration <= 0:
        raise RuntimeError("配音时长读取失败")

    if abs(audio_duration - video_duration) > tolerance_seconds:
        raise RuntimeError(f"配音时长与视频时长不匹配：视频 {video_duration:.1f}s，配音 {audio_duration:.1f}s")

    subtitle_filter = _property_subtitle_filter(subtitle_path)
    safe_bgm_volume = max(0.0, min(float(bgm_volume or 0.10), 0.30))
    if bgm_path and bgm_path.exists() and safe_bgm_volume > 0:
        fade_out_start = max(0.0, video_duration - 1.2)
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            str(video_path),
            "-i",
            str(audio_path),
            "-stream_loop",
            "-1",
            "-i",
            str(bgm_path),
            "-t",
            f"{video_duration:.3f}",
            "-filter_complex",
            (
                f"[0:v]{subtitle_filter}[vout];"
                f"[2:a]volume={safe_bgm_volume:.3f},atrim=0:{video_duration:.3f},"
                f"asetpts=PTS-STARTPTS,afade=t=in:st=0:d=0.8,"
                f"afade=t=out:st={fade_out_start:.3f}:d=1.2[bgm];"
                "[1:a][bgm]amix=inputs=2:duration=first:dropout_transition=0[aout]"
            ),
            "-map",
            "[vout]",
            "-map",
            "[aout]",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "20",
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            "-movflags",
            "+faststart",
            str(output_path),
        ]
        try:
            _run(cmd)
        except RuntimeError as exc:
            # Subtitle burn-in can fail on some ffmpeg/libass/font builds. Keep
            # the deliverable video usable and still export the SRT alongside it.
            fallback_cmd = [
                "ffmpeg",
                "-y",
                "-i",
                str(video_path),
                "-i",
                str(audio_path),
                "-stream_loop",
                "-1",
                "-i",
                str(bgm_path),
                "-t",
                f"{video_duration:.3f}",
                "-filter_complex",
                (
                    f"[2:a]volume={safe_bgm_volume:.3f},atrim=0:{video_duration:.3f},"
                    f"asetpts=PTS-STARTPTS,afade=t=in:st=0:d=0.8,"
                    f"afade=t=out:st={fade_out_start:.3f}:d=1.2[bgm];"
                    "[1:a][bgm]amix=inputs=2:duration=first:dropout_transition=0[aout]"
                ),
                "-map",
                "0:v:0",
                "-map",
                "[aout]",
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-crf",
                "20",
                "-c:a",
                "aac",
                "-b:a",
                "192k",
                "-movflags",
                "+faststart",
                str(output_path),
            ]
            _run(fallback_cmd)
        return audio_duration, video_duration

    base_cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(video_path),
        "-i",
        str(audio_path),
        "-t",
        f"{video_duration:.3f}",
        "-map",
        "0:v:0",
        "-map",
        "1:a:0",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "20",
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        "-movflags",
        "+faststart",
        str(output_path),
    ]
    try:
        _run(base_cmd[:6] + ["-vf", subtitle_filter] + base_cmd[6:])
    except RuntimeError as exc:
        # Some local ffmpeg builds omit libass/subtitles. Keep the workflow usable
        # and still export the SRT; production images normally support burn-in.
        _run(base_cmd)
    return audio_duration, video_duration


def build_property_video(
    *,
    output_dir: Path,
    uploaded_video_paths: list[Path],
    script_text: str,
    voice_id: str,
    voice_preset: dict,
    speed: float,
    target_market: str,
    generate_audio_fn: Callable[..., str],
    bgm_path: Path | None = None,
    bgm_volume: float = 0.10,
    timeline_segments: list[dict] | None = None,
    log: Optional[Callable[[str, Optional[int]], None]] = None,
) -> dict:
    def emit(message: str, step: Optional[int] = None) -> None:
        if log:
            log(message, step)

    if not uploaded_video_paths:
        raise ValueError("请至少上传一个房源视频")
    script_text = (script_text or "").strip()
    if not script_text:
        raise ValueError("请填写解说文案")

    output_dir.mkdir(parents=True, exist_ok=True)
    upload_dir = output_dir / "uploads"
    work_dir = output_dir / "work"
    audio_dir = output_dir / "audio"
    subtitle_dir = output_dir / "subtitles"
    final_dir = output_dir / "final"
    for directory in (upload_dir, work_dir, audio_dir, subtitle_dir, final_dir):
        directory.mkdir(parents=True, exist_ok=True)

    source_videos: list[Path] = []
    for index, source in enumerate(uploaded_video_paths, start=1):
        suffix = source.suffix.lower()
        if suffix not in PROPERTY_VIDEO_EXTENSIONS:
            raise ValueError(f"不支持的视频格式：{source.name}")
        destination = upload_dir / f"clip_{index:02d}{suffix}"
        if source.resolve() != destination.resolve():
            shutil.copy2(source, destination)
        source_videos.append(destination)

    emit("正在统一视频规格并按上传顺序拼接...", 3)
    normalized_paths: list[Path] = []
    for index, source in enumerate(source_videos, start=1):
        normalized = work_dir / f"normalized_{index:02d}.mp4"
        _normalize_clip(source, normalized)
        normalized_paths.append(normalized)

    merged_video_path = work_dir / "merged_silent.mp4"
    if len(normalized_paths) == 1:
        shutil.copy2(normalized_paths[0], merged_video_path)
    else:
        _concat_clips(normalized_paths, merged_video_path)
    video_duration = _ffprobe_duration(merged_video_path)
    if video_duration <= 0:
        raise RuntimeError("合并后视频时长读取失败")

    audio_path = audio_dir / "narration.mp3"
    subtitle_path = subtitle_dir / "property_narration.srt"
    normalized_timeline = _normalize_timeline_segments(timeline_segments, video_duration, script_text)
    timeline_result: list[dict] = []
    if normalized_timeline:
        emit(f"检测到一镜到底讲解单元，共 {len(normalized_timeline)} 段，正在生成完整连续口播...", 1)
        script_text = "\n".join(str(segment.get("script") or "").strip() for segment in normalized_timeline if segment.get("script")).strip()
        if not _has_natural_closing(script_text):
            script_text = f"{script_text}\n{_closing_sentence_for_market(target_market)}".strip()
        script_text, audio_duration, calibration_attempts = _generate_calibrated_narration(
            script_text=script_text,
            audio_path=audio_path,
            target_duration=video_duration,
            target_market=target_market,
            voice_id=voice_id,
            voice_preset=voice_preset,
            speed=speed,
            generate_audio_fn=generate_audio_fn,
            emit=emit,
            tolerance_seconds=PROPERTY_TIMELINE_AUDIO_TOLERANCE_SECONDS,
            keep_closing=True,
        )
        timeline_result = normalized_timeline
        emit("正在生成完整口播字幕时间轴...", 2)
        _write_property_subtitles(script_text, audio_duration, subtitle_path, audio_path=audio_path, target_market=target_market)
    else:
        emit(f"正在生成房源解说配音，并校准到视频总时长 {video_duration:.1f}s...", 1)
        script_text, audio_duration, calibration_attempts = _generate_calibrated_narration(
            script_text=script_text,
            audio_path=audio_path,
            target_duration=video_duration,
            target_market=target_market,
            voice_id=voice_id,
            voice_preset=voice_preset,
            speed=speed,
            generate_audio_fn=generate_audio_fn,
            emit=emit,
        )
        emit("正在生成字幕时间轴...", 2)
        _write_property_subtitles(script_text, audio_duration, subtitle_path, audio_path=audio_path, target_market=target_market)

    if bgm_path and bgm_path.exists():
        emit(f"正在合成配音、字幕、背景音乐和最终成片（BGM 音量 {int(max(0.0, min(float(bgm_volume or 0.10), 0.30)) * 100)}%）...", 4)
    else:
        emit("正在合成配音、字幕和最终成片...", 4)
    final_video_path = final_dir / "property_real_shot_final.mp4"
    audio_duration, video_duration = _mux_voice_and_subtitles(
        merged_video_path,
        audio_path,
        subtitle_path,
        final_video_path,
        bgm_path=bgm_path,
        bgm_volume=bgm_volume,
        tolerance_seconds=PROPERTY_TIMELINE_AUDIO_TOLERANCE_SECONDS if timeline_result else PROPERTY_AUDIO_TOLERANCE_SECONDS,
    )

    result = {
        "mode": "property_video",
        "title": "房源实拍成片",
        "topic": "房源实拍成片",
        "script_text": script_text,
        "segment_count": len(source_videos),
        "total_duration": round(audio_duration, 2),
        "source_video_duration": round(video_duration, 2),
        "audio_video_delta_seconds": round(audio_duration - video_duration, 2),
        "script_calibration_attempts": calibration_attempts,
        "timeline_segments": timeline_result,
        "final_video_path": str(final_video_path),
        "subtitle_path": str(subtitle_path),
        "narration_audio_path": str(audio_path),
        "bgm_path": str(bgm_path) if bgm_path else "",
        "bgm_volume": max(0.0, min(float(bgm_volume or 0.10), 0.30)) if bgm_path else 0.0,
        "source_videos": [str(path) for path in source_videos],
        "workflow_config": {
            "target_market": target_market,
            "voice_preset": {
                **voice_preset,
                "selected_speed": speed,
            },
            "bgm_path": str(bgm_path) if bgm_path else "",
            "bgm_volume": max(0.0, min(float(bgm_volume or 0.10), 0.30)) if bgm_path else 0.0,
            "property_video_mode": "one_take_narration_units" if timeline_result else "real_shot_voiceover",
        },
        "files": [],
    }
    (output_dir / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    emit("房源实拍成片已完成", 4)
    return result
