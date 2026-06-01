"""
房源实拍视频视觉分析。

只在用户主动点击“AI 分析视频并生成文案”时调用 OpenAI 视觉模型。
"""

from __future__ import annotations

import base64
import json
import os
import re
import subprocess
from pathlib import Path

import requests


OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions"
TIMELINE_TARGET_SEGMENT_SECONDS = 12
TIMELINE_MAX_SEGMENT_SECONDS = 18
TIMELINE_MIN_SEGMENT_SECONDS = 2.8
NARRATION_UNIT_MIN_SECONDS = 10.0
NARRATION_UNIT_MAX_SECONDS = 25.0
TIMELINE_MIN_SCRIPT_RATIO = 0.82
OPENAI_MAX_TEXT_TOKENS = 8192


def _run(cmd: list[str]) -> None:
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError((result.stderr or result.stdout or "ffmpeg failed").strip())


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


def get_video_duration(path: Path) -> float:
    return _ffprobe_duration(path)


def _frame_timestamps(duration: float, max_frames: int = 3) -> list[float]:
    if duration <= 0.1:
        return [0.0]
    if max_frames <= 1 or duration < 2:
        return [min(0.5, duration / 2)]
    if max_frames == 2:
        return [0.5, max(0.5, duration - 0.5)]
    step = max(duration / max(max_frames - 1, 1), 1.0)
    return sorted(
        {
            round(max(0.0, min(duration - 0.1, index * step)), 2)
            for index in range(max_frames)
        }
    )


def extract_property_video_frames(video_paths: list[Path], work_dir: Path, max_frames_per_video: int = 3) -> list[dict]:
    frames_dir = work_dir / "frames"
    frames_dir.mkdir(parents=True, exist_ok=True)
    frames: list[dict] = []
    clip_offset = 0.0
    for clip_index, video_path in enumerate(video_paths, start=1):
        duration = _ffprobe_duration(video_path)
        for frame_index, timestamp in enumerate(_frame_timestamps(duration, max_frames=max_frames_per_video), start=1):
            frame_path = frames_dir / f"clip_{clip_index:02d}_frame_{frame_index:02d}.jpg"
            _run(
                [
                    "ffmpeg",
                    "-y",
                    "-ss",
                    f"{timestamp:.3f}",
                    "-i",
                    str(video_path),
                    "-frames:v",
                    "1",
                    "-vf",
                    "scale='min(960,iw)':-2",
                    "-q:v",
                    "4",
                    str(frame_path),
                ]
            )
            if frame_path.exists() and frame_path.stat().st_size > 0:
                frames.append(
                    {
                        "clip_index": clip_index,
                        "frame_index": frame_index,
                        "timestamp": round(timestamp, 2),
                        "global_timestamp": round(clip_offset + timestamp, 2),
                        "path": str(frame_path),
                    }
                )
        clip_offset += duration
    return frames


def _image_data_url(path: Path) -> str:
    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    return f"data:image/jpeg;base64,{encoded}"


def _target_language(target_market: str) -> str:
    if target_market == "tw":
        return "繁體中文"
    if target_market == "jp":
        return "日语"
    return "简体中文"


def _chars_per_second(target_market: str) -> tuple[float, float]:
    if target_market == "jp":
        return 2.2, 3.0
    if target_market == "tw":
        return 3.2, 4.0
    return 3.8, 4.6


def _visible_text_length(text: str) -> int:
    return len(re.sub(r"[\s，,。！？!?；;、：:（）()《》<>【】\[\]“”\"'‘’…—\-]+", "", text or ""))


WET_AREA_RE = re.compile(r"(厕所|洗手间|卫生间|衛生間|トイレ|toilet|浴室|浴缸|浴槽|お風呂|風呂|bath|洗面|洗面室|脱衣|水回り|水周り|水回り)")
NON_WET_ROOM_RE = re.compile(r"(厨房|キッチン|kitchen|客厅|客廳|リビング|LDK|卧室|臥室|洋室|bedroom|玄关|玄關|玄関|阳台|陽台|バルコニー)")


def _is_wet_area_text(*values: object) -> bool:
    text = " ".join(str(value or "") for value in values)
    return bool(WET_AREA_RE.search(text))


def _safe_wet_area_script(room_type: str, visual_summary: str, target_market: str) -> str:
    room = room_type if room_type and room_type != "空间" else "卫生间/浴室"
    if target_market == "tw":
        return f"這裡是{room}區域，可以重點確認乾淨程度、使用動線、通風與日常清潔是否方便。"
    if target_market == "jp":
        return f"こちらは{room}まわりです。清潔感、使いやすい動線、換気のしやすさを確認しておきたいポイントです。"
    return f"这里是{room}区域，可以重点确认整洁度、使用动线、通风情况，以及日常清洁是否方便。"


def _sanitize_protected_room_script(segment: dict, target_market: str) -> dict:
    copied = dict(segment)
    room_type = str(copied.get("room_type") or "")
    visual_summary = str(copied.get("visual_summary") or "")
    script = str(copied.get("script") or "")
    if _is_wet_area_text(room_type, visual_summary) and NON_WET_ROOM_RE.search(script):
        copied["script"] = _safe_wet_area_script(room_type, visual_summary, target_market)
    return copied


def _segment_target_chars(segment: dict, target_market: str) -> tuple[int, int]:
    min_cps, max_cps = _chars_per_second(target_market)
    duration = max(1.0, float(segment.get("duration") or 0))
    lower = int(max(8, duration * min_cps))
    upper = int(max(lower + 6, duration * max_cps))
    return lower, upper


def _timeline_windows_from_frames(frames: list[dict], total_duration: float, target_market: str) -> list[dict]:
    ordered = sorted(frames, key=lambda item: float(item.get("global_timestamp") or 0.0))
    windows: list[dict] = []
    pending_start: float | None = None
    pending_frame: dict | None = None
    for index, frame in enumerate(ordered):
        start = pending_start if pending_start is not None else float(frame.get("global_timestamp") or 0.0)
        source_frame = pending_frame or frame
        if index == 0 or pending_start == 0.0:
            start = 0.0
        end = float(ordered[index + 1].get("global_timestamp") or total_duration) if index + 1 < len(ordered) else total_duration
        start = max(0.0, min(total_duration, start))
        end = max(start, min(total_duration, end))
        if end - start < TIMELINE_MIN_SEGMENT_SECONDS:
            if windows:
                windows[-1]["end"] = round(end, 2)
                windows[-1]["duration"] = round(float(windows[-1]["end"]) - float(windows[-1]["start"]), 2)
                pending_start = None
                pending_frame = None
            else:
                pending_start = start
                pending_frame = source_frame
            continue
        lower, upper = _segment_target_chars({"duration": end - start}, target_market)
        windows.append(
            {
                "index": len(windows) + 1,
                "start": round(start, 2),
                "end": round(end, 2),
                "duration": round(end - start, 2),
                "frame_global_timestamp": round(float(source_frame.get("global_timestamp") or start), 2),
                "clip_index": source_frame.get("clip_index"),
                "frame_index": source_frame.get("frame_index"),
                "target_chars": f"{lower}-{upper}",
            }
        )
        pending_start = None
        pending_frame = None
    return windows


def _merge_timeline_segments_into_narration_units(segments: list[dict], target_market: str) -> list[dict]:
    units: list[dict] = []
    current: dict | None = None

    def flush() -> None:
        nonlocal current
        if not current:
            return
        duration = max(0.0, float(current["end"]) - float(current["start"]))
        current["duration"] = round(duration, 2)
        current["index"] = len(units) + 1
        lower, upper = _segment_target_chars({"duration": duration}, target_market)
        current["target_chars"] = f"{lower}-{upper}"
        units.append(current)
        current = None

    for original_item in sorted(segments, key=lambda seg: (float(seg.get("start") or 0), float(seg.get("end") or 0))):
        item = _sanitize_protected_room_script(original_item, target_market)
        start = float(item.get("start") or 0.0)
        end = float(item.get("end") or start)
        if end <= start:
            continue
        item_is_wet = _is_wet_area_text(item.get("room_type"), item.get("visual_summary"), item.get("script"))
        if current is None:
            current = {
                "index": 0,
                "start": start,
                "end": end,
                "room_type": str(item.get("room_type") or "空间"),
                "visual_summary": str(item.get("visual_summary") or ""),
                "evidence_timestamps": list(item.get("evidence_timestamps") or []),
                "script": str(item.get("script") or "").strip(),
            }
            continue

        current_duration = float(current["end"]) - float(current["start"])
        next_duration = end - float(current["start"])
        room = str(item.get("room_type") or "空间")
        same_room = room and room in str(current.get("room_type") or "")
        current_is_wet = _is_wet_area_text(current.get("room_type"), current.get("visual_summary"), current.get("script"))
        protected_boundary = current_is_wet != item_is_wet
        should_merge = (not protected_boundary and current_duration < NARRATION_UNIT_MIN_SECONDS) or same_room or (not protected_boundary and next_duration <= NARRATION_UNIT_MAX_SECONDS)
        if not should_merge:
            flush()
            current = {
                "index": 0,
                "start": start,
                "end": end,
                "room_type": room,
                "visual_summary": str(item.get("visual_summary") or ""),
                "evidence_timestamps": list(item.get("evidence_timestamps") or []),
                "script": str(item.get("script") or "").strip(),
            }
            continue

        current["end"] = end
        if room and room not in str(current.get("room_type") or ""):
            current["room_type"] = f"{current.get('room_type') or '空间'} / {room}"
        if item.get("visual_summary"):
            current["visual_summary"] = "；".join(part for part in [str(current.get("visual_summary") or ""), str(item.get("visual_summary") or "")] if part)
        if item.get("evidence_timestamps"):
            current["evidence_timestamps"] = list(current.get("evidence_timestamps") or []) + list(item.get("evidence_timestamps") or [])
        if item.get("script"):
            current["script"] = "\n".join(part for part in [str(current.get("script") or "").strip(), str(item.get("script") or "").strip()] if part)
        current = _sanitize_protected_room_script(current, target_market)

    flush()
    return [_sanitize_protected_room_script(unit, target_market) for unit in units]


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


def _expand_timeline_scripts_with_openai(
    *,
    timeline_segments: list[dict],
    suggested_script: str,
    overall_summary: str,
    target_market: str,
    user_notes: str,
    api_key: str,
) -> tuple[list[dict], str, dict]:
    if not timeline_segments:
        return timeline_segments, suggested_script, {}
    language = _target_language(target_market)
    min_cps, max_cps = _chars_per_second(target_market)
    segment_payload = []
    needs_expansion = False
    for item in timeline_segments:
        lower, upper = _segment_target_chars(item, target_market)
        current_len = _visible_text_length(str(item.get("script") or ""))
        if current_len < int(lower * TIMELINE_MIN_SCRIPT_RATIO):
            needs_expansion = True
        segment_payload.append(
            {
                "index": item.get("index"),
                "start": item.get("start"),
                "end": item.get("end"),
                "duration": item.get("duration"),
                "room_type": item.get("room_type"),
                "visual_summary": item.get("visual_summary"),
                "evidence_timestamps": item.get("evidence_timestamps") or [],
                "current_script": item.get("script") or "",
                "current_chars": current_len,
                "target_chars": f"{lower}-{upper}",
            }
        )
    total_target_min = sum(_segment_target_chars(item, target_market)[0] for item in timeline_segments)
    total_current = sum(_visible_text_length(str(item.get("script") or "")) for item in timeline_segments)
    if not needs_expansion and total_current >= int(total_target_min * TIMELINE_MIN_SCRIPT_RATIO):
        return timeline_segments, suggested_script, {}

    model = (os.getenv("OPENAI_TEXT_MODEL") or os.getenv("OPENAI_VISION_MODEL") or "gpt-4o-mini").strip()
    prompt = f"""
你是 iHouse 的一镜到底房源视频“分段销售解说”扩写助手。

输出语言：{language}
销售补充信息：
{user_notes or "未填写。"}

整体判断：
{overall_summary or "无"}

任务：
请把每个 timeline segment 的 current_script 扩写到 target_chars 范围内。必须保持原来的 start/end/room_type，不要合并段落，不要新增段落。

关键规则：
1. 每段只讲该段画面对应的空间，禁止把厨房、卫生间、卧室等不同空间混讲。
2. 不能编造价格、面积、楼层、车站距离、收益率、朝向等硬信息。
3. 如果画面信息有限，就围绕该空间的动线、采光、收纳、使用体验、客户看房关注点自然展开。
4. 文案要像销售陪客户看房的口吻，适合直接配音，不要列点；段落之间要自然衔接，不要像一条条孤立说明。
5. 字数很重要：每秒约 {min_cps:.1f}-{max_cps:.1f} 个字。当前总字数约 {total_current}，目标至少约 {total_target_min}。
6. 如果 room_type 或 visual_summary 是厕所、トイレ、卫生间、洗面室、浴室、浴缸、脱衣所、水回り，该段只能讲对应水回り空间，绝对不能扩写成厨房、客厅、卧室、玄关、阳台。
7. suggested_script 请用所有扩写后的段落自然拼接生成。

请严格输出 JSON：
{{
  "timeline_segments": [
    {{"index": 1, "script": "扩写后的这一段文案"}}
  ],
  "suggested_script": "所有分段文案自然拼接后的完整文案"
}}

当前分段：
{json.dumps(segment_payload, ensure_ascii=False, indent=2)}
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
            "max_tokens": OPENAI_MAX_TEXT_TOKENS,
            "response_format": {"type": "json_object"},
        },
        timeout=160,
    )
    if response.status_code >= 400:
        return timeline_segments, suggested_script, {"error": f"{response.status_code} {response.text[:300]}"}
    body = response.json()
    raw = body.get("choices", [{}])[0].get("message", {}).get("content", "")
    try:
        expanded = _extract_json_object(raw)
    except Exception as exc:
        return timeline_segments, suggested_script, {"error": str(exc)}
    script_by_index = {}
    for item in expanded.get("timeline_segments") or []:
        if isinstance(item, dict) and item.get("index") is not None:
            script = str(item.get("script") or "").strip()
            if script:
                script_by_index[int(item.get("index"))] = script
    updated_segments = []
    for item in timeline_segments:
        copied = dict(item)
        index = int(copied.get("index") or 0)
        if index in script_by_index:
            copied["script"] = script_by_index[index]
        updated_segments.append(copied)
    updated_script = str(expanded.get("suggested_script") or "").strip()
    if not updated_script:
        updated_script = "\n".join(str(item.get("script") or "").strip() for item in updated_segments if item.get("script"))
    return updated_segments, updated_script, {"model": model, "usage": body.get("usage") or {}}


def analyze_property_video_with_openai(
    *,
    video_paths: list[Path],
    work_dir: Path,
    target_market: str = "cn",
    user_notes: str = "",
) -> dict:
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("未配置 OPENAI_API_KEY，无法使用视频视觉分析")

    language = _target_language(target_market)
    clip_durations = [round(_ffprobe_duration(path), 2) for path in video_paths]
    total_duration = round(sum(clip_durations), 2)
    if len(video_paths) == 1:
        max_frames_per_video = max(5, min(18, int(total_duration // TIMELINE_TARGET_SEGMENT_SECONDS) + 1))
    else:
        max_frames_per_video = max(3, min(10, int(max(clip_durations or [0]) // TIMELINE_TARGET_SEGMENT_SECONDS) + 1))
    frames = extract_property_video_frames(video_paths, work_dir, max_frames_per_video=max_frames_per_video)
    if not frames:
        raise RuntimeError("没有成功抽取到视频关键帧")
    fixed_timeline_windows = _timeline_windows_from_frames(frames, total_duration, target_market)

    min_cps, max_cps = _chars_per_second(target_market)
    target_min_chars = int(max(20, total_duration * min_cps))
    target_max_chars = int(max(target_min_chars + 10, total_duration * max_cps))
    recommended_segment_count = max(1, int(round(total_duration / TIMELINE_TARGET_SEGMENT_SECONDS)))
    minimum_segment_count = max(1, int(total_duration // TIMELINE_MAX_SEGMENT_SECONDS))
    prompt = f"""
你是 iHouse 的房源实拍视频销售解说助手。请优先根据销售人员填写的“AI 分析补充信息”生成解说文案，视频关键帧只作为辅助理解画面顺序和校验素材内容。

输出语言：{language}
销售人员填写的房源信息、素材描述和重点介绍要求：
{user_notes or "未填写。请只根据视频画面做保守解说，不要编造价格、面积、交通、楼层、朝向等信息。"}

上传视频会按原顺序合并成一条完整看房视频。
每个片段原始时长：{clip_durations} 秒
合并后总时长：{total_duration} 秒
请同时输出：
1. suggested_script：一整段自然连贯的完整解说文案。
2. timeline_segments：按下面 fixed_timeline_windows 逐个窗口生成“一镜到底分段文案”。这是最重要的字段，必须保证每个时间段只讲该时间段画面正在看到的空间。
本次已按全局时间抽取 {len(frames)} 张关键帧。请严格依据每张图前面的“全局第 X 秒”判断空间变化。
系统固定时间窗 fixed_timeline_windows，必须逐条返回，不能合并，不能改 start/end：
{json.dumps(fixed_timeline_windows, ensure_ascii=False, indent=2)}

生成原则：
1. 销售补充信息里明确写到的房源特点、客户关注点和重点卖点，要优先体现在 suggested_script 里。
2. 视频画面里能看到的空间顺序、采光、收纳、装修、动线等，可以作为辅助描述。
3. 如果销售补充信息与画面不完全一致，不要直接否定销售；在 warnings 里提醒“画面未明显体现该信息”，正文里用更稳妥的说法。
4. 不要编造销售没有提供、画面也看不出来的信息，例如具体价格、面积、车站距离、楼层、朝向、收益率。
5. 文案要像销售发给客户的自然讲解，不要像广告硬广，也不要像 AI 列提纲。
6. timeline_segments 必须和 fixed_timeline_windows 数量一致，index/start/end 必须照抄。不要自己决定时间段，不要合并大段。
7. 每个窗口只依据对应关键帧及其前后 1 个关键帧判断当前画面。比如 0-12 秒画面是门口鞋柜，就只能写“玄关/鞋柜/入户收纳”，绝对不能提前讲厨房。
8. 最高优先级保护规则：只要画面识别为厕所、トイレ、卫生间、洗面室、浴室、浴缸、脱衣所、水回り，这个时间段的 room_type 和 script 必须明确讲对应水回り空间；绝对不能写厨房、客厅、卧室、玄关、阳台等其他空间。这个规则高于文案连贯性。
9. 每个 timeline_segments[].script 要按该段时长控制长度。{language} 每秒大约 {min_cps:.1f}-{max_cps:.1f} 个字，比如 12 秒段落要写约 {int(12 * min_cps)}-{int(12 * max_cps)} 字。不能只写一句很短的话。总字数必须尽量接近 {target_min_chars}-{target_max_chars} 字。
10. 文案必须连贯，像销售一边走一边讲。每段之间可以自然使用“接着往里看”“这里可以看到”“往前走就是”等承接语，但不要为了连贯提前讲后面还没出现的空间。
11. 如果连续几帧都是同一个空间，也要按视频推进拆成多个自然讲解段，分别讲空间感、采光、动线、收纳或使用体验。

请严格输出 JSON，不要输出 Markdown：
{{
  "overall_summary": "整体视频内容概述",
  "suggested_script": "完整销售解说文案，适合直接配音，语气自然，不夸大，不编造价格、面积、车站距离等画面里看不出来的信息",
  "total_video_duration": {total_duration},
  "target_script_chars": "{target_min_chars}-{target_max_chars}",
  "estimated_narration_seconds": {total_duration},
  "timeline_segments": [
    {{
      "index": 1,
      "start": 0.0,
      "end": 6.0,
      "room_type": "玄关/客厅/厨房/卧室/浴室/阳台/走廊/外观/过渡/其他",
      "visual_summary": "这个时间段画面实际看到的内容",
      "evidence_timestamps": [0.0, 6.0, 12.0],
      "script": "只适合这个时间段配音的解说，不要讲别的空间；长度要匹配该段时长"
    }}
  ],
  "clips": [
    {{
      "clip_index": 1,
      "room_type": "玄关/客厅/厨房/卧室/浴室/阳台/走廊/外观/其他",
      "visual_summary": "这一段画面看到了什么",
      "selling_points": ["可从画面合理看出的卖点"],
      "suggested_narration": "适合这一段的短解说"
    }}
  ],
  "warnings": ["如果画面信息不足或无法判断，请写在这里"]
}}
""".strip()

    content = [{"type": "text", "text": prompt}]
    for frame in frames:
        content.append(
            {
                "type": "text",
                "text": f"视频片段 {frame['clip_index']}，关键帧 {frame['frame_index']}，片段内约第 {frame['timestamp']} 秒，合并后全局第 {frame['global_timestamp']} 秒：",
            }
        )
        content.append(
            {
                "type": "image_url",
                "image_url": {"url": _image_data_url(Path(frame["path"])), "detail": os.getenv("OPENAI_VISION_FRAME_DETAIL", "auto")},
            }
        )

    model = (os.getenv("OPENAI_VISION_MODEL") or "gpt-4o-mini").strip()
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "你只输出可解析 JSON。"},
            {"role": "user", "content": content},
        ],
        "temperature": 0.4,
        "response_format": {"type": "json_object"},
    }
    response = requests.post(
        OPENAI_CHAT_COMPLETIONS_URL,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=payload,
        timeout=180,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"OpenAI 视觉分析失败：{response.status_code} {response.text[:500]}")
    body = response.json()
    raw_text = body.get("choices", [{}])[0].get("message", {}).get("content", "")
    data = _extract_json_object(raw_text)
    data["frames"] = frames
    data["model"] = model
    data["usage"] = body.get("usage") or {}
    data["clip_durations"] = clip_durations
    data["total_video_duration"] = data.get("total_video_duration") or total_duration
    data["target_script_chars"] = data.get("target_script_chars") or f"{target_min_chars}-{target_max_chars}"
    data["estimated_narration_seconds"] = data.get("estimated_narration_seconds") or total_duration
    raw_segments = [item for item in (data.get("timeline_segments") or []) if isinstance(item, dict)]
    raw_by_index: dict[int, dict] = {}
    for fallback_index, item in enumerate(raw_segments, start=1):
        try:
            index = int(item.get("index") or fallback_index)
        except (TypeError, ValueError):
            index = fallback_index
        raw_by_index[index] = item
    clip_by_index: dict[int, dict] = {}
    for item in data.get("clips") or []:
        if not isinstance(item, dict):
            continue
        try:
            clip_index = int(item.get("clip_index") or 0)
        except (TypeError, ValueError):
            clip_index = 0
        if clip_index:
            clip_by_index[clip_index] = item
    timeline_segments = []
    for window in fixed_timeline_windows:
        index = int(window.get("index") or len(timeline_segments) + 1)
        item = raw_by_index.get(index, {})
        script = str(item.get("script") or item.get("suggested_narration") or "").strip()
        visual_summary = str(item.get("visual_summary") or "")
        room_type = str(item.get("room_type") or item.get("room") or "空间")
        clip_fallback = clip_by_index.get(int(window.get("clip_index") or 0), {})
        if room_type in {"", "空间", "其他"} and clip_fallback:
            room_type = str(clip_fallback.get("room_type") or clip_fallback.get("room") or room_type or "空间")
        if not visual_summary and clip_fallback:
            visual_summary = str(clip_fallback.get("visual_summary") or "")
        if not script:
            script = f"这里是{room_type}区域，可以结合画面观察空间动线、收纳和实际使用感。"
        timeline_segments.append(
            {
                "index": index,
                "start": float(window["start"]),
                "end": float(window["end"]),
                "duration": float(window["duration"]),
                "room_type": room_type,
                "visual_summary": visual_summary,
                "evidence_timestamps": item.get("evidence_timestamps") if isinstance(item.get("evidence_timestamps"), list) else [window.get("frame_global_timestamp")],
                "script": script,
            }
        )
    narration_units = _merge_timeline_segments_into_narration_units(timeline_segments, target_market)
    warnings = data.get("warnings") if isinstance(data.get("warnings"), list) else []
    if narration_units and (
        len(narration_units) < minimum_segment_count
        or any(float(item.get("duration") or 0) > NARRATION_UNIT_MAX_SECONDS + 8 for item in narration_units)
    ):
        warnings.append(
            f"讲解单元偏粗：当前 {len(narration_units)} 段，建议至少 {minimum_segment_count} 段；请重新分析或手动拆分过长段落。"
        )
    timeline_chars = sum(_visible_text_length(str(item.get("script") or "")) for item in narration_units)
    if narration_units and timeline_chars < int(target_min_chars * TIMELINE_MIN_SCRIPT_RATIO):
        expanded_segments, expanded_script, expansion_meta = _expand_timeline_scripts_with_openai(
            timeline_segments=narration_units,
            suggested_script=str(data.get("suggested_script") or ""),
            overall_summary=str(data.get("overall_summary") or ""),
            target_market=target_market,
            user_notes=user_notes,
            api_key=api_key,
        )
        expanded_chars = sum(_visible_text_length(str(item.get("script") or "")) for item in expanded_segments)
        if expanded_chars > timeline_chars:
            narration_units = expanded_segments
            data["suggested_script"] = expanded_script
            data["script_expansion"] = {
                **expansion_meta,
                "before_chars": timeline_chars,
                "after_chars": expanded_chars,
                "target_min_chars": target_min_chars,
            }
        else:
            warnings.append(
                f"分段文案偏短：当前约 {timeline_chars} 字，建议至少约 {target_min_chars} 字。请手动补充重点内容后再生成。"
            )
            if expansion_meta.get("error"):
                warnings.append(f"自动扩写未成功：{expansion_meta.get('error')}")
    data["warnings"] = warnings
    data["raw_timeline_events"] = timeline_segments
    data["timeline_segments"] = narration_units
    if narration_units:
        data["suggested_script"] = "\n".join(item.get("script", "") for item in narration_units if item.get("script"))
    return data
