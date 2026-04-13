"""
TTS配音模块
输入文案 → 输出音频文件
使用 MiniMax Speech TTS API
"""

import os
import re
import subprocess
import tempfile
from pathlib import Path
import requests
from dotenv import load_dotenv
from opencc import OpenCC

load_dotenv(override=False)

MINIMAX_API_KEY = os.getenv("MINIMAX_API_KEY")
MINIMAX_TTS_URL = "https://api.minimaxi.com/v1/t2a_v2"
T2S_CONVERTER = OpenCC("t2s")


def _contains_japanese_kana(text: str) -> bool:
    return any("\u3040" <= ch <= "\u30ff" for ch in text or "")


def _split_mixed_language_segments(text: str, language: str = "") -> list[str]:
    normalized = (text or "").strip()
    if not normalized or str(language or "").lower().startswith("ja"):
        return [normalized] if normalized else []
    if not _contains_japanese_kana(normalized):
        return [normalized]

    pattern = re.compile(r"(「[\u3040-\u30ffー・]+」|[\u3040-\u30ffー・]{2,})")
    parts: list[str] = []
    cursor = 0
    for match in pattern.finditer(normalized):
        start, end = match.span()
        if start > cursor:
            prefix = normalized[cursor:start].strip()
            if prefix:
                parts.append(prefix)
        token = match.group(0).strip()
        if token:
            parts.append(token)
        cursor = end
    if cursor < len(normalized):
        suffix = normalized[cursor:].strip()
        if suffix:
            parts.append(suffix)
    return parts or [normalized]


def _should_normalize_to_simplified(text: str) -> bool:
    if not text:
        return False
    # Japanese copy should bypass Chinese conversion.
    if any("\u3040" <= ch <= "\u30ff" for ch in text):
        return False
    return any("\u4e00" <= ch <= "\u9fff" for ch in text)


def _prepare_tts_text(text: str, language: str = "") -> str:
    normalized = (text or "").strip()
    if _should_normalize_to_simplified(normalized):
        return T2S_CONVERTER.convert(normalized)
    return normalized


def _request_tts_audio_bytes(
    text: str,
    voice: str,
    speed: float,
    volume: float,
    language: str = "",
) -> bytes:
    tts_text = _prepare_tts_text(text, language=language)
    print(f"🎙️ 正在生成配音：{tts_text[:30]}...")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {MINIMAX_API_KEY}",
    }

    payload = {
        "model": "speech-2.8-hd",
        "text": tts_text,
        "stream": False,
        "voice_setting": {
            "voice_id": voice,
            "speed": speed,
            "vol": volume,
            "pitch": 0,
        },
        "audio_setting": {
            "sample_rate": 32000,
            "bitrate": 128000,
            "format": "mp3",
            "channel": 1,
        },
    }

    response = requests.post(MINIMAX_TTS_URL, headers=headers, json=payload)
    response.raise_for_status()
    result = response.json()

    status_code = result.get("base_resp", {}).get("status_code", -1)
    if status_code != 0:
        status_msg = result.get("base_resp", {}).get("status_msg", "unknown error")
        raise Exception(f"MiniMax TTS 失败: {status_msg} (code={status_code})")

    return bytes.fromhex(result["data"]["audio"])


def _concat_audio_segments(output_path: str, audio_segments: list[bytes]):
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        list_path = tmp / "segments.txt"
        lines = []
        for index, audio_bytes in enumerate(audio_segments):
            chunk_path = tmp / f"chunk_{index:02d}.mp3"
            chunk_path.write_bytes(audio_bytes)
            lines.append(f"file '{chunk_path.as_posix()}'")
        list_path.write_text("\n".join(lines), encoding="utf-8")
        subprocess.run(
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
                str(output),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


def generate_audio(
    text: str,
    output_path: str,
    voice: str = "Chinese (Mandarin)_Warm_Bestie",
    speed: float = 1.2,
    volume: float = 1.0,
    language: str = "",
) -> str:
    """
    输入文案，生成音频文件

    voice: MiniMax voice_id，可在 MiniMax 平台获取
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    parts = _split_mixed_language_segments(text, language=language)
    if len(parts) <= 1:
        audio_bytes = _request_tts_audio_bytes(text, voice, speed, volume, language=language)
        with open(output_path, "wb") as f:
            f.write(audio_bytes)
    else:
        print(f"🧩 检测到混合语种文案，拆分为 {len(parts)} 段 TTS：{parts}")
        audio_segments = [
            _request_tts_audio_bytes(part, voice, speed, volume, language=language)
            for part in parts
        ]
        _concat_audio_segments(output_path, audio_segments)

    print(f"✅ 配音已保存：{output_path}")
    return output_path


def generate_all_audio(
    segments: list,
    output_dir: str,
    voice: str = "Chinese (Mandarin)_Warm_Bestie",
    speed: float = 1.2,
    volume: float = 1.0,
    language: str = "",
) -> list:
    """
    批量生成所有段落的配音
    返回带音频路径的segments列表
    """
    audio_segments = []

    for i, seg in enumerate(segments):
        script = seg.get("script", "")
        if not script:
            continue

        seg_type = seg.get("type", "")
        filename = f"segment_{i:02d}_{seg_type}.mp3"
        output_path = os.path.join(output_dir, "audio", filename)

        os.makedirs(os.path.join(output_dir, "audio"), exist_ok=True)

        audio_path = generate_audio(script, output_path, voice, speed=speed, volume=volume, language=language)

        seg_with_audio = seg.copy()
        seg_with_audio["audio_path"] = audio_path
        audio_segments.append(seg_with_audio)

    print(f"✅ 全部配音完成，共 {len(audio_segments)} 段")
    return audio_segments


if __name__ == "__main__":
    test_text = "大家好，欢迎收看今日日本房产知识分享。"
    generate_audio(test_text, "/tmp/test_audio.mp3")
