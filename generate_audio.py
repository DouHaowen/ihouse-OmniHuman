"""
TTS配音模块
输入文案 → 输出音频文件
使用 MiniMax Speech TTS API
"""

import os
import requests
from dotenv import load_dotenv
from opencc import OpenCC

load_dotenv(override=False)

MINIMAX_API_KEY = os.getenv("MINIMAX_API_KEY")
MINIMAX_TTS_URL = "https://api.minimaxi.com/v1/t2a_v2"
T2S_CONVERTER = OpenCC("t2s")


def _should_normalize_to_simplified(text: str) -> bool:
    if not text:
        return False
    # Japanese copy should bypass Chinese conversion.
    if any("\u3040" <= ch <= "\u30ff" for ch in text):
        return False
    return any("\u4e00" <= ch <= "\u9fff" for ch in text)


def _prepare_tts_text(text: str) -> str:
    normalized = (text or "").strip()
    if _should_normalize_to_simplified(normalized):
        return T2S_CONVERTER.convert(normalized)
    return normalized


def generate_audio(
    text: str,
    output_path: str,
    voice: str = "Chinese (Mandarin)_Warm_Bestie",
    speed: float = 1.2,
    volume: float = 1.0,
) -> str:
    """
    输入文案，生成音频文件

    voice: MiniMax voice_id，可在 MiniMax 平台获取
    """
    tts_text = _prepare_tts_text(text)
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

    audio_hex = result["data"]["audio"]
    audio_bytes = bytes.fromhex(audio_hex)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "wb") as f:
        f.write(audio_bytes)

    print(f"✅ 配音已保存：{output_path}")
    return output_path


def generate_all_audio(
    segments: list,
    output_dir: str,
    voice: str = "Chinese (Mandarin)_Warm_Bestie",
    speed: float = 1.2,
    volume: float = 1.0,
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

        audio_path = generate_audio(script, output_path, voice, speed=speed, volume=volume)

        seg_with_audio = seg.copy()
        seg_with_audio["audio_path"] = audio_path
        audio_segments.append(seg_with_audio)

    print(f"✅ 全部配音完成，共 {len(audio_segments)} 段")
    return audio_segments


if __name__ == "__main__":
    test_text = "大家好，欢迎收看今日日本房产知识分享。"
    generate_audio(test_text, "/tmp/test_audio.mp3")
