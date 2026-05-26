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
    return sorted({max(0.0, min(duration - 0.1, value)) for value in [0.5, duration / 2, max(0.5, duration - 0.5)]})


def extract_property_video_frames(video_paths: list[Path], work_dir: Path, max_frames_per_video: int = 3) -> list[dict]:
    frames_dir = work_dir / "frames"
    frames_dir.mkdir(parents=True, exist_ok=True)
    frames: list[dict] = []
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
                    "scale='min(768,iw)':-2",
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
                        "path": str(frame_path),
                    }
                )
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

    frames = extract_property_video_frames(video_paths, work_dir)
    if not frames:
        raise RuntimeError("没有成功抽取到视频关键帧")

    language = _target_language(target_market)
    clip_durations = [round(_ffprobe_duration(path), 2) for path in video_paths]
    total_duration = round(sum(clip_durations), 2)
    min_cps, max_cps = _chars_per_second(target_market)
    target_min_chars = int(max(20, total_duration * min_cps))
    target_max_chars = int(max(target_min_chars + 10, total_duration * max_cps))
    prompt = f"""
你是 iHouse 的房源实拍视频销售解说助手。请优先根据销售人员填写的“AI 分析补充信息”生成解说文案，视频关键帧只作为辅助理解画面顺序和校验素材内容。

输出语言：{language}
销售人员填写的房源信息、素材描述和重点介绍要求：
{user_notes or "未填写。请只根据视频画面做保守解说，不要编造价格、面积、交通、楼层、朝向等信息。"}

上传视频会按原顺序合并成一条完整看房视频。
每个片段原始时长：{clip_durations} 秒
合并后总时长：{total_duration} 秒
请让 suggested_script 的配音时长尽量匹配总视频时长，目标字数控制在 {target_min_chars} 到 {target_max_chars} 字之间。
不要写成逐段列表，要写成一整段自然连贯的看房解说。

生成原则：
1. 销售补充信息里明确写到的房源特点、客户关注点和重点卖点，要优先体现在 suggested_script 里。
2. 视频画面里能看到的空间顺序、采光、收纳、装修、动线等，可以作为辅助描述。
3. 如果销售补充信息与画面不完全一致，不要直接否定销售；在 warnings 里提醒“画面未明显体现该信息”，正文里用更稳妥的说法。
4. 不要编造销售没有提供、画面也看不出来的信息，例如具体价格、面积、车站距离、楼层、朝向、收益率。
5. 文案要像销售发给客户的自然讲解，不要像广告硬广，也不要像 AI 列提纲。

请严格输出 JSON，不要输出 Markdown：
{{
  "overall_summary": "整体视频内容概述",
  "suggested_script": "完整销售解说文案，适合直接配音，语气自然，不夸大，不编造价格、面积、车站距离等画面里看不出来的信息",
  "total_video_duration": {total_duration},
  "target_script_chars": "{target_min_chars}-{target_max_chars}",
  "estimated_narration_seconds": {total_duration},
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
                "text": f"视频片段 {frame['clip_index']}，关键帧 {frame['frame_index']}，约第 {frame['timestamp']} 秒：",
            }
        )
        content.append(
            {
                "type": "image_url",
                "image_url": {"url": _image_data_url(Path(frame["path"])), "detail": "low"},
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
    return data
