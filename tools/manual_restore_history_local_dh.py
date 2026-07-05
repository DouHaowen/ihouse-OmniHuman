#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

from app import (
    INFINITETALK_ENGINE_ID,
    _combine_prompt,
    _generate_digital_human_video_by_engine,
    _get_avatar_option,
    _sync_live_task_result,
    _switch_5090_gpu_profile,
)


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: manual_restore_history_local_dh.py <result_json> [segment_index ...]", file=sys.stderr)
        return 2

    result_path = Path(sys.argv[1]).resolve()
    if not result_path.exists():
        print(f"result json not found: {result_path}", file=sys.stderr)
        return 2

    requested_segments = [int(item) for item in sys.argv[2:]] or [6, 9]
    output_dir = result_path.parent
    result = json.loads(result_path.read_text(encoding="utf-8"))
    workflow_config = result.get("workflow_config") or {}
    engine_id = str(workflow_config.get("digital_human_engine") or "").strip()
    if engine_id != INFINITETALK_ENGINE_ID:
        print(f"history engine is not {INFINITETALK_ENGINE_ID}: {engine_id}", file=sys.stderr)
        return 2

    target_market = str(workflow_config.get("target_market") or "cn").strip() or "cn"
    avatar_cfg = workflow_config.get("avatar") or {}
    avatar_option = _get_avatar_option(str(avatar_cfg.get("id") or "").strip(), target_market_id=target_market)
    image_path = str((avatar_option or {}).get("image_path") or "").strip()
    if not image_path or not Path(image_path).exists():
        print(f"avatar image missing: {image_path}", file=sys.stderr)
        return 2

    style_prompt = str((avatar_option or {}).get("style_prompt") or "").strip()
    segments = result.get("segments") or []
    if not segments:
        print("history has no segments", file=sys.stderr)
        return 2

    digital_dir = output_dir / "digital_human"
    digital_dir.mkdir(parents=True, exist_ok=True)

    profile_result = _switch_5090_gpu_profile("digital_intro", reason=f"manual restore {output_dir.name}")
    print(json.dumps({"profile_switch": profile_result}, ensure_ascii=False))

    try:
        for segment_index in requested_segments:
            if segment_index < 1 or segment_index > len(segments):
                raise RuntimeError(f"segment out of range: {segment_index}")
            segment = segments[segment_index - 1]
            if segment.get("type") != "digital_human":
                raise RuntimeError(f"segment {segment_index} is not digital_human")

            audio_path = str(segment.get("audio_path") or "").strip()
            if not audio_path or not Path(audio_path).exists():
                raise RuntimeError(f"segment {segment_index} audio missing: {audio_path}")

            video_output = digital_dir / f"dh_{segment_index - 1:02d}_manual5090_{int(time.time())}.mp4"
            print(
                json.dumps(
                    {
                        "segment_index": segment_index,
                        "audio_path": audio_path,
                        "image_path": image_path,
                        "output_path": str(video_output),
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
            video_path = _generate_digital_human_video_by_engine(
                engine_id=engine_id,
                image_url="",
                image_path=image_path,
                audio_url=str(segment.get("audio_url") or "").strip(),
                audio_path=audio_path,
                output_path=str(video_output),
                prompt=_combine_prompt(style_prompt, str(segment.get("action") or "").strip()),
                task_id=output_dir.name,
                segment_index=segment_index,
            )
            segment["video_path"] = video_path
            segment["digital_human_engine"] = engine_id
            print(json.dumps({"segment_index": segment_index, "video_path": video_path}, ensure_ascii=False), flush=True)
    finally:
        _switch_5090_gpu_profile("material", reason=f"manual restore finished {output_dir.name}")

    result["final_video_path"] = ""
    result["subtitle_path"] = ""
    result_path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    _sync_live_task_result(str(output_dir), result)
    print(json.dumps({"ok": True, "result_path": str(result_path)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
