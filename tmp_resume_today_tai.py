import json
from pathlib import Path

import app
from fetch_materials import fetch_all_materials
from generate_digital_human import generate_digital_human_video
from tos_uploader import upload_file_and_get_url
from video_composer import compose_history_video

OUTPUT_DIR = Path("/app/output/1776134455_full_高淨值首選頂級塔樓TowerM")
AVATAR_ID = "avatar_test_0cd3d70a.png"
TARGET_MARKET = "tw"
DEPARTMENT_ID = "real_estate"
OWNER_USERNAME = "tai"
OWNER_DISPLAY_NAME = "tai"


def _generate_dh_with_retry(image_url: str, audio_url: str, output_path: str, prompt: str, retries: int = 12, delay: int = 20) -> str:
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return generate_digital_human_video(
                image_url=image_url,
                audio_url=audio_url,
                output_path=output_path,
                prompt=prompt,
            )
        except Exception as exc:
            last_error = exc
            message = str(exc)
            if "50430" not in message and "Concurrent Limit" not in message and "API Concurrent Limit" not in message:
                raise
            print(f"digital human 并发受限，{delay} 秒后重试 ({attempt}/{retries})...", flush=True)
            import time
            time.sleep(delay)
    raise RuntimeError(f"数字人任务连续重试失败: {last_error}")


def main():
    script_path = OUTPUT_DIR / "script.json"
    if not script_path.exists():
        raise RuntimeError(f"找不到脚本文件: {script_path}")

    script_data = json.loads(script_path.read_text(encoding="utf-8"))
    segments = list(script_data.get("segments") or [])
    if not segments:
        raise RuntimeError("脚本里没有分镜段落，无法继续")

    avatar_option = app._get_avatar_option(AVATAR_ID)
    if not avatar_option:
        raise RuntimeError(f"找不到主播图: {AVATAR_ID}")

    image_url = upload_file_and_get_url(avatar_option["image_path"], key_prefix="full/image")
    print("avatar", AVATAR_ID, image_url, flush=True)

    prepared_segments = []
    for index, seg in enumerate(segments):
      seg_copy = dict(seg)
      audio_path = OUTPUT_DIR / "audio" / f"segment_{index:02d}_{seg.get('type', '')}.mp3"
      if audio_path.exists():
          seg_copy["audio_path"] = str(audio_path)
          seg_copy["audio_url"] = upload_file_and_get_url(str(audio_path), key_prefix="full/audio")
      prepared_segments.append(seg_copy)

    digital_segments = []
    for index, seg in enumerate(prepared_segments):
        if seg.get("type") != "digital_human":
            digital_segments.append(seg)
            continue

        video_output = OUTPUT_DIR / "digital_human" / f"dh_{index:02d}.mp4"
        video_output.parent.mkdir(parents=True, exist_ok=True)
        if video_output.exists():
            print("reuse video", video_output, flush=True)
            seg["video_path"] = str(video_output)
        else:
            print("generate video", index, seg.get("script", "")[:24], flush=True)
            seg["video_path"] = _generate_dh_with_retry(
                image_url=image_url,
                audio_url=seg.get("audio_url"),
                output_path=str(video_output),
                prompt=app._combine_prompt(avatar_option.get("style_prompt", ""), seg.get("action", "")),
            )
        digital_segments.append(seg)

    final_segments = fetch_all_materials(segments=digital_segments, output_dir=str(OUTPUT_DIR))
    result_data = {
        "topic": script_data.get("topic") or script_data.get("title", ""),
        "owner_username": OWNER_USERNAME,
        "owner_display_name": OWNER_DISPLAY_NAME,
        "owner_role": "employee",
        "title": script_data.get("title", ""),
        "cover_title": script_data.get("cover_title", ""),
        "total_duration": script_data.get("total_duration", 0),
        "segment_count": len(final_segments),
        "script": script_data,
        "segments": final_segments,
        "social_post": script_data.get("social_post") or app._get_social_post(script_data, TARGET_MARKET),
        "workflow_config": {
            "voice_preset": {
                "id": "taiwan_clone",
                "name": "みん音色",
                "subtitle": "中文台湾语",
                "selected_speed": 1.1,
                "selected_volume": 1.0,
                "language": "繁體中文",
            },
            "web_search_enabled": True,
            "target_market": TARGET_MARKET,
            "department_id": DEPARTMENT_ID,
            "avatar": {
                "id": AVATAR_ID,
                "image_url": "/public/assets/avatar_test_0cd3d70a.png",
            },
            "compose_transition_id": "fade",
            "subtitle_template_id": "classic",
        },
        "cost_entries": [],
        "cost_summary": app._empty_cost_summary(),
    }

    result_data.update(
        compose_history_video(
            str(OUTPUT_DIR),
            result_data,
            transition_id="fade",
            subtitle_template_id="classic",
        )
    )

    (OUTPUT_DIR / "result.json").write_text(json.dumps(result_data, ensure_ascii=False, indent=2), encoding="utf-8")
    print("done", result_data.get("final_video_path"), flush=True)


if __name__ == "__main__":
    main()
