import json
from pathlib import Path

import app
from fetch_materials import fetch_all_materials
from generate_digital_human import generate_digital_human_video
from tos_uploader import upload_file_and_get_url
from video_composer import compose_history_video

OUTPUT_DIR = Path("/app/output/1775725774_full_日本是众所周知的地震大国他们的房子都是")
TOPIC = "日本是众所周知的地震大国 他们的房子都是怎么做耐震的"
AVATAR_ID = "avatar_test_0cd3d70a.png"
TARGET_MARKET = "cn"


def main():
    script_data = json.loads((OUTPUT_DIR / "script.json").read_text())
    segments = script_data.get("segments", [])
    avatar_option = app._get_avatar_option(AVATAR_ID)
    if not avatar_option:
        raise RuntimeError(f"找不到主播图: {AVATAR_ID}")

    image_url = upload_file_and_get_url(avatar_option["image_path"], key_prefix="full/image")
    print("avatar", AVATAR_ID, image_url)

    audio_segments = []
    for index, seg in enumerate(segments):
        seg_copy = dict(seg)
        audio_path = OUTPUT_DIR / "audio" / f"segment_{index:02d}_{seg.get('type', '')}.mp3"
        if audio_path.exists():
            seg_copy["audio_path"] = str(audio_path)
            seg_copy["audio_url"] = upload_file_and_get_url(str(audio_path), key_prefix="full/audio")
        seg_copy["target_market"] = TARGET_MARKET
        audio_segments.append(seg_copy)

    segments_with_dh = []
    for index, seg in enumerate(audio_segments):
        if seg.get("type") != "digital_human":
            segments_with_dh.append(seg)
            continue

        video_output = OUTPUT_DIR / "digital_human" / f"dh_{index:02d}.mp4"
        video_output.parent.mkdir(parents=True, exist_ok=True)
        seg_copy = dict(seg)
        if video_output.exists():
            print("reuse", video_output)
            seg_copy["video_path"] = str(video_output)
        else:
            print("generate", index, seg.get("script", "")[:24])
            seg_copy["video_path"] = generate_digital_human_video(
                image_url=image_url,
                audio_url=seg.get("audio_url"),
                output_path=str(video_output),
                prompt=app._combine_prompt(avatar_option.get("style_prompt", ""), seg.get("action", "")),
            )
        segments_with_dh.append(seg_copy)

    final_segments = fetch_all_materials(segments=segments_with_dh, output_dir=str(OUTPUT_DIR))
    result_data = {
        "topic": TOPIC,
        "owner_username": "admin",
        "owner_display_name": "管理员",
        "owner_role": "admin",
        "title": script_data.get("title", ""),
        "cover_title": script_data.get("cover_title", ""),
        "total_duration": script_data.get("total_duration", 0),
        "segment_count": len(final_segments),
        "script": script_data,
        "segments": final_segments,
        "social_post": app._get_social_post(script_data, TARGET_MARKET),
        "workflow_config": {
            "voice_preset": {
                "id": "mandarin_female",
                "name": "温润女声",
                "subtitle": "中文普通话",
                "selected_speed": 1.1,
                "selected_volume": 1.0,
                "language": "简体中文",
            },
            "web_search_enabled": False,
            "target_market": TARGET_MARKET,
            "department_id": "real_estate",
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
    print("done", OUTPUT_DIR / "result.json")


if __name__ == "__main__":
    main()
