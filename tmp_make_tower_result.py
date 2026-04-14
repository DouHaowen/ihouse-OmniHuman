import json
from pathlib import Path

OUTPUT_DIR = Path("/app/output/1776134455_full_高淨值首選頂級塔樓TowerM")


def main():
    script_path = OUTPUT_DIR / "script.json"
    if not script_path.exists():
        raise RuntimeError(f"找不到脚本文件: {script_path}")

    script_data = json.loads(script_path.read_text(encoding="utf-8"))
    segments = []
    for idx, seg in enumerate(script_data.get("segments") or []):
        seg_copy = dict(seg)
        audio_path = OUTPUT_DIR / "audio" / f"segment_{idx:02d}_{seg.get('type', '')}.mp3"
        if audio_path.exists():
            seg_copy["audio_path"] = str(audio_path)
            seg_copy["audio_url"] = f"/public/output/{OUTPUT_DIR.name}/audio/{audio_path.name}"
        segments.append(seg_copy)

    result_data = {
        "topic": script_data.get("topic") or script_data.get("title", ""),
        "owner_username": "tai",
        "owner_display_name": "tai",
        "owner_role": "employee",
        "title": script_data.get("title", ""),
        "cover_title": script_data.get("cover_title", ""),
        "total_duration": script_data.get("total_duration", 0),
        "segment_count": len(segments),
        "script": script_data,
        "segments": segments,
        "social_post": script_data.get("social_post", ""),
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
            "target_market": "tw",
            "department_id": "real_estate",
            "avatar": {
                "id": "avatar_test_0cd3d70a.png",
                "image_url": "/public/assets/avatar_test_0cd3d70a.png",
            },
            "compose_transition_id": "fade",
            "subtitle_template_id": "classic",
        },
        "cost_entries": [],
        "cost_summary": {
            "estimated_total": 0,
            "currency": "CNY",
            "by_provider": [],
            "by_event_type": {},
            "by_market": {},
        },
    }

    (OUTPUT_DIR / "result.json").write_text(json.dumps(result_data, ensure_ascii=False, indent=2), encoding="utf-8")
    print("wrote", OUTPUT_DIR / "result.json")


if __name__ == "__main__":
    main()
