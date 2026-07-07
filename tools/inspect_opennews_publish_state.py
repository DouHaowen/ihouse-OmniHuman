#!/usr/bin/env python3
import json
from pathlib import Path


def is_opennews(result: dict) -> bool:
    workflow = result.get("workflow_config") if isinstance(result, dict) else {}
    if isinstance(workflow, dict) and workflow.get("type") == "opennews":
        return True
    if workflow.get("opennews") or workflow.get("opennews_material_only"):
        return True
    if workflow.get("digital_human_engine") == "opennews_material_only":
        return True
    title = str(result.get("title") or "")
    topic = str(result.get("topic") or "")
    return title.startswith("OpenNews") or topic.startswith("OpenNews")


def version_summary(version: dict) -> dict:
    videos = version.get("videos") if isinstance(version.get("videos"), dict) else {}
    vertical = videos.get("vertical") if isinstance(videos.get("vertical"), dict) else {}
    return {
        "market": version.get("target_market") or "",
        "title": version.get("title") or "",
        "x_records": len(version.get("x_publish_records") or []),
        "facebook_records": len(version.get("facebook_publish_records") or []),
        "x_error": version.get("x_auto_publish_error") or version.get("x_publish_error") or "",
        "facebook_error": version.get("facebook_auto_publish_error") or version.get("facebook_publish_error") or "",
        "vertical_video": vertical.get("file") or version.get("vertical_video_path") or "",
    }


def main() -> None:
    base = Path("/app/output")
    rows = []
    candidates = sorted(
        [path for path in base.iterdir() if path.is_dir() and (path / "result.json").exists()],
        key=lambda p: (p / "result.json").stat().st_mtime,
        reverse=True,
    )[:20]
    for output_dir in candidates:
        path = output_dir / "result.json"
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not is_opennews(data):
            continue
        workflow = data.get("workflow_config") if isinstance(data.get("workflow_config"), dict) else {}
        rows.append(
            {
                "dir": output_dir.name,
                "title": data.get("title") or ((data.get("script") or {}).get("title")) or data.get("topic") or "",
                "final": bool(data.get("final_video_path") or data.get("vertical_video_path") or data.get("final_video")),
                "x_auto_publish": workflow.get("x_auto_publish"),
                "facebook_auto_publish": workflow.get("facebook_auto_publish"),
                "x_records": len(data.get("x_publish_records") or []),
                "facebook_records": len(data.get("facebook_publish_records") or []),
                "x_error": data.get("x_auto_publish_error") or data.get("x_publish_error") or "",
                "facebook_error": data.get("facebook_auto_publish_error") or data.get("facebook_publish_error") or "",
                "channel": workflow.get("opennews_channel_id") or "",
                "markets": workflow.get("opennews_language_markets") or [],
                "versions": [
                    version_summary(version)
                    for version in (data.get("language_versions") or [])
                    if isinstance(version, dict)
                ],
            }
        )
    print(json.dumps(rows, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
