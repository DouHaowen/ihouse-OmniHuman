import json
import os
import sys
from pathlib import Path

sys.path.insert(0, "/app")

import app


def main() -> None:
    os.environ["OPENNEWS_MODEL_PROVIDER"] = "local"
    batch_path = Path("/app/output/opennews_batches/batches/batch_20260701_125457.json")
    batch = json.loads(batch_path.read_text(encoding="utf-8"))
    item_id = "9006ecdfe1e625e0a7"
    item = next(
        (it for it in (batch.get("items") or []) if str(it.get("batch_item_id") or "") == item_id),
        None,
    )
    if not item:
        raise SystemExit(f"missing item: {item_id}")
    user = app._external_news_user()
    job = app.create_opennews_batch_job(
        app.OPENNEWS_BATCH_DIR,
        username=user.get("username") or "admin",
        items=[item],
        options={
            "target_market": "cn",
            "department_id": user.get("department_id") or "real_estate",
            "voice_preset_id": "mandarin_female",
            "aspect_ratio": "vertical",
            "notes": "Codex end-to-end validation run",
            "youtube_auto_publish": True,
            "youtube_privacy_status": "public",
            "youtube_aspects": ["vertical"],
            "x_auto_publish": True,
            "x_aspects": ["vertical"],
            "material_strategy": "free_library_script_match",
            "external_request": {
                "x_auto_publish": True,
                "x_aspects": ["vertical"],
            },
        },
    )
    print(job.get("job_id") or "")


if __name__ == "__main__":
    main()
