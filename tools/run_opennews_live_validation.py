import json
import os
import sys
from pathlib import Path

sys.path.insert(0, "/app")

import app


DEFAULT_PUBLIC_BASE_URL = "https://aiagent.office.ihousejapan.cn"


def _latest_pending_item() -> tuple[Path, dict]:
    batches_dir = Path("/app/output/opennews_batches/batches")
    batch_paths = sorted(batches_dir.glob("batch_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    for batch_path in batch_paths:
        batch = json.loads(batch_path.read_text(encoding="utf-8"))
        for item in batch.get("items") or []:
            if not isinstance(item, dict):
                continue
            status = str(item.get("status") or "").strip().lower()
            if status in {"", "pending", "failed"}:
                return batch_path, item
    raise SystemExit("no pending item found in latest opennews batches")


def _resolve_item() -> tuple[Path, dict]:
    wanted_item_id = str(os.getenv("OPENNEWS_ITEM_ID") or "").strip()
    wanted_batch = str(os.getenv("OPENNEWS_BATCH_FILE") or "").strip()
    batches_dir = Path("/app/output/opennews_batches/batches")
    if wanted_batch:
        batch_path = batches_dir / wanted_batch
        if not batch_path.exists():
            raise SystemExit(f"batch file not found: {batch_path}")
        batch = json.loads(batch_path.read_text(encoding="utf-8"))
        for item in batch.get("items") or []:
            if str(item.get("batch_item_id") or "") == wanted_item_id or not wanted_item_id:
                return batch_path, item
        raise SystemExit(f"item not found in batch: {wanted_item_id}")
    if wanted_item_id:
        for batch_path in sorted(batches_dir.glob("batch_*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
            batch = json.loads(batch_path.read_text(encoding="utf-8"))
            for item in batch.get("items") or []:
                if str(item.get("batch_item_id") or "") == wanted_item_id:
                    return batch_path, item
        raise SystemExit(f"item not found: {wanted_item_id}")
    return _latest_pending_item()


def main() -> None:
    os.environ["OPENNEWS_MODEL_PROVIDER"] = os.getenv("OPENNEWS_MODEL_PROVIDER", "local")
    batch_path, item = _resolve_item()
    public_base_url = str(os.getenv("OPENNEWS_PUBLIC_BASE_URL") or DEFAULT_PUBLIC_BASE_URL).strip() or DEFAULT_PUBLIC_BASE_URL
    user = app._external_news_user()

    title = (
        item.get("title")
        or ((item.get("article") or {}).get("title") if isinstance(item.get("article"), dict) else "")
        or "OpenNews validation"
    )
    item_id = str(item.get("batch_item_id") or "")
    print(json.dumps({
        "selected_batch": batch_path.name,
        "selected_item_id": item_id,
        "selected_title": title,
    }, ensure_ascii=False), flush=True)

    job = app.create_opennews_batch_job(
        app.OPENNEWS_BATCH_DIR,
        username=user.get("username") or "admin",
        items=[item],
        options={
            "target_market": str(os.getenv("OPENNEWS_TARGET_MARKET") or "cn"),
            "department_id": user.get("department_id") or "real_estate",
            "voice_preset_id": str(os.getenv("OPENNEWS_VOICE_PRESET_ID") or "mandarin_female"),
            "aspect_ratio": "vertical",
            "notes": str(os.getenv("OPENNEWS_NOTES") or "Codex live full-chain validation"),
            "youtube_auto_publish": True,
            "youtube_privacy_status": str(os.getenv("OPENNEWS_YOUTUBE_PRIVACY_STATUS") or "public"),
            "youtube_aspects": ["vertical"],
            "x_auto_publish": True,
            "x_aspects": ["vertical"],
            "material_strategy": str(os.getenv("OPENNEWS_MATERIAL_STRATEGY") or "free_library_script_match"),
            "external_request": {
                "x_auto_publish": True,
                "x_aspects": ["vertical"],
            },
        },
    )
    job_id = str(job.get("job_id") or "")
    print(json.dumps({"job_id": job_id, "message": "job created"}, ensure_ascii=False), flush=True)

    app._run_opennews_external_produce_job(
        job_id,
        user=dict(user),
        public_base_url=public_base_url,
    )

    final_job = app.load_opennews_batch_job(app.OPENNEWS_BATCH_DIR, job_id) or {}
    print(json.dumps({
        "job_id": job_id,
        "status": final_job.get("status"),
        "message": final_job.get("message"),
        "items": final_job.get("items") or [],
    }, ensure_ascii=False, indent=2, default=str), flush=True)


if __name__ == "__main__":
    main()
