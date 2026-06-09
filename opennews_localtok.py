"""Persistent state for the OpenNews -> LocalTok approval/publish workflow."""

from __future__ import annotations

import hashlib
import json
import threading
import time
from pathlib import Path
from typing import Any


_LOCK = threading.Lock()


def _ensure_root(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "proposals").mkdir(parents=True, exist_ok=True)


def _proposal_path(root: Path, proposal_id: str) -> Path:
    return root / "proposals" / f"{proposal_id}.json"


def _read_json(path: Path, fallback: Any) -> Any:
    try:
        if not path.exists():
            return fallback
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return fallback


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def make_local_proposal_id(username: str) -> str:
    basis = f"{username}|{time.time()}"
    return f"localtok_{int(time.time())}_{hashlib.sha1(basis.encode()).hexdigest()[:8]}"


def create_proposal(root: Path, proposal: dict) -> dict:
    _ensure_root(root)
    proposal_id = str(proposal.get("proposal_id") or proposal.get("local_proposal_id") or make_local_proposal_id(""))
    now = time.time()
    payload = {
        "local_proposal_id": proposal_id,
        "proposal_id": proposal.get("proposal_id") or "",
        "username": proposal.get("username") or "",
        "status": proposal.get("status") or "proposed",
        "message": proposal.get("message") or "LocalTok 提案已提交，等待审核。",
        "created_at": now,
        "updated_at": now,
        "titles": proposal.get("titles") or [],
        "summary": proposal.get("summary") or "",
        "options": proposal.get("options") or [],
        "items": proposal.get("items") or [],
        "dup_titles": proposal.get("dup_titles") or [],
        "decision": {},
        "selected_item": {},
        "task_id": "",
        "history_id": "",
        "publish_result": {},
        "error": "",
        "settings": proposal.get("settings") or {},
    }
    with _LOCK:
        _write_json(_proposal_path(root, proposal_id), payload)
    return payload


def load_proposal(root: Path, local_proposal_id: str) -> dict | None:
    _ensure_root(root)
    with _LOCK:
        payload = _read_json(_proposal_path(root, local_proposal_id), None)
    return payload if isinstance(payload, dict) else None


def update_proposal(root: Path, local_proposal_id: str, updater) -> dict:
    _ensure_root(root)
    with _LOCK:
        payload = _read_json(_proposal_path(root, local_proposal_id), {})
        if not isinstance(payload, dict):
            payload = {"local_proposal_id": local_proposal_id}
        updater(payload)
        payload["updated_at"] = time.time()
        _write_json(_proposal_path(root, local_proposal_id), payload)
        return payload


def list_proposals(root: Path, *, limit: int = 20) -> list[dict]:
    _ensure_root(root)
    with _LOCK:
        paths = sorted((root / "proposals").glob("localtok_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        proposals = []
        for path in paths[: max(1, min(int(limit or 20), 80))]:
            payload = _read_json(path, {})
            if isinstance(payload, dict):
                proposals.append(payload)
        return proposals
