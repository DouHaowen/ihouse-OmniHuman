"""物件(房源)接口自动化适配层。

从公司房源接口拉取房源记录，抽取可用于「房源实拍成片」的数据（实拍视频 URL + 房源资料文本），
并维护逐条状态（done/failed/attempts）。实际制作复用 app.py 的房源视频管线，本模块不依赖 app。
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Optional

import requests


DEFAULT_PROPERTY_API_URL = "https://bukken.office.ihousejapan.cn/api/properties/all"


def property_auto_config() -> dict:
    return {
        "api_url": (os.getenv("PROPERTY_API_URL") or os.getenv("BUKKEN_API_URL") or DEFAULT_PROPERTY_API_URL).strip(),
        "api_token": (os.getenv("PROPERTY_API_TOKEN") or os.getenv("BUKKEN_API_TOKEN") or "").strip(),
        "timeout_seconds": max(5, int(os.getenv("PROPERTY_API_TIMEOUT_SECONDS", "40") or "40")),
    }


def property_auto_is_configured() -> bool:
    return bool(property_auto_config()["api_token"])


def _auth_headers(config: Optional[dict] = None) -> dict:
    config = config or property_auto_config()
    return {"Authorization": f"Bearer {config['api_token']}"}


def fetch_property_records(config: Optional[dict] = None) -> list[dict]:
    config = config or property_auto_config()
    if not config["api_token"]:
        raise RuntimeError("未配置 PROPERTY_API_TOKEN，无法拉取房源数据")
    resp = requests.get(config["api_url"], headers=_auth_headers(config), timeout=config["timeout_seconds"])
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict):
        records = data.get("properties") or data.get("records") or data.get("data")
    elif isinstance(data, list):
        records = data
    else:
        records = None
    return [r for r in (records or []) if isinstance(r, dict)]


def _basic_info_to_notes(bi: dict) -> str:
    """把房源资料拼成给 AI 写文案用的说明文本（简体中文字段名）。"""
    if not isinstance(bi, dict):
        return ""
    fields = [
        ("bukken_name", "物件名"), ("bukken_type", "类型"), ("address", "地址"),
        ("station", "交通"), ("price", "价格"), ("layout", "户型"), ("area", "面积"),
        ("land_area", "土地面积"), ("building_area", "建物面积"), ("built", "建造年"),
        ("total_floors", "楼层"), ("structure", "结构"), ("direction", "朝向"),
        ("parking", "停车"), ("notes", "备注"),
    ]
    lines = []
    for key, label in fields:
        val = bi.get(key)
        if val is None or str(val).strip() in {"", "0"}:
            continue
        lines.append(f"{label}：{str(val).strip()}")
    return "\n".join(lines)


def extract_property(record: dict) -> Optional[dict]:
    """抽取一条房源：需要有 id 且至少一个实拍视频。缺视频则返回 None（无法做实拍成片）。"""
    record_id = str(record.get("id") or "").strip()
    if not record_id:
        return None
    bi = record.get("basic_info") if isinstance(record.get("basic_info"), dict) else {}
    media = record.get("media") if isinstance(record.get("media"), list) else []
    video_urls = [str(m.get("url") or "").strip() for m in media
                  if isinstance(m, dict) and str(m.get("type") or "").lower() == "video" and str(m.get("url") or "").strip()]
    if not video_urls:
        return None
    return {
        "record_id": record_id,
        "name": str(bi.get("bukken_name") or record.get("id") or "").strip() or record_id,
        "video_urls": video_urls,
        "notes_text": _basic_info_to_notes(bi),
        "updated_at": str(record.get("updated_at") or record.get("created_at") or "").strip(),
    }


# ── 逐条状态存储（与 topic_auto 相同结构：成功才 done、失败可重试） ──

def _state_path(store_dir: str) -> Path:
    return Path(store_dir) / "property_state.json"


def load_property_state(store_dir: str) -> dict:
    path = _state_path(store_dir)
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict) and isinstance(data.get("records"), dict):
                data.setdefault("reconciled_tasks", [])
                return data
        except Exception:
            pass
    return {"records": {}, "reconciled_tasks": [], "updated_at": time.time()}


def save_property_state(store_dir: str, state: dict) -> None:
    path = _state_path(store_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at"] = time.time()
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def mark_record_result(state: dict, record_id: str, *, success: bool, task_id: str = "",
                       name: str = "", max_attempts: int = 3) -> None:
    records = state.setdefault("records", {})
    entry = records.get(record_id) or {"status": "", "attempts": 0, "last_task": "", "name": name, "updated_at": 0}
    if name and not entry.get("name"):
        entry["name"] = name
    entry["last_task"] = task_id or entry.get("last_task") or ""
    entry["updated_at"] = time.time()
    if success:
        entry["status"] = "done"
    else:
        entry["attempts"] = int(entry.get("attempts") or 0) + 1
        entry["status"] = "skipped" if entry["attempts"] >= max_attempts else "failed"
    records[record_id] = entry


def select_pending_properties(records: list[dict], state: dict, *, max_attempts: int = 3, limit: int = 20) -> list[dict]:
    rec_state = state.get("records") or {}
    selected: list[dict] = []
    for record in records:
        ex = extract_property(record)
        if not ex:
            continue
        st = rec_state.get(ex["record_id"]) or {}
        status = str(st.get("status") or "")
        if status in {"done", "skipped"}:
            continue
        if int(st.get("attempts") or 0) >= max_attempts:
            continue
        selected.append(ex)
        if len(selected) >= max(1, limit):
            break
    return selected


def download_videos(video_urls: list[str], dest_dir: Path, config: Optional[dict] = None) -> list[str]:
    """下载房源实拍视频到本地，返回本地路径列表。"""
    config = config or property_auto_config()
    dest_dir.mkdir(parents=True, exist_ok=True)
    paths: list[str] = []
    for i, url in enumerate(video_urls, start=1):
        try:
            r = requests.get(url, headers=_auth_headers(config), timeout=config["timeout_seconds"], stream=True)
            r.raise_for_status()
            ext = ".mp4"
            for cand in (".mp4", ".mov", ".m4v", ".webm"):
                if cand in url.lower():
                    ext = cand
                    break
            out = dest_dir / f"clip_{i:02d}{ext}"
            with out.open("wb") as fh:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    if chunk:
                        fh.write(chunk)
            if out.stat().st_size > 0:
                paths.append(str(out))
        except Exception as exc:
            print(f"[property_auto] 下载视频失败 {url[:60]}: {exc!r}", flush=True)
    return paths
