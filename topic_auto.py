"""话题(选题)接口自动化适配层。

从同事提供的话题采集接口拉取选题记录，抽取可用于数字人视频的选题，
并维护去重存储（已提交制作的话题 id）。批次的实际创建/生产复用 app.py 里
现有的"批量数字人"管线，本模块只负责拉取、抽取、去重与配置，不依赖 app 内部。
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Optional

import requests


DEFAULT_TOPIC_API_URL = "https://topic.office.ihousejapan.cn/api/topic-collector/records/all"


def topic_auto_config() -> dict:
    return {
        "api_url": (os.getenv("TOPIC_COLLECTOR_API_URL") or DEFAULT_TOPIC_API_URL).strip(),
        "api_token": (os.getenv("TOPIC_COLLECTOR_API_TOKEN") or "").strip(),
        "timeout_seconds": max(5, int(os.getenv("TOPIC_COLLECTOR_TIMEOUT_SECONDS", "30") or "30")),
    }


def topic_auto_is_configured() -> bool:
    return bool(topic_auto_config()["api_token"])


def fetch_topic_records(config: Optional[dict] = None) -> list[dict]:
    """拉取话题接口的全部记录。返回 records 列表。"""
    config = config or topic_auto_config()
    if not config["api_token"]:
        raise RuntimeError("未配置 TOPIC_COLLECTOR_API_TOKEN，无法拉取话题选题")
    response = requests.get(
        config["api_url"],
        headers={"Authorization": f"Bearer {config['api_token']}"},
        timeout=config["timeout_seconds"],
    )
    response.raise_for_status()
    data = response.json()
    if isinstance(data, dict):
        records = data.get("records")
    elif isinstance(data, list):
        records = data
    else:
        records = None
    return [r for r in (records or []) if isinstance(r, dict)]


def extract_topic_from_record(record: dict) -> Optional[dict]:
    """从一条话题记录抽取数字人选题。缺少 id 或选题文本则返回 None。"""
    record_id = str(record.get("id") or "").strip()
    topic = str(record.get("videoTopicSuggestion") or "").strip()
    if not record_id or not topic:
        return None
    angle_parts: list[str] = []
    for key in ("customerQuestion", "customerConcern"):
        value = str(record.get(key) or "").strip()
        if value:
            angle_parts.append(value)
    angle = "；".join(angle_parts)[:160]
    tags = record.get("tags") if isinstance(record.get("tags"), list) else []
    return {
        "record_id": record_id,
        "topic": topic[:100],
        "angle": angle,
        "tags": [str(t) for t in tags][:8],
        "scene": str(record.get("scene") or "").strip(),
        "created_at": str(record.get("createdAt") or record.get("submittedAt") or "").strip(),
    }


# ── 去重存储：记录已提交制作的话题 record_id ──

def _produced_ids_path(store_dir: str) -> Path:
    return Path(store_dir) / "produced_ids.json"


def load_produced_ids(store_dir: str) -> set[str]:
    path = _produced_ids_path(store_dir)
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return {str(x) for x in (data or [])}
    except Exception:
        return set()


def save_produced_ids(store_dir: str, ids: set[str]) -> None:
    path = _produced_ids_path(store_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"updated_at": time.time(), "ids": sorted(ids)}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_produced_ids_compat(store_dir: str) -> set[str]:
    """兼容旧格式（纯数组）与新格式（{ids: [...]}）。"""
    path = _produced_ids_path(store_dir)
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return set()
    if isinstance(data, dict):
        return {str(x) for x in (data.get("ids") or [])}
    if isinstance(data, list):
        return {str(x) for x in data}
    return set()


def select_new_topics(records: list[dict], produced_ids: set[str], limit: int = 10) -> list[dict]:
    """从记录中选出尚未制作过的选题，按接口顺序（通常新→旧），最多 limit 条。"""
    selected: list[dict] = []
    seen_topics: set[str] = set()
    for record in records:
        extracted = extract_topic_from_record(record)
        if not extracted:
            continue
        if extracted["record_id"] in produced_ids:
            continue
        topic_key = "".join(extracted["topic"].split()).lower()
        if topic_key in seen_topics:
            continue
        seen_topics.add(topic_key)
        selected.append(extracted)
        if len(selected) >= max(1, limit):
            break
    return selected
