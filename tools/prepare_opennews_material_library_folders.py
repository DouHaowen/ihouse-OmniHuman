#!/usr/bin/env python3
"""
Create Desktop review folders for building the OpenNews local material library.

This does not download or import anything. It creates one folder per high-value
news visual topic, plus a tag template CSV so reviewed images can later be
imported with consistent entity/scene tags.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
import time
from pathlib import Path
from urllib.parse import quote_plus

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from ai_material_harvester import NEWS_TOPIC_HARVEST_PRESETS  # noqa: E402
from opennews_material_sources import TOPIC_OFFICIAL_SOURCE_URLS  # noqa: E402


DEFAULT_DESKTOP = Path.home() / "Desktop"

ENTITY_TAGS_BY_TOPIC = {
    "xai_grok_bedrock": ["domain:ai", "entity:xai", "entity:grok", "entity:amazon", "entity:aws", "entity:bedrock", "scene:ai_interface", "scene:data_center", "usage:exact_entity", "safety:youtube_safe"],
    "anthropic_claude": ["domain:ai", "entity:anthropic", "entity:claude", "scene:ai_interface", "scene:office", "usage:exact_entity", "safety:youtube_safe"],
    "openai_chatgpt": ["domain:ai", "entity:openai", "entity:chatgpt", "entity:gpt", "scene:ai_interface", "scene:office", "usage:exact_entity", "safety:youtube_safe"],
    "google_gemini_ai": ["domain:ai", "entity:google", "entity:gemini", "entity:deepmind", "scene:ai_interface", "scene:cloud", "usage:exact_entity", "safety:youtube_safe"],
    "meta_ai": ["domain:ai", "entity:meta", "entity:llama", "scene:ai_interface", "scene:office", "usage:exact_entity", "safety:youtube_safe"],
    "microsoft_copilot_ai": ["domain:ai", "entity:microsoft", "entity:copilot", "entity:azure", "scene:ai_interface", "scene:cloud", "usage:exact_entity", "safety:youtube_safe"],
    "deepseek_ai": ["domain:ai", "entity:deepseek", "scene:ai_interface", "scene:data_center", "usage:exact_entity", "safety:youtube_safe"],
    "ai_nvidia_chip": ["domain:ai", "domain:technology", "entity:nvidia", "entity:nvidia_huang", "scene:gpu", "scene:chip", "scene:data_center", "usage:exact_entity", "safety:youtube_safe"],
    "ai_model_companies": ["domain:ai", "scene:ai_interface", "scene:office", "scene:data_center", "usage:generic_safe", "safety:youtube_safe"],
    "data_center_servers": ["domain:technology", "domain:ai", "scene:data_center", "scene:server_rack", "scene:cloud", "scene:cooling", "usage:generic_safe", "safety:youtube_safe"],
    "robotics_humanoid": ["domain:technology", "entity:robotics", "scene:humanoid_robot", "scene:industrial_robot", "scene:warehouse_robot", "usage:exact_topic", "safety:youtube_safe"],
    "white_house_us_politics": ["domain:politics", "entity:white_house", "entity:us_government", "scene:press_briefing", "scene:government_building", "usage:exact_entity", "safety:youtube_safe"],
    "trump_us_election": ["domain:politics", "entity:trump", "scene:rally", "scene:press_conference", "usage:exact_entity", "safety:youtube_safe"],
    "military_conflict": ["domain:military", "entity:nato", "entity:us_military", "scene:warship", "scene:fighter_jet", "scene:missile", "scene:drone", "usage:generic_safe", "safety:youtube_safe"],
    "middle_east_iran_israel": ["domain:military", "domain:energy", "entity:iran", "entity:israel", "entity:hormuz", "scene:map", "scene:oil_tanker", "scene:diplomacy", "usage:exact_topic", "safety:youtube_safe"],
    "oil_energy": ["domain:finance", "domain:energy", "entity:oil_market", "entity:opec", "scene:oil_tanker", "scene:refinery", "scene:pipeline", "scene:gas_station", "usage:generic_safe", "safety:youtube_safe"],
    "fed_inflation_markets": ["domain:finance", "entity:federal_reserve", "entity:jerome_powell", "entity:wall_street", "scene:central_bank", "scene:trading_floor", "scene:stock_board", "usage:exact_topic", "safety:youtube_safe"],
    "real_estate_us_housing": ["domain:real_estate", "entity:us_housing", "scene:suburban_home", "scene:real_estate_sign", "scene:apartment", "scene:mortgage", "usage:generic_safe", "safety:youtube_safe"],
    "immigration_visa": ["domain:immigration", "entity:immigration", "scene:passport", "scene:visa_office", "scene:airport", "scene:border_control", "usage:generic_safe", "safety:youtube_safe"],
    "general_press_briefing": ["domain:general_news", "scene:press_conference", "scene:newsroom", "scene:official_building", "scene:city_street", "usage:generic_safe", "safety:youtube_safe"],
}


PRIORITY_TOPIC_IDS = [
    "ai_nvidia_chip",
    "openai_chatgpt",
    "anthropic_claude",
    "xai_grok_bedrock",
    "google_gemini_ai",
    "microsoft_copilot_ai",
    "data_center_servers",
    "robotics_humanoid",
    "fed_inflation_markets",
    "white_house_us_politics",
    "trump_us_election",
    "oil_energy",
    "real_estate_us_housing",
    "military_conflict",
    "middle_east_iran_israel",
    "immigration_visa",
    "general_press_briefing",
]

OFFICIAL_SEARCH_GUIDES = {
    "ai_nvidia_chip": [
        ("NVIDIA Newsroom", "https://www.nvidia.com/en-us/about-nvidia/newsroom/"),
        ("NVIDIA Jensen Huang search", "https://www.nvidia.com/en-us/search/?q=Jensen%20Huang"),
        ("NVIDIA data center", "https://www.nvidia.com/en-us/data-center/"),
    ],
    "openai_chatgpt": [
        ("OpenAI News", "https://openai.com/news/"),
        ("OpenAI ChatGPT", "https://openai.com/chatgpt/"),
    ],
    "anthropic_claude": [
        ("Anthropic News", "https://www.anthropic.com/news"),
        ("Claude", "https://www.anthropic.com/claude"),
    ],
    "xai_grok_bedrock": [
        ("xAI News", "https://x.ai/news"),
        ("Grok", "https://x.ai/grok"),
        ("AWS Bedrock", "https://aws.amazon.com/bedrock/"),
        ("AWS ML Blog", "https://aws.amazon.com/blogs/machine-learning/"),
    ],
    "google_gemini_ai": [
        ("Google AI Blog", "https://blog.google/technology/ai/"),
        ("Google DeepMind Blog", "https://deepmind.google/discover/blog/"),
        ("Gemini", "https://gemini.google.com/"),
    ],
    "microsoft_copilot_ai": [
        ("Microsoft AI Blog", "https://blogs.microsoft.com/ai/"),
        ("Microsoft News", "https://news.microsoft.com/"),
        ("Microsoft Copilot", "https://www.microsoft.com/en-us/microsoft-copilot"),
    ],
    "data_center_servers": [
        ("NVIDIA Data Center", "https://www.nvidia.com/en-us/data-center/"),
        ("Google Cloud Infrastructure Blog", "https://cloud.google.com/blog/products/infrastructure"),
        ("Microsoft Azure Blog", "https://azure.microsoft.com/en-us/blog/"),
    ],
    "white_house_us_politics": [
        ("White House Briefing Room", "https://www.whitehouse.gov/briefing-room/"),
        ("White House Photos", "https://www.whitehouse.gov/photos-and-video/"),
    ],
    "trump_us_election": [
        ("White House Briefing Room", "https://www.whitehouse.gov/briefing-room/"),
    ],
    "fed_inflation_markets": [
        ("Federal Reserve News", "https://www.federalreserve.gov/newsevents.htm"),
        ("Federal Reserve About", "https://www.federalreserve.gov/aboutthefed.htm"),
        ("NYSE News", "https://www.nyse.com/news"),
    ],
    "oil_energy": [
        ("EIA Today in Energy", "https://www.eia.gov/todayinenergy/"),
        ("US Energy Department", "https://www.energy.gov/articles"),
        ("OPEC Press Room", "https://www.opec.org/opec_web/en/press_room/28.htm"),
    ],
    "real_estate_us_housing": [
        ("NAR Newsroom", "https://www.nar.realtor/newsroom"),
        ("Redfin News", "https://www.redfin.com/news/"),
        ("Zillow Research", "https://www.zillow.com/research/"),
    ],
    "military_conflict": [
        ("DVIDS Image Search", "https://www.dvidshub.net/search?q=military+exercise&type=image"),
        ("Defense Photos", "https://www.defense.gov/Multimedia/Photos/"),
        ("NATO Photos", "https://www.nato.int/cps/en/natohq/photos.htm"),
    ],
    "middle_east_iran_israel": [
        ("Defense Photos", "https://www.defense.gov/Multimedia/Photos/"),
        ("State Department", "https://www.state.gov/press-releases/"),
    ],
    "immigration_visa": [
        ("USCIS Newsroom", "https://www.uscis.gov/newsroom"),
        ("DHS News", "https://www.dhs.gov/news-releases"),
        ("State Department", "https://www.state.gov/press-releases/"),
    ],
}


def _slugify(value: str, fallback: str = "topic") -> str:
    slug = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "_", str(value or "").strip()).strip("_")
    return slug or fallback


def _topic_rows(limit: int) -> list[dict]:
    by_id = {str(item.get("id") or ""): dict(item) for item in NEWS_TOPIC_HARVEST_PRESETS}
    rows = []
    for topic_id in PRIORITY_TOPIC_IDS:
        topic = by_id.get(topic_id)
        if topic:
            rows.append(topic)
    if limit > 0:
        rows = rows[:limit]
    return rows


def _official_source_rows(topic: dict) -> list[dict]:
    topic_id = str(topic.get("id") or "")
    rows = []
    for label, url in OFFICIAL_SEARCH_GUIDES.get(topic_id, []):
        rows.append({"label": label, "url": url, "source_type": "official_guide"})
    for url in TOPIC_OFFICIAL_SOURCE_URLS.get(topic_id, []):
        if url not in {row["url"] for row in rows}:
            rows.append({"label": "Official source", "url": url, "source_type": "official_source"})
    topic_query = str(topic.get("topic") or topic.get("name") or "").strip()
    if topic_query:
        rows.append(
            {
                "label": "Google image search helper",
                "url": f"https://www.google.com/search?tbm=isch&q={quote_plus(topic_query + ' official photo')}",
                "source_type": "manual_search_helper",
            }
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare local OpenNews material-library review folders.")
    parser.add_argument("--output", default="", help="Output folder. Default: Desktop/OpenNews本地素材库建设_TIMESTAMP")
    parser.add_argument("--limit", type=int, default=0, help="Limit topics. Default: all priority topics.")
    args = parser.parse_args()

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_root = Path(args.output).expanduser() if args.output else DEFAULT_DESKTOP / f"OpenNews本地素材库建设_{timestamp}"
    output_root.mkdir(parents=True, exist_ok=True)

    rows = []
    for index, topic in enumerate(_topic_rows(args.limit), start=1):
        topic_id = str(topic.get("id") or "")
        folder = output_root / f"{index:02d}_{_slugify(topic.get('category') or 'news')}_{_slugify(topic.get('name') or topic_id)}"
        folder.mkdir(parents=True, exist_ok=True)
        tags = ENTITY_TAGS_BY_TOPIC.get(topic_id, ["domain:general_news", "usage:generic_safe", "safety:youtube_safe"])
        readme = "\n".join(
            [
                f"# {topic.get('name') or topic_id}",
                "",
                f"- category: {topic.get('category') or ''}",
                f"- topic_id: {topic_id}",
                f"- recommended_tags: {', '.join(tags)}",
                f"- search_topic: {topic.get('topic') or ''}",
                f"- notes: {topic.get('notes') or ''}",
                "",
                "放图规则：",
                "1. 只放和该专题明确相关的安全图片。",
                "2. 不确定是不是对应实体的图片不要放。",
                "3. 不要放裸露、擦边、血腥、伤者、尸体、广告水印明显的图片。",
                "4. 文件名建议包含实体，例如 nvidia_huang_001.jpg。",
                "",
            ]
        )
        (folder / "README.md").write_text(readme, encoding="utf-8")
        official_rows = _official_source_rows(topic)
        if official_rows:
            with (folder / "official_sources.csv").open("w", newline="", encoding="utf-8-sig") as source_handle:
                source_writer = csv.DictWriter(source_handle, fieldnames=["label", "url", "source_type"])
                source_writer.writeheader()
                source_writer.writerows(official_rows)
        rows.append(
            {
                "topic_id": topic_id,
                "topic_name": topic.get("name") or "",
                "category": topic.get("category") or "",
                "folder": str(folder),
                "recommended_tags": "、".join(tags),
                "search_topic": topic.get("topic") or "",
                "notes": topic.get("notes") or "",
            }
        )

    csv_path = output_root / "tag_template.csv"
    with csv_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["topic_id", "topic_name", "category", "folder", "recommended_tags", "search_topic", "notes"],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"已生成 OpenNews 本地素材库建设目录：{output_root}")
    print(f"专题数量：{len(rows)}")
    print(f"标签模板：{csv_path}")
    print("下一步：把审核好的图片放进对应专题文件夹，然后运行导入脚本入库并同步 5090 视觉向量库。")


if __name__ == "__main__":
    main()
