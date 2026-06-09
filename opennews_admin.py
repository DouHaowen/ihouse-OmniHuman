"""
OpenNews 管理员测试功能。

第一版只做新闻候选抓取和中文新闻口播稿草稿，不接入正式生产链路。
"""

from __future__ import annotations

import html
import json
import os
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, parse_qsl, quote_plus, unquote, urlencode, urljoin, urlparse, urlunparse
from xml.etree import ElementTree as ET

import requests

try:
    import anthropic
except Exception:  # pragma: no cover - optional in local tooling
    anthropic = None


DEFAULT_HEADERS = {
    "User-Agent": "iHouse-OpenNews-Test/0.1 (+https://aiagent.office.ihousejapan.cn)",
}

ANTHROPIC_CLIENT = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY")) if anthropic and os.getenv("ANTHROPIC_API_KEY") else None


def _get_openai_relay_api_key() -> str:
    return (
        os.getenv("OPENAI_RELAY_API_KEY")
        or os.getenv("SUB2API_API_KEY")
        or os.getenv("API_RELAY_OPENAI_API_KEY")
        or ""
    ).strip()


def _get_openai_relay_base_url() -> str:
    return (os.getenv("OPENAI_RELAY_BASE_URL") or "https://sub2api.ihousejapan.cn").strip().rstrip("/")


def _get_openai_relay_responses_url() -> str:
    base_url = _get_openai_relay_base_url()
    if base_url.endswith("/responses"):
        return base_url
    if base_url.endswith("/v1"):
        return f"{base_url}/responses"
    return f"{base_url}/v1/responses"


def _get_openai_relay_model() -> str:
    return (os.getenv("OPENAI_RELAY_MODEL") or "gpt-5.5").strip() or "gpt-5.5"


def _get_openai_relay_reasoning_effort() -> str:
    return (os.getenv("OPENAI_RELAY_REASONING_EFFORT") or "xhigh").strip() or "xhigh"


def _get_opennews_relay_reasoning_effort() -> str:
    # OpenNews draft generation is user-facing and synchronous; keep it lighter
    # than the main long-form script model so the page does not sit on timeouts.
    return (
        os.getenv("OPENAI_RELAY_OPENNEWS_REASONING_EFFORT")
        or os.getenv("OPENNEWS_RELAY_REASONING_EFFORT")
        or "medium"
    ).strip() or "medium"


def _get_opennews_relay_timeout_seconds() -> int:
    raw_value = (
        os.getenv("OPENAI_RELAY_OPENNEWS_TIMEOUT_SECONDS")
        or os.getenv("OPENNEWS_RELAY_TIMEOUT_SECONDS")
        or "240"
    )
    try:
        return max(60, min(420, int(float(raw_value))))
    except (TypeError, ValueError):
        return 240


def _extract_openai_relay_text(payload: dict) -> str:
    output_text = payload.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()
    chunks: list[str] = []
    for item in payload.get("output", []) or []:
        if not isinstance(item, dict):
            continue
        for content in item.get("content", []) or []:
            if isinstance(content, dict):
                text = content.get("text") or content.get("output_text")
                if isinstance(text, str):
                    chunks.append(text)
    return "\n".join(chunks).strip()


def _extract_json_object_text(raw: str) -> str:
    text = (raw or "").strip()
    if not text:
        return ""
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE).strip()
        text = re.sub(r"\s*```$", "", text).strip()
    match = re.search(r"\{[\s\S]*\}", text)
    if match:
        return match.group(0)
    start = text.find("{")
    end = text.rfind("}")
    if 0 <= start < end:
        return text[start : end + 1]
    return ""


def _repair_opennews_relay_json(raw: str) -> dict:
    api_key = _get_openai_relay_api_key()
    if not api_key:
        raise RuntimeError("未配置 OPENAI_RELAY_API_KEY，无法修复新闻稿 JSON")
    repair_prompt = f"""
下面是一个 OpenNews 新闻稿模型输出，但它不是合法 JSON。请把它修复成严格 JSON。

必须只返回 JSON，不要解释。字段必须包含：
video_title, summary, script, material_keywords, material_visual_plan, fact_check_notes, source_credit, news_time_label

原始输出：
{(raw or "")[:12000]}
""".strip()
    response = requests.post(
        _get_openai_relay_responses_url(),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": _get_openai_relay_model(),
            "input": repair_prompt,
            "instructions": "你只输出可解析 JSON。",
            "max_output_tokens": 4096,
            "reasoning": {"effort": "minimal"},
            "text": {"format": {"type": "json_object"}},
            "store": False,
        },
        timeout=_get_opennews_relay_timeout_seconds(),
    )
    if response.status_code >= 400:
        raise RuntimeError(f"API 中转新闻稿 JSON 修复失败：{response.status_code} {response.text[:500]}")
    repaired_raw = _extract_openai_relay_text(response.json())
    candidate = _extract_json_object_text(repaired_raw)
    if not candidate:
        raise RuntimeError("API 中转新闻稿生成失败：JSON 修复后仍未返回可解析 JSON")
    repaired = json.loads(candidate)
    if not isinstance(repaired, dict):
        raise RuntimeError("API 中转新闻稿生成失败：JSON 修复后返回的不是对象")
    return repaired


def _request_opennews_relay_json(prompt: str, *, max_output_tokens: int = 4096) -> dict:
    api_key = _get_openai_relay_api_key()
    if not api_key:
        raise RuntimeError("未配置 OPENAI_RELAY_API_KEY，无法生成新闻稿")
    efforts = [_get_opennews_relay_reasoning_effort()]
    if efforts[0] != "minimal":
        efforts.append("minimal")
    last_error: Exception | None = None
    for attempt, effort in enumerate(efforts, start=1):
        try:
            response = requests.post(
                _get_openai_relay_responses_url(),
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={
                    "model": _get_openai_relay_model(),
                    "input": prompt,
                    "instructions": "你只输出可解析 JSON。",
                    "max_output_tokens": max_output_tokens,
                    "reasoning": {"effort": effort},
                    "text": {"format": {"type": "json_object"}},
                    "store": False,
                },
                timeout=_get_opennews_relay_timeout_seconds(),
            )
            if response.status_code >= 400:
                raise RuntimeError(f"API 中转新闻稿生成失败：{response.status_code} {response.text[:500]}")
            raw = _extract_openai_relay_text(response.json())
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    return parsed
                last_error = RuntimeError("模型返回的 JSON 不是对象")
            except json.JSONDecodeError:
                candidate = _extract_json_object_text(raw)
                if candidate:
                    try:
                        parsed = json.loads(candidate)
                        if isinstance(parsed, dict):
                            return parsed
                        last_error = RuntimeError("模型返回的 JSON 对象提取结果不是对象")
                    except json.JSONDecodeError as exc:
                        last_error = exc
                else:
                    last_error = RuntimeError("模型未返回 JSON 对象")
            if attempt < len(efforts):
                time.sleep(1.5)
                continue
            try:
                return _repair_opennews_relay_json(raw)
            except Exception as repair_exc:
                return {
                    "_raw_text": raw,
                    "_json_parse_warning": str(repair_exc),
                }
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = exc
            if attempt < len(efforts):
                time.sleep(1.5)
                continue
            raise RuntimeError(
                "API 中转模型生成 OpenNews 新闻稿超时，请稍后重试；系统已改用更轻推理强度重试但仍未返回。"
            ) from exc
    raise RuntimeError(f"API 中转新闻稿生成失败：{last_error}")


def _request_opennews_claude_json(prompt: str, *, max_output_tokens: int = 4096) -> dict:
    if not ANTHROPIC_CLIENT:
        raise RuntimeError("未配置 ANTHROPIC_API_KEY，无法使用 Claude 生成新闻稿")
    prompts = [
        prompt,
        prompt + """

重要补充：
1. 你必须只返回合法 JSON，本次不要输出任何解释文字。
2. JSON 字符串内部不要出现未转义的半角双引号。
3. 如果内容里必须提到引号，请改用中文引号「」或『』。
4. 不要输出 markdown、代码块、注释或省略号。
""",
    ]
    last_error: Exception | None = None
    for item in prompts:
        try:
            message = ANTHROPIC_CLIENT.messages.create(
                model=os.getenv("ANTHROPIC_OPENNEWS_MODEL", "claude-sonnet-4-6"),
                max_tokens=max_output_tokens,
                system="你是专业新闻视频编辑。只输出可解析 JSON，不输出解释。",
                messages=[{"role": "user", "content": item}],
            )
            raw = "\n".join(
                getattr(block, "text", "").strip()
                for block in getattr(message, "content", []) or []
                if getattr(block, "type", "") == "text" and getattr(block, "text", "").strip()
            ).strip()
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    return parsed
            except json.JSONDecodeError:
                candidate = _extract_json_object_text(raw)
                if candidate:
                    parsed = json.loads(candidate)
                    if isinstance(parsed, dict):
                        return parsed
                raise
        except Exception as exc:
            last_error = exc
            continue
    if last_error:
        raise RuntimeError(f"Claude 新闻稿生成失败：{last_error}") from last_error
    raise RuntimeError("Claude 新闻稿生成失败：模型未返回可解析 JSON")


def _request_opennews_model_json(prompt: str, *, max_output_tokens: int = 4096) -> dict:
    try:
        return _request_opennews_claude_json(prompt, max_output_tokens=max_output_tokens)
    except Exception as claude_error:
        print(f"[opennews_claude_fallback] {claude_error!r} -> api_relay")
        return _request_opennews_relay_json(prompt, max_output_tokens=max_output_tokens)


def _ensure_string_list(value: object, *, limit: int = 8) -> list[str]:
    if isinstance(value, list):
        values = value
    elif value is None:
        values = []
    else:
        values = re.split(r"[、,，;\n]+", str(value))
    result: list[str] = []
    seen: set[str] = set()
    for item in values:
        text = _strip_tags(str(item or "")).strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(text)
        if len(result) >= limit:
            break
    return result


def _extract_labeled_opennews_text(raw: str, labels: tuple[str, ...]) -> str:
    text = (raw or "").strip()
    if not text:
        return ""
    escaped = "|".join(re.escape(label) for label in labels)
    next_labels = (
        "标题|视频标题|新闻标题|摘要|简要|口播|口播稿|文案|正文|script|summary|"
        "关键词|素材|事实|核验|来源|时间|source|credit"
    )
    match = re.search(
        rf"(?:{escaped})\s*[:：]\s*([\s\S]*?)(?=\n\s*(?:{next_labels})\s*[:：]|\Z)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return ""
    return _strip_tags(match.group(1)).strip(" \n\r\t-")


def _clean_opennews_script_text(value: str) -> str:
    text = _strip_tags(value or "")
    text = re.sub(r"^```(?:json)?|```$", "", text, flags=re.IGNORECASE).strip()
    text = re.sub(r"^\s*[{[][\s\S]*?[}\]]\s*$", "", text).strip() if '"script"' in text[:200] else text
    text = re.sub(r"(一句话看事件|背景方面|影响方面|首先|其次|最后)\s*[:：]?", "", text)
    banned_sentences = (
        "这条新闻值得关注的地方，在于它背后涉及的现实变化，以及相关各方接下来可能作出的反应。",
        "从已经公开的信息看，这件事不只是单一事件，还牵动了相关群体和行业的后续判断。",
        "后续还要看当事方说明和更多公开报道带来的新信息。",
        "目前相关报道已经引发关注。",
        "具体细节仍需以原始报道、官方信息和后续公开消息为准。",
    )
    for sentence in banned_sentences:
        text = text.replace(sentence, "")
    text = re.sub(r"\s+", " ", text).strip()
    # Keep it suitable for a short video, but do not crush it into a one-line summary.
    if len(text) > 420:
        sentences = re.split(r"(?<=[。！？!?])\s*", text)
        selected: list[str] = []
        char_count = 0
        for sentence in sentences:
            sentence = sentence.strip()
            if not sentence:
                continue
            selected.append(sentence)
            char_count += len(sentence)
            if char_count >= 300:
                break
        text = "".join(selected).strip() or text[:340].strip()
    return text


def _looks_like_generic_opennews_script(script: str, article: dict) -> bool:
    text = _strip_tags(script or "")
    if not text:
        return True
    generic_patterns = (
        "这条英文热点新闻主要涉及",
        "这是一条关于",
        "英文媒体报道了",
        "领域的新动向",
        "领域的新变化",
        "国际新闻中的一项新动向",
        "这件事可能影响相关行业",
        "相关公司、政策或市场参与方",
        "官方文件、市场反应和更多公开报道",
        "这条新闻值得关注的地方",
        "从已经公开的信息看",
        "后续还要看当事方说明",
        "背后涉及的现实变化",
        "相关各方接下来可能作出的反应",
        "相关群体和行业的后续判断",
        "目前相关报道已经引发关注",
        "具体细节仍需以原始报道",
        "后续进展仍需",
        "请继续关注",
    )
    if sum(1 for pattern in generic_patterns if pattern in text) >= 2:
        return True
    title_terms = [
        term.lower()
        for term in re.findall(r"[A-Za-z]{4,}|[\u4e00-\u9fff]{2,}", str(article.get("title") or ""))
    ]
    if title_terms:
        lowered = text.lower()
        matched = sum(1 for term in title_terms[:8] if term in lowered)
        if matched == 0 and len(text) < 180:
            return True
    cjk_count = len(re.findall(r"[\u4e00-\u9fff]", text))
    if cjk_count and cjk_count < 90:
        return True
    return False


def _target_language_name(target_market: str) -> str:
    return "繁體中文" if target_market == "tw" else ("日本語" if target_market == "jp" else "简体中文")


def _needs_opennews_language_rewrite(text: str, target_market: str) -> bool:
    text = _strip_tags(text or "")
    if not text:
        return True
    cjk_count = len(re.findall(r"[\u4e00-\u9fff]", text))
    kana_count = len(re.findall(r"[\u3040-\u30ff]", text))
    alpha_count = len(re.findall(r"[A-Za-z]", text))
    if target_market == "jp":
        return kana_count < 10 and cjk_count < 25 and alpha_count > 30
    return cjk_count < 35 and alpha_count > max(40, cjk_count * 2)


def _first_non_empty_text(*values: object) -> str:
    for value in values:
        text = _strip_tags(str(value or "")).strip()
        if text:
            return text
    return ""


def _has_cjk_text(value: str, *, minimum: int = 6) -> bool:
    return len(re.findall(r"[\u4e00-\u9fff]", value or "")) >= minimum


def _format_opennews_source_time(source_name: str, published_at: str) -> str:
    source = (source_name or "公开新闻源").strip()
    time_label = (published_at or "").strip()
    if not time_label:
        return f"据{source}报道"
    return f"据{source} {time_label}报道"


def _article_concrete_title_summary(article: dict) -> tuple[str, str]:
    title = _first_non_empty_text(
        article.get("title_zh"),
        article.get("translated_title"),
        article.get("video_title"),
        article.get("title"),
    )
    summary = _first_non_empty_text(
        article.get("summary_zh"),
        article.get("translated_summary"),
        article.get("summary"),
        title,
    )
    return title, summary


def _compact_opennews_subject_text(title: str, summary: str, *, target_market: str) -> tuple[str, str]:
    """Build a concrete fallback subject from the article itself, never from a broad category."""
    title = _strip_tags(title or "").strip()
    summary = _strip_tags(summary or "").strip()
    if target_market == "jp":
        if _has_cjk_text(title, minimum=3) or len(re.findall(r"[\u3040-\u30ff]", title)) >= 3:
            video_title = title[:80]
        else:
            video_title = f"英語圏ニュース：{title[:60] or '最新報道'}"
        if _has_cjk_text(summary, minimum=6) or len(re.findall(r"[\u3040-\u30ff]", summary)) >= 6:
            subject = summary[:180]
        else:
            subject = f"「{title[:120] or summary[:120] or '英語圏の最新報道'}」について報じられています"
        return video_title, subject
    if _has_cjk_text(title, minimum=4):
        video_title = title[:80]
    else:
        video_title = f"英文热点：{title[:60] or '最新报道'}"
    if _has_cjk_text(summary, minimum=10):
        subject = summary[:190]
    elif _has_cjk_text(title, minimum=4):
        subject = title[:150]
    else:
        concrete = title or summary or "这条英文报道"
        subject = f"这条新闻围绕“{concrete[:130]}”展开"
    return video_title, subject


def _opennews_topic_kind(article: dict, title: str, summary: str) -> str:
    category = str(article.get("category") or "").lower()
    text = " ".join([category, title or "", summary or ""]).lower()
    if re.search(r"高考|exam|college entrance|student|school|education|university|gaokao", text, flags=re.IGNORECASE):
        return "education"
    if re.search(r"spacex|meta|nvidia|openai|anthropic|ai|chip|semiconductor|tech|technology|software|tool", text, flags=re.IGNORECASE):
        return "technology"
    if re.search(r"stock|market|ipo|fed|inflation|rate|tariff|finance|economy|investment|earnings|bank", text, flags=re.IGNORECASE):
        return "finance"
    if re.search(r"missile|drone|military|defense|war|ukraine|navy|army|air force|attack", text, flags=re.IGNORECASE):
        return "military"
    if re.search(r"white house|congress|election|minister|president|policy|sanction|government|parliament", text, flags=re.IGNORECASE):
        return "politics"
    return "general"


def _opennews_natural_closing(kind: str, *, target_market: str) -> str:
    if target_market == "jp":
        closings = {
            "education": "受験生と家族にとっては、大きな節目となる一日です。",
            "technology": "今後は、実際の利用場面と業界への広がりが焦点になります。",
            "finance": "市場では、この動きが投資判断や企業戦略にどう影響するかが注目されます。",
            "military": "現地情勢と各国の対応について、引き続き慎重な確認が必要です。",
            "politics": "今後の制度設計や関係国の反応が焦点になります。",
            "general": "今後の追加情報と現地の反応が注目されます。",
        }
        return closings.get(kind, closings["general"])
    closings = {
        "education": "对考生和家庭来说，这不仅是一场考试，也是一段重要人生节点的开始。",
        "technology": "接下来，外界更关心的是这项技术会如何落地，以及会给行业带来什么变化。",
        "finance": "市场接下来会关注公司的订阅增长、广告业务、内容投入和盈利预期能不能支撑新的估值想象。",
        "military": "目前相关信息仍需要结合官方通报和多方公开报道继续确认。",
        "politics": "接下来，各方对这项政策或表态的反应，将成为外界观察的重点。",
        "general": "接下来，新闻的关键会落在当事方如何回应，以及事件时间线是否会进一步清晰。",
    }
    return closings.get(kind, closings["general"])


def _opennews_context_sentence(kind: str, subject: str, *, target_market: str) -> str:
    if target_market == "jp":
        sentences = {
            "education": "現場では、受験生だけでなく家族にとっても緊張感のある一日となっています。",
            "technology": "背景には、AI やデジタル技術をめぐる競争が一段と激しくなっていることがあります。",
            "finance": "投資家は、企業が次にどんな成長材料を示せるのかを慎重に見極めようとしています。",
            "military": "この動きは、地域の安全保障環境と各国の対応を考えるうえで重要な材料になります。",
            "politics": "この発言や政策は、国内外の関係者の判断にも影響する可能性があります。",
            "general": "今回の報道は、現場の動きと関係者の反応をあわせて見る必要があります。",
        }
        return sentences.get(kind, sentences["general"])
    sentences = {
        "education": "现场最受关注的，不只是考试本身，还有考生家庭在这个节点上的压力、期待和投入。",
        "technology": "这背后反映的是科技公司围绕产品能力、用户场景和行业竞争展开的新一轮角力。",
        "finance": "对市场来说，关键不只是这家公司眼下的表现，而是它能不能拿出新的增长故事来说服投资者。",
        "military": "这类消息的重点，在于事件本身如何改变地区安全判断，以及相关各方随后会采取什么动作。",
        "politics": "这类政策或表态的影响，往往不只停留在政府层面，也会传导到企业、市场和国际关系。",
        "general": "报道中的关键线索，集中在事件发生的时间、涉及的主体，以及已经出现的直接影响。",
    }
    return sentences.get(kind, sentences["general"])


def _opennews_detail_sentence(kind: str, *, target_market: str) -> str:
    if target_market == "jp":
        sentences = {
            "education": "毎年多くの受験生が進路を左右する試験に臨み、学校や家庭にも大きな影響を与えています。",
            "technology": "企業は新機能やサービスを通じて利用者を広げようとしており、競合他社との差別化も問われます。",
            "finance": "株価の評価は、売上や利益だけでなく、投資家が次の成長シナリオを信じられるかにも左右されます。",
            "military": "発表内容や現地映像だけでなく、当事国の説明と周辺国の反応をあわせて見る必要があります。",
            "politics": "政策の方向性は、国内の議論だけでなく、企業活動や外交関係にも波及する可能性があります。",
            "general": "短い発表の中にも、当事者の立場や社会への影響を読み解く手がかりがあります。",
        }
        return sentences.get(kind, sentences["general"])
    sentences = {
        "education": "每年高考都会牵动大量家庭，也折射出教育竞争、升学压力和社会流动机会这些现实议题。",
        "technology": "如果相关产品或工具真正进入用户场景，它影响的就不只是单家公司，也会改变行业竞争节奏。",
        "finance": "如果公司拿不出更清晰的增长路径，投资者对估值和未来盈利的疑问就很难消失。",
        "military": "在信息仍然快速变化的情况下，公开通报、现场素材和多方报道之间的交叉验证尤其重要。",
        "politics": "政策信号一旦释放，相关企业、市场和盟友都会重新评估自身的应对策略。",
        "general": "如果后续信息进一步确认，相关机构、企业或普通民众都可能据此调整判断。",
    }
    return sentences.get(kind, sentences["general"])


def _localized_known_opennews_subject(title: str, summary: str, *, target_market: str) -> str:
    if target_market == "jp":
        return ""
    text = " ".join([title or "", summary or ""]).lower()
    if "gaokao" in text or ("college entrance" in text and "exam" in text):
        location = "北京" if "beijing" in text or "北京" in text else "中国多地"
        return f"在{location}，大批年轻考生在家长陪同下走进考场，参加中国一年一度的高考。"
    if "spacex" in text and ("ipo" in text or "public offering" in text):
        return "围绕 SpaceX 是否可能推进上市计划，英文媒体报道了投资者、马斯克和资本市场关注的新动向。"
    if "anthropic" in text and ("pause" in text or "emergency" in text):
        return "Anthropic 呼吁为先进人工智能系统建立类似“暂停键”的安全机制，以便在高风险情况下及时控制模型运行。"
    if ("netflix" in text or "奈飞" in text) and ("stock" in text or "market" in text or "shares" in text or "股票" in text):
        return "奈飞股价这一年来表现不够理想，市场正在等待这家公司拿出新的增长叙事。"
    if ("job cut" in text or "layoff" in text or "裁员" in text) and ("ai" in text or "artificial intelligence" in text or "人工智能" in text):
        number_match = re.search(r"(\d{1,3}(?:,\d{3})+|\d{4,})", title + " " + summary)
        percent_match = re.search(r"(\d{1,2})\s*%", title + " " + summary)
        number_text = number_match.group(1) if number_match else "大量"
        percent_text = percent_match.group(1) if percent_match else ""
        percent_part = f"，其中约 {percent_text}% 被认为与人工智能有关" if percent_text else "，人工智能成为其中一个重要原因"
        return f"美国雇主在最新统计中宣布裁员 {number_text} 人，创下近几年少见的高位{percent_part}。"
    return ""


def _local_opennews_language_fallback(*, article: dict, target_market: str, published_at: str) -> dict:
    source_name = str(article.get("source_name") or "公开新闻源")
    original_title, original_summary = _article_concrete_title_summary(article)
    fallback_title, subject = _compact_opennews_subject_text(original_title, original_summary, target_market=target_market)
    localized_subject = _localized_known_opennews_subject(original_title, original_summary, target_market=target_market)
    if localized_subject:
        subject = localized_subject
        if fallback_title.startswith("英文热点："):
            fallback_title = subject[:36]
    topic_kind = _opennews_topic_kind(article, original_title, original_summary)
    context_sentence = _opennews_context_sentence(topic_kind, subject, target_market=target_market)
    detail_sentence = _opennews_detail_sentence(topic_kind, target_market=target_market)
    closing = _opennews_natural_closing(topic_kind, target_market=target_market)
    if target_market == "jp":
        lead = _format_opennews_source_time(source_name, published_at).replace("据", "").replace("报道", "が伝えた内容によると")
        return {
            "video_title": fallback_title,
            "summary": subject,
            "script": _clean_opennews_script_text(f"{lead}、{subject}。{context_sentence}{detail_sentence}{closing}"),
        }
    lead = _format_opennews_source_time(source_name, published_at)
    subject_sentence = subject.rstrip("。！？!?；; ")
    return {
        "video_title": fallback_title,
        "summary": subject,
        "script": _clean_opennews_script_text(f"{lead}，{subject_sentence}。{context_sentence}{detail_sentence}{closing}"),
    }


def _rewrite_opennews_text_language(*, title: str, summary: str, script: str, article: dict, target_market: str, published_at: str) -> dict:
    language = _target_language_name(target_market)
    source_name = str(article.get("source_name") or "公开新闻源")
    fallback_title = title or str(article.get("title") or "OpenNews 新闻")
    fallback_summary = summary or str(article.get("summary") or fallback_title)
    fallback_script = script or fallback_summary or fallback_title
    prompt = f"""
请把下面 OpenNews 新闻内容改写成{language}新闻视频播出稿。

要求：
- 只返回 JSON，不要解释。
- 保持事实边界，不要新增原文没有的信息。
- script 是 45-60 秒自然口播，一段即可，必须像主播正在播一条完整短新闻。
- 简体/繁体中文写 220-320 字；日语写 420-560 字。
- 开头直接讲“谁/哪家机构/哪家公司做了什么”，不要先写空泛导语。
- 中间至少写 2-3 句，补足新闻背景、关键数字、市场/政策/行业/社会影响，不要只写一句摘要。
- 结尾自然收住，点出下一步最具体的看点，不要套话。
- 不要写“一句话看事件/背景方面/影响方面/首先/其次/最后”。
- 不要写“这条新闻主要涉及”“引发关注”“具体细节仍需以原始报道为准”这种空泛模板句。
- 不要写“这条新闻值得关注的地方”“从已经公开的信息看”“这件事不只是单一事件”“后续还要看当事方说明”这类机器总结句。
- 自然提到来源和时间。

来源：{source_name}
时间：{published_at or "来源页面未标注明确发布时间"}
标题：{fallback_title}
摘要：{fallback_summary}
现有口播：{fallback_script}

JSON 字段：
{{"video_title":"...","summary":"...","script":"..."}}
""".strip()
    try:
        parsed = _request_opennews_model_json(prompt, max_output_tokens=1600)
        if isinstance(parsed, dict):
            return {
                "video_title": _strip_tags(str(parsed.get("video_title") or fallback_title)),
                "summary": _strip_tags(str(parsed.get("summary") or fallback_summary)),
                "script": _clean_opennews_script_text(str(parsed.get("script") or fallback_script)),
            }
    except Exception:
        pass
    return _local_opennews_language_fallback(article=article, target_market=target_market, published_at=published_at)


def _polish_opennews_broadcast_copy(
    *,
    draft: dict,
    article: dict,
    article_text: str,
    related_context: str,
    target_market: str,
    published_at: str,
) -> dict:
    """Rewrite title/summary/script as natural broadcast copy after JSON generation."""
    language = _target_language_name(target_market)
    length_rule = "简体/繁体中文 220-320 字" if target_market != "jp" else "日本語 420-560 字"
    source_name = str(article.get("source_name") or "公开新闻源")
    title = _strip_tags(str(draft.get("video_title") or article.get("title") or ""))
    summary = _strip_tags(str(draft.get("summary") or article.get("summary") or ""))
    script = _clean_opennews_script_text(str(draft.get("script") or summary or title))
    prompt = f"""
你是电视新闻节目的资深中文/日文新闻编辑。请把下面内容改写成真正可以直接播出的短新闻口播稿。

输出语言：{language}
目标长度：{length_rule}，一段完整口播，不要分标题小节。

必须遵守：
- 根据新闻标题、摘要和正文内容写，必须具体讲这条新闻本身，不要写成“某领域新动向”。
- 第一句直接交代“谁/哪家公司/哪个机构/哪个国家发生了什么”。
- 中间至少写 2-3 句，补最关键的事实、数字、背景、现场信息或利益相关方。
- 必须说明这件事为什么值得关注，但必须贴合新闻类型：教育讲考生和家庭，科技讲产品和行业，财经讲市场和投资，政治讲政策和各方反应，军事讲局势和官方通报。
- 结尾自然收住，点出下一步最具体的看点，不能突然断，也不要套“后续仍需关注”“具体细节以原文为准”这种空话。
- 不要出现“一句话看事件”“背景方面”“影响方面”“首先/其次/最后”。
- 不要出现“这条新闻值得关注的地方”“从已经公开的信息看”“这件事不只是单一事件”“后续还要看当事方说明”。
- 不要加入原文没有的价格、人数、结论或立场。
- 语气像新闻主播，不要像机器摘要，不要像论文简介。

来源：{source_name}
发布时间：{published_at or "来源页面未标注明确发布时间"}
原始标题：{article.get("title") or ""}
原始摘要：{article.get("summary") or ""}
当前标题：{title}
当前摘要：{summary}
当前口播稿：{script}

正文节选：
{(article_text or "")[:4500] or "无正文。"}

相关报道：
{related_context or "无"}

只返回 JSON：
{{"video_title":"适合视频标题的短标题","summary":"一句中文/日文摘要","script":"完整播出稿"}}
""".strip()
    try:
        parsed = _request_opennews_model_json(prompt, max_output_tokens=2200)
        if not isinstance(parsed, dict):
            return draft
        polished = dict(draft)
        polished["video_title"] = _strip_tags(str(parsed.get("video_title") or title))
        polished["summary"] = _strip_tags(str(parsed.get("summary") or summary))
        polished["script"] = _clean_opennews_script_text(str(parsed.get("script") or script))
        polished["_broadcast_polished"] = True
        return polished
    except Exception:
        return draft


def _fallback_opennews_visual_plan(script: str, keywords: list[str], article: dict, category: str) -> list[dict]:
    seed_parts = [
        str(article.get("title") or ""),
        str(article.get("summary") or ""),
        script,
        " ".join(keywords),
    ]
    queries = _expanded_media_queries(*seed_parts, category=category, limit=8)
    if not queries:
        queries = [_compact_query(str(article.get("title") or script or "news"), max_chars=70)]
    plan: list[dict] = []
    for index, query in enumerate(queries[:4]):
        plan.append({
            "title": query,
            "script_context": script[:140],
            "visual_need": f"与新闻事实直接相关的画面：{query}",
            "queries": [query],
        })
    return plan


def _normalize_opennews_draft_payload(raw_draft: dict, *, article: dict, target_market: str, published_at: str) -> dict:
    """Make relay output usable even when the model returns prose instead of strict JSON."""
    draft = dict(raw_draft or {})
    raw_text = str(draft.get("_raw_text") or "")
    title = _strip_tags(str(
        draft.get("video_title")
        or draft.get("title")
        or _extract_labeled_opennews_text(raw_text, ("video_title", "视频标题", "新闻标题", "标题"))
        or article.get("title")
        or "OpenNews 新闻"
    ))
    summary = _strip_tags(str(
        draft.get("summary")
        or _extract_labeled_opennews_text(raw_text, ("summary", "摘要", "简要"))
        or article.get("summary")
        or title
    ))
    script = _clean_opennews_script_text(str(
        draft.get("script")
        or draft.get("口播稿")
        or draft.get("文案")
        or _extract_labeled_opennews_text(raw_text, ("script", "口播稿", "文案", "正文"))
        or raw_text
        or summary
        or title
    ))
    if not script:
        source_name = str(article.get("source_name") or "公开新闻源")
        time_label = published_at or "来源页面未标注明确发布时间"
        script = f"据{source_name}{time_label}消息，{summary or title}。这条新闻仍需结合公开来源继续关注后续进展。"
    if (
        _needs_opennews_language_rewrite(title, target_market)
        or _needs_opennews_language_rewrite(summary, target_market)
        or _needs_opennews_language_rewrite(script, target_market)
    ):
        rewritten = _rewrite_opennews_text_language(
            title=title,
            summary=summary,
            script=script,
            article=article,
            target_market=target_market,
            published_at=published_at,
        )
        title = str(rewritten.get("video_title") or title)
        summary = str(rewritten.get("summary") or summary)
        script = _clean_opennews_script_text(str(rewritten.get("script") or script))
        if _looks_like_generic_opennews_script(script, article):
            hard_fallback = _local_opennews_language_fallback(article=article, target_market=target_market, published_at=published_at)
            title = str(hard_fallback.get("video_title") or title)
            summary = str(hard_fallback.get("summary") or summary)
            script = _clean_opennews_script_text(str(hard_fallback.get("script") or script))
        if _needs_opennews_language_rewrite(script, target_market):
            hard_fallback = _local_opennews_language_fallback(article=article, target_market=target_market, published_at=published_at)
            title = str(hard_fallback.get("video_title") or title)
            summary = str(hard_fallback.get("summary") or summary)
            script = _clean_opennews_script_text(str(hard_fallback.get("script") or script))
    if _looks_like_generic_opennews_script(script, article):
        rewritten = _rewrite_opennews_text_language(
            title=title,
            summary=summary,
            script=script,
            article=article,
            target_market=target_market,
            published_at=published_at,
        )
        title = str(rewritten.get("video_title") or title)
        summary = str(rewritten.get("summary") or summary)
        script = _clean_opennews_script_text(str(rewritten.get("script") or script))
    keywords = _ensure_string_list(draft.get("material_keywords"), limit=8)
    if not keywords:
        keyword_seed = " ".join([title, summary, script[:120]])
        keywords = _expanded_media_queries(keyword_seed, category=str(article.get("category") or "all"), limit=6)
    visual_plan = draft.get("material_visual_plan") or draft.get("visual_plan") or []
    if not isinstance(visual_plan, list) or not visual_plan:
        visual_plan = _fallback_opennews_visual_plan(script, keywords, article, str(article.get("category") or "all"))
    fact_notes = _ensure_string_list(draft.get("fact_check_notes"), limit=6)
    if not fact_notes:
        fact_notes = ["请以原始新闻链接和相关公开报道为准，避免把推测写成确定事实。"]
    source_name = str(article.get("source_name") or draft.get("_meta", {}).get("source_name") or "OpenNews")
    source_credit = _strip_tags(str(draft.get("source_credit") or f"来源：{source_name}"))
    news_time_label = _strip_tags(str(draft.get("news_time_label") or published_at or "来源页面未标注明确发布时间"))
    normalized = {
        **draft,
        "video_title": title,
        "summary": summary,
        "script": script,
        "material_keywords": keywords,
        "material_visual_plan": visual_plan,
        "fact_check_notes": fact_notes,
        "source_credit": source_credit,
        "news_time_label": news_time_label,
    }
    if raw_text:
        normalized["_relay_raw_text"] = raw_text[:4000]
    return normalized


@dataclass(frozen=True)
class OpenNewsSource:
    id: str
    name: str
    category: str
    country: str
    url: str
    license: str
    content_type: str
    search_url: str = ""
    rss_url: str = ""
    latest_url: str = ""


OPENNEWS_SOURCES: list[OpenNewsSource] = [
    OpenNewsSource(
        id="dvids",
        name="DVIDS 美军媒体素材库",
        category="military",
        country="美国",
        url="https://www.dvidshub.net/",
        license="Public Domain",
        content_type="军事公开视频/图片/新闻",
        search_url="https://www.dvidshub.net/search?q={query}",
        latest_url="https://www.dvidshub.net/news/list/most-recent",
    ),
    OpenNewsSource(
        id="dod",
        name="美国国防部",
        category="military",
        country="美国",
        url="https://www.defense.gov/",
        license="Public Domain",
        content_type="国防声明/新闻/图片",
        search_url="https://www.defense.gov/Search-Results/?query={query}",
        latest_url="https://www.defense.gov/News/Releases/",
    ),
    OpenNewsSource(
        id="indopacom",
        name="美国印太司令部",
        category="military",
        country="美国",
        url="https://www.pacom.mil/",
        license="Public Domain",
        content_type="印太/台海/南海军事动态",
        search_url="https://www.pacom.mil/Search/?query={query}",
        latest_url="https://www.pacom.mil/Media/News/",
    ),
    OpenNewsSource(
        id="mod_jp",
        name="日本防卫省",
        category="military",
        country="日本",
        url="https://www.mod.go.jp/",
        license="PDL 1.0 / CC BY 4.0 equivalent",
        content_type="防卫省/自卫队动态",
        search_url="https://www.mod.go.jp/j/search/?q={query}",
        latest_url="https://www.mod.go.jp/j/press/news/",
    ),
    OpenNewsSource(
        id="mofa_jp",
        name="日本外务省",
        category="politics",
        country="日本",
        url="https://www.mofa.go.jp/",
        license="PDL 1.0 / CC BY 4.0 equivalent",
        content_type="外交声明/政策新闻",
        search_url="https://www.mofa.go.jp/search.html?q={query}",
        latest_url="https://www.mofa.go.jp/press/release/pressite_000001_00001.html",
    ),
    OpenNewsSource(
        id="mnd_tw",
        name="台湾国防部",
        category="military",
        country="台湾",
        url="https://www.mnd.gov.tw/",
        license="OGDL-Taiwan / CC BY 4.0 equivalent",
        content_type="国防新闻/共机绕台数据",
        search_url="https://www.mnd.gov.tw/Search.aspx?query={query}",
        latest_url="https://www.mnd.gov.tw/News.aspx",
    ),
    OpenNewsSource(
        id="voa_zh",
        name="VOA 中文",
        category="politics",
        country="美国",
        url="https://www.voachinese.com/",
        license="Public Domain",
        content_type="中文新闻参考",
        rss_url="https://www.voachinese.com/api/",
        search_url="https://www.voachinese.com/s?k={query}",
        latest_url="https://www.voachinese.com/z/1739",
    ),
]

OPENNEWS_CATEGORIES = [
    {"id": "all", "name": "全部"},
    {"id": "military", "name": "军事类"},
    {"id": "politics", "name": "政治类"},
    {"id": "technology", "name": "科技类"},
    {"id": "finance", "name": "金融类"},
    {"id": "society", "name": "社会类"},
]


OPENNEWS_KEYWORD_EXPANSIONS = {
    "SpaceX": ["SpaceX"],
    "spacex": ["SpaceX"],
    "Meta": ["Meta", "Meta AI", "Meta company"],
    "mate": ["Meta", "Meta AI", "Meta company"],
    "facebook": ["Meta", "Facebook"],
    "微软": ["Microsoft"],
    "谷歌": ["Google", "Alphabet"],
    "英伟达": ["Nvidia"],
    "苹果": ["Apple"],
    "亚马逊": ["Amazon"],
    "马斯克": ["Elon Musk", "Musk"],
    "埃隆": ["Elon Musk"],
    "IPO": ["IPO", "initial public offering"],
    "上市": ["IPO", "initial public offering"],
    "万亿富翁": ["trillionaire", "Elon Musk trillionaire"],
    "生物技术": ["biotechnology", "biotech"],
    "投资": ["investment", "investors"],
    "白宫": ["White House", "White House press briefing"],
    "发言人": ["press briefing", "spokesperson"],
    "发布会": ["press briefing", "press conference"],
    "中国科技": ["China technology", "Chinese technology"],
    "中国": ["China"],
    "台海": ["Taiwan Strait", "Taiwan"],
    "台湾": ["Taiwan"],
    "印太": ["Indo-Pacific", "INDOPACOM"],
    "南海": ["South China Sea"],
    "军演": ["military exercise", "joint exercise", "training"],
    "演习": ["military exercise", "training"],
    "舰艇": ["ship", "navy", "destroyer", "vessel"],
    "军舰": ["warship", "navy ship", "destroyer"],
    "航母": ["aircraft carrier", "carrier strike group"],
    "飞机": ["aircraft", "fighter jet"],
    "战机": ["fighter jet", "aircraft"],
    "无人机": ["drone", "UAV", "unmanned aircraft"],
    "导弹": ["missile"],
    "防卫省": ["Japan Ministry of Defense", "JSDF"],
    "自卫队": ["JSDF", "Japan Self-Defense Force"],
    "美军": ["U.S. military", "U.S. Navy", "U.S. Air Force"],
    "国防部": ["Department of Defense", "DoD"],
    "芯片": ["chip", "semiconductor"],
    "半导体": ["semiconductor", "chip"],
    "人工智能": ["artificial intelligence", "AI"],
    "机器人": ["robot", "robotics"],
    "金融": ["finance", "market", "economy"],
    "股市": ["stock market"],
    "汇率": ["exchange rate"],
}


OPENNEWS_CATEGORY_MEDIA_TERMS = {
    "military": ["military exercise", "navy ship", "aircraft", "troops", "defense ministry", "fighter jet", "warship", "missile", "drone UAV"],
    "politics": ["press briefing", "government meeting", "diplomacy", "White House", "parliament", "cabinet meeting", "foreign ministry"],
    "technology": ["technology", "AI", "semiconductor", "robotics", "chip factory", "data center", "laboratory", "electronics manufacturing"],
    "finance": ["financial market", "economy", "stock market", "central bank", "currency exchange", "trading floor", "business district"],
    "society": ["city street", "public service", "community", "school", "hospital", "public transport", "local government"],
}


GENERAL_WEB_SEARCH_URLS = (
    "https://duckduckgo.com/html/?q={query}",
    "https://www.bing.com/search?q={query}",
)


GENERAL_WEB_MEDIA_SEARCH_URLS = (
    "https://www.bing.com/images/search?q={query}",
    "https://www.bing.com/videos/search?q={query}",
)


GENERAL_WEB_BLOCKED_HOST_TOKENS = (
    "facebook.com",
    "instagram.com",
    "tiktok.com",
    "x.com",
    "twitter.com",
    "youtube.com",
    "youtu.be",
    "linkedin.com",
    "pinterest.",
    "reddit.com",
)


def source_payloads() -> list[dict]:
    return [source.__dict__ for source in OPENNEWS_SOURCES]


def category_payloads() -> list[dict]:
    return list(OPENNEWS_CATEGORIES)


def _strip_tags(value: str) -> str:
    text = re.sub(r"<script[\s\S]*?</script>", " ", value or "", flags=re.I)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _compact_query(value: str, max_chars: int = 90) -> str:
    value = _strip_tags(value or "")
    value = re.sub(r"[，。！？、；：,.!?;:（）()【】\[\]「」\"']", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value[:max_chars]


def _expanded_media_queries(*parts: str, category: str = "all", limit: int = 8) -> list[str]:
    text = " ".join(_strip_tags(part or "") for part in parts if part)
    queries: list[str] = []
    for key, expansions in OPENNEWS_KEYWORD_EXPANSIONS.items():
        if key in text:
            queries.extend(expansions)
    for part in parts:
        compact = _compact_query(part or "")
        if compact:
            queries.append(compact)
    # 分类通用词只能作为最后兜底，不能盖过新闻实体本身。
    queries.extend(OPENNEWS_CATEGORY_MEDIA_TERMS.get(category, []))
    deduped: list[str] = []
    seen: set[str] = set()
    for query in queries:
        query = _compact_query(query, max_chars=80)
        key = query.lower()
        if len(query) < 2 or key in seen:
            continue
        seen.add(key)
        deduped.append(query)
        if len(deduped) >= limit:
            break
    return deduped


def _opennews_entity_search_query(keywords: list[str], article: dict, draft: dict, *, limit: int = 8) -> str:
    parts = [
        " ".join(str(item).strip() for item in keywords if str(item).strip()),
        str(draft.get("video_title") or ""),
        str(article.get("title") or ""),
        str(draft.get("summary") or ""),
    ]
    queries = _expanded_media_queries(*parts, category="all", limit=limit)
    # 只取前面的实体/标题相关词，避免把分类兜底词暴露成主检索词。
    blocked_generic = {
        "press briefing", "government meeting", "diplomacy", "white house",
        "parliament", "cabinet meeting", "foreign ministry", "military exercise",
        "navy ship", "aircraft", "troops", "defense ministry",
    }
    filtered = []
    for query in queries:
        key = query.lower().strip()
        if key in blocked_generic:
            continue
        filtered.append(query)
        if len(filtered) >= limit:
            break
    if not filtered:
        filtered = [str(item).strip() for item in keywords if str(item).strip()][:limit]
    return " ".join(filtered).strip() or str(article.get("title") or draft.get("video_title") or "news")


def _parse_timestamp(value: str) -> float:
    value = (value or "").strip()
    if not value:
        return 0.0
    value = html.unescape(value)
    value = re.sub(r"\s+", " ", value)
    value = re.sub(r"(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日", r"\1-\2-\3", value)
    value = re.sub(r"(\d{4})/(\d{1,2})/(\d{1,2})", r"\1-\2-\3", value)
    try:
        return parsedate_to_datetime(value).timestamp()
    except Exception:
        pass
    date_match = re.search(r"(20\d{2})-(\d{1,2})-(\d{1,2})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?)?", value)
    if date_match:
        y, m, d, hh, mm, ss = date_match.groups()
        normalized = f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
        if hh is not None:
            normalized += f" {int(hh):02d}:{int(mm or 0):02d}:{int(ss or 0):02d}"
        value = normalized
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return time.mktime(time.strptime(value[:19] if " " in fmt or "T" in fmt else value[:10], fmt))
        except Exception:
            continue
    return 0.0


def _recent_news_window_start_ts(days: int = 2) -> float:
    """Local midnight of yesterday when days=2, i.e. today + previous day only."""
    jst = timezone(timedelta(hours=9))
    now = datetime.now(jst)
    today_midnight = datetime(now.year, now.month, now.day, tzinfo=jst)
    return (today_midnight - timedelta(days=max(0, days - 1))).timestamp()


def _recent_news_window_label(days: int = 2) -> str:
    start_ts = _recent_news_window_start_ts(days)
    end_ts = time.time()
    jst = timezone(timedelta(hours=9))
    start_label = datetime.fromtimestamp(start_ts, jst).strftime("%Y-%m-%d")
    end_label = datetime.fromtimestamp(end_ts, jst).strftime("%Y-%m-%d")
    return f"{start_label} 至 {end_label}"


def _is_recent_news_candidate(candidate: dict, *, days: int = 2) -> bool:
    published_ts = float(candidate.get("published_ts") or 0)
    return bool(published_ts and published_ts >= _recent_news_window_start_ts(days) and published_ts <= time.time() + 86400)


def _source_by_id(source_id: str) -> OpenNewsSource | None:
    for source in OPENNEWS_SOURCES:
        if source.id == source_id:
            return source
    return None


def _canonical_news_url(url: str) -> str:
    parsed = urlparse((url or "").strip())
    if not parsed.scheme or not parsed.netloc:
        return (url or "").strip()
    ignored_params = {
        "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
        "fbclid", "gclid", "mc_cid", "mc_eid", "cmp", "cid", "output",
    }
    query_items = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=False)
        if key.lower() not in ignored_params
    ]
    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    if path != "/":
        path = path.rstrip("/")
    return urlunparse((
        parsed.scheme.lower(),
        parsed.netloc.lower(),
        path,
        "",
        urlencode(query_items, doseq=True),
        "",
    ))


def _normalized_news_title(title: str) -> str:
    title = _strip_tags(title or "").lower()
    title = re.sub(r"\s*[-|｜_]\s*(voa|美国之音|dvids|defense\.gov|防衛省|外務省).*$", "", title, flags=re.I)
    title = re.sub(r"[^\w\u3040-\u30ff\u3400-\u9fff]+", "", title)
    return title[:90]


def _title_tokens(title: str) -> set[str]:
    raw = _strip_tags(title or "").lower()
    tokens = set(re.findall(r"[a-z0-9]{3,}|[\u3040-\u30ff]{2,}|[\u3400-\u9fff]{2,4}", raw))
    generic = {
        "新闻", "最新", "报道", "消息", "视频", "图片", "全文", "美国", "中国", "日本",
        "voa", "dvids", "news", "press", "release", "video", "photo", "image",
    }
    return {token for token in tokens if token not in generic and len(token) >= 2}


def _titles_are_similar(left: str, right: str) -> bool:
    left_key = _normalized_news_title(left)
    right_key = _normalized_news_title(right)
    if not left_key or not right_key:
        return False
    if left_key in right_key or right_key in left_key:
        return min(len(left_key), len(right_key)) >= 14
    if min(len(left_key), len(right_key)) >= 18 and SequenceMatcher(None, left_key, right_key).ratio() >= 0.58:
        return True
    left_tokens = _title_tokens(left)
    right_tokens = _title_tokens(right)
    if len(left_tokens) < 3 or len(right_tokens) < 3:
        return False
    overlap = len(left_tokens & right_tokens)
    ratio = overlap / max(1, min(len(left_tokens), len(right_tokens)))
    return overlap >= 3 and ratio >= 0.72


def _candidate_dedupe_key(candidate: dict) -> str:
    canonical = _canonical_news_url(str(candidate.get("url") or ""))
    title_key = _normalized_news_title(str(candidate.get("title") or ""))
    if canonical:
        parsed = urlparse(canonical)
        path = parsed.path.rstrip("/")
        if path and path != "/":
            return f"url:{parsed.netloc}{path}"
    if title_key:
        return f"title:{candidate.get('source_id') or ''}:{title_key}"
    return f"id:{candidate.get('id') or uuid.uuid4().hex}"


def _dedupe_opennews_candidates(candidates: list[dict]) -> list[dict]:
    deduped: list[dict] = []
    seen: dict[str, int] = {}
    title_seen: dict[str, int] = {}
    for item in candidates:
        if not isinstance(item, dict):
            continue
        item = dict(item)
        item["canonical_url"] = _canonical_news_url(str(item.get("url") or ""))
        item["dedupe_key"] = _candidate_dedupe_key(item)
        title_key = _normalized_news_title(str(item.get("title") or ""))
        keys = [item["dedupe_key"]]
        if title_key and len(title_key) >= 12:
            keys.append(f"title:{item.get('source_id') or ''}:{title_key}")

        existing_index = None
        for key in keys:
            if key in seen:
                existing_index = seen[key]
                break
            if key in title_seen:
                existing_index = title_seen[key]
                break
        if existing_index is None and title_key:
            for index, existing in enumerate(deduped):
                if item.get("source_id") != existing.get("source_id"):
                    continue
                if _titles_are_similar(str(item.get("title") or ""), str(existing.get("title") or "")):
                    existing_index = index
                    break
        if existing_index is None:
            seen[item["dedupe_key"]] = len(deduped)
            if title_key and len(title_key) >= 12:
                title_seen[f"title:{item.get('source_id') or ''}:{title_key}"] = len(deduped)
            deduped.append(item)
            continue

        existing = deduped[existing_index]
        if not existing.get("published_at") and item.get("published_at"):
            existing["published_at"] = item.get("published_at")
            existing["published_ts"] = item.get("published_ts") or 0
            existing["is_latest"] = item.get("is_latest", False)
        if not existing.get("summary") and item.get("summary"):
            existing["summary"] = item.get("summary")
        if item.get("published_ts", 0) > existing.get("published_ts", 0):
            existing["published_ts"] = item.get("published_ts")
            existing["published_at"] = item.get("published_at")
            existing["is_latest"] = item.get("is_latest", False)
    return deduped


def _candidate_from_source(source: OpenNewsSource, **kwargs) -> dict:
    published_at = _strip_tags(str(kwargs.get("published_at") or ""))[:100]
    source_url = str(kwargs.get("url") or source.url)
    canonical_url = _canonical_news_url(source_url)
    return {
        "id": kwargs.get("id") or uuid.uuid4().hex[:12],
        "source_id": source.id,
        "source_name": source.name,
        "category": source.category,
        "category_name": next((item["name"] for item in OPENNEWS_CATEGORIES if item["id"] == source.category), source.category),
        "title": _strip_tags(str(kwargs.get("title") or ""))[:180],
        "url": source_url,
        "canonical_url": canonical_url,
        "summary": _strip_tags(str(kwargs.get("summary") or ""))[:420],
        "published_at": published_at,
        "published_ts": _parse_timestamp(published_at),
        "license": source.license,
        "content_type": source.content_type,
        "is_latest": bool(_parse_timestamp(published_at) and time.time() - _parse_timestamp(published_at) <= 3 * 86400),
        "dedupe_key": "",
        **({"error": kwargs.get("error")} if kwargs.get("error") else {}),
    }


def _extract_meta_content(page_html: str, names: tuple[str, ...]) -> str:
    for name in names:
        patterns = [
            rf'<meta[^>]+property=["\']{re.escape(name)}["\'][^>]+content=["\']([^"\']+)["\']',
            rf'<meta[^>]+name=["\']{re.escape(name)}["\'][^>]+content=["\']([^"\']+)["\']',
            rf'<meta[^>]+content=["\']([^"\']+)["\'][^>]+(?:property|name)=["\']{re.escape(name)}["\']',
        ]
        for pattern in patterns:
            match = re.search(pattern, page_html or "", flags=re.I)
            if match:
                return html.unescape(match.group(1)).strip()
    return ""


OPENNEWS_MEDIA_BAD_TOKENS = (
    "favicon",
    "apple-touch-icon",
    "sprite",
    "/icons/",
    "/icon/",
    "logo",
    "avatar",
    "author",
    "profile",
    "social",
    "share",
    "tracking",
    "pixel",
    "spacer",
    "blank",
    "placeholder",
    "advert",
    "/ads/",
    "banner-ad",
)


OPENNEWS_MEDIA_PAGE_TOKENS = (
    "/video/",
    "/videos/",
    "/image/",
    "/images/",
    "/photo/",
    "/photos/",
    "/media/",
    "/gallery/",
)


def _media_dedupe_key(url: str) -> str:
    parsed = urlparse(url)
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{parsed.path.lower()}"


def _normalized_media_basename(path: str) -> str:
    name = os.path.basename((path or "").lower())
    if not name:
        return ""
    stem, ext = os.path.splitext(name)
    stem = re.sub(r"[-_@](?:\d{2,5}x\d{2,5}|\d{2,5}w|large|medium|small|thumb|thumbnail|preview|orig|original)$", "", stem)
    stem = re.sub(r"(?:[-_](?:copy|scaled|resize|crop|web|mobile))+$", "", stem)
    return f"{stem}{ext}" if stem and ext else name


def _media_identity_keys(url: str) -> set[str]:
    parsed = urlparse(url or "")
    url_key = _media_dedupe_key(url)
    basename = _normalized_media_basename(parsed.path)
    keys = {url_key} if url_key else set()
    if parsed.netloc and basename:
        keys.add(f"basename:{parsed.netloc.lower()}:{basename}")
    return keys


def _prefer_large_news_media_url(url: str) -> str:
    url = str(url or "").strip()
    if "gdb.voanews.com" in url.lower() or "gdb.rferl.org" in url.lower():
        return re.sub(r"_w\d+_", "_w1200_", url)
    return url


def _looks_like_low_quality_media(url: str, title: str, kind: str) -> bool:
    text = f"{url} {title}".lower()
    if any(token in text for token in OPENNEWS_MEDIA_BAD_TOKENS):
        return True
    path = urlparse(url).path.lower()
    if kind == "image" and re.search(r"\.(svg|gif|ico)(?:$|\?)", path):
        return True
    return False


def _merge_media_items(*groups: Iterable[dict], limit: int = 24) -> list[dict]:
    merged: list[dict] = []
    seen: set[str] = set()
    for group in groups:
        for item in group or []:
            if not isinstance(item, dict) or not item.get("url"):
                continue
            identity_keys = _media_identity_keys(str(item.get("url") or ""))
            if not identity_keys or identity_keys & seen:
                continue
            if _looks_like_low_quality_media(str(item.get("url") or ""), str(item.get("title") or ""), str(item.get("kind") or "image")):
                continue
            seen.update(identity_keys)
            merged.append(dict(item))
            if len(merged) >= limit:
                return merged
    return merged


def _best_srcset_url(srcset: str) -> str:
    candidates = []
    for part in (srcset or "").split(","):
        bits = part.strip().split()
        if not bits:
            continue
        url = bits[0]
        width = 0
        if len(bits) > 1 and bits[1].endswith("w"):
            try:
                width = int(bits[1][:-1])
            except Exception:
                width = 0
        candidates.append((width, url))
    if not candidates:
        return ""
    return sorted(candidates, key=lambda item: item[0], reverse=True)[0][1]


def _extract_media_page_links(page_html: str, base_url: str, limit: int = 8) -> list[str]:
    links: list[str] = []
    seen: set[str] = set()
    base_host = urlparse(base_url).netloc.lower()
    for match in re.finditer(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>([\s\S]{0,260}?)</a>', page_html or "", flags=re.I):
        href, label_html = match.groups()
        full_url = urljoin(base_url, html.unescape(href or "").strip())
        parsed = urlparse(full_url)
        if not parsed.scheme.startswith("http") or parsed.netloc.lower() != base_host:
            continue
        lowered = full_url.lower()
        if not any(token in lowered for token in OPENNEWS_MEDIA_PAGE_TOKENS):
            continue
        label = _strip_tags(label_html).lower()
        if any(token in f"{lowered} {label}" for token in OPENNEWS_MEDIA_BAD_TOKENS):
            continue
        key = _media_dedupe_key(full_url)
        if key in seen:
            continue
        seen.add(key)
        links.append(full_url)
        if len(links) >= limit:
            break
    return links


def extract_article_media(page_html: str, base_url: str, limit: int = 12) -> list[dict]:
    media: list[dict] = []
    seen: set[str] = set()

    def add(url: str, kind: str, title: str = "", score: int = 0):
        url = html.unescape((url or "").strip())
        if not url:
            return
        full_url = urljoin(base_url, url)
        full_url = _prefer_large_news_media_url(full_url)
        dedupe_key = _media_dedupe_key(full_url)
        if not full_url.startswith("http") or dedupe_key in seen:
            return
        if _looks_like_low_quality_media(full_url, title, kind):
            return
        parsed_path = urlparse(full_url).path.lower()
        if kind == "image" and not re.search(r"\.(jpg|jpeg|png|webp)(?:$|\?)", parsed_path):
            if "image" not in full_url.lower():
                return
        if kind == "video" and not re.search(r"\.(mp4|mov|m4v|webm)(?:$|\?)", parsed_path):
            if not any(token in full_url.lower() for token in ("video", "download")):
                return
        seen.add(dedupe_key)
        media.append({
            "url": full_url,
            "kind": kind,
            "title": title[:120] or kind,
            "source_url": base_url,
            "score": score + (100 if kind == "video" else 0),
        })

    og_image = _extract_meta_content(page_html, ("og:image", "twitter:image"))
    if og_image:
        add(og_image, "image", "OpenGraph image", score=45)
    og_video = _extract_meta_content(page_html, ("og:video", "og:video:url", "twitter:player:stream"))
    if og_video:
        add(og_video, "video", "OpenGraph video", score=70)

    for match in re.finditer(r'<img([^>]*)>', page_html or "", flags=re.I):
        attrs = match.group(1) or ""
        srcset_match = re.search(r'(?:srcset|data-srcset)=["\']([^"\']+)["\']', attrs, flags=re.I)
        if srcset_match:
            add(_best_srcset_url(srcset_match.group(1)), "image", "article image", score=42)
        for attr in ("data-original", "data-lazy-src", "data-src", "src"):
            attr_match = re.search(rf'{attr}=["\']([^"\']+)["\']', attrs, flags=re.I)
            if attr_match:
                add(attr_match.group(1), "image", "article image", score=35)
    for match in re.finditer(r'<(?:source|video)[^>]+src=["\']([^"\']+)["\'][^>]*>', page_html or "", flags=re.I):
        add(match.group(1), "video", "article video", score=80)
    for match in re.finditer(r'<video[^>]+poster=["\']([^"\']+)["\'][^>]*>', page_html or "", flags=re.I):
        add(match.group(1), "image", "video poster", score=55)
    for match in re.finditer(r'href=["\']([^"\']+\.(?:mp4|mov|m4v|webm)(?:\?[^"\']*)?)["\']', page_html or "", flags=re.I):
        add(match.group(1), "video", "linked video", score=65)
    for match in re.finditer(r'["\'](?:contentUrl|thumbnailUrl|embedUrl)["\']\s*:\s*["\']([^"\']+)["\']', page_html or "", flags=re.I):
        url = match.group(1)
        kind = "video" if re.search(r"\.(mp4|mov|m4v|webm)(?:$|\?)", url, flags=re.I) else "image"
        add(url, kind, "structured media", score=60)
    media.sort(key=lambda item: int(item.get("score") or 0), reverse=True)
    for item in media:
        item.pop("score", None)
    return media[:limit]


def _extract_nested_source_media(page_html: str, base_url: str, limit: int = 8) -> list[dict]:
    nested_media: list[dict] = []
    for link in _extract_media_page_links(page_html, base_url, limit=limit):
        try:
            response = requests.get(link, headers=DEFAULT_HEADERS, timeout=12)
        except Exception:
            continue
        if response.status_code >= 400:
            continue
        nested_media.extend(extract_article_media(response.text, link, limit=limit))
        if len(nested_media) >= limit:
            break
    return _merge_media_items(nested_media, limit=limit)


def discover_related_opennews_media(source_id: str, query: str, *, article_url: str = "", limit: int = 16) -> list[dict]:
    source = _source_by_id(source_id)
    if not source or not query.strip():
        return []
    query = re.sub(r"\s+", " ", query).strip()
    search_targets: list[str] = []
    if source.search_url:
        search_targets.append(source.search_url.format(query=quote_plus(query)))
    if source.id == "dvids":
        search_targets.extend([
            f"https://www.dvidshub.net/search?q={quote_plus(query)}&type=video",
            f"https://www.dvidshub.net/search?q={quote_plus(query)}&type=image",
        ])
    related: list[dict] = []
    for search_url in search_targets:
        try:
            response = requests.get(search_url, headers=DEFAULT_HEADERS, timeout=15)
        except Exception:
            continue
        if response.status_code >= 400:
            continue
        related.extend(extract_article_media(response.text, search_url, limit=limit))
        for link in _extract_media_page_links(response.text, search_url, limit=6):
            if article_url and _media_dedupe_key(link) == _media_dedupe_key(article_url):
                continue
            try:
                detail_response = requests.get(link, headers=DEFAULT_HEADERS, timeout=12)
            except Exception:
                continue
            if detail_response.status_code >= 400:
                continue
            related.extend(extract_article_media(detail_response.text, link, limit=limit))
            if len(related) >= limit:
                break
        if len(related) >= limit:
            break
    return _merge_media_items(related, limit=limit)


def discover_broad_opennews_media(
    *,
    source_id: str = "",
    category: str = "all",
    queries: Iterable[str] = (),
    article_url: str = "",
    limit: int = 32,
) -> list[dict]:
    """按文案语义跨新闻源补找公开视频/图片素材。"""
    source_ids: list[str] = []
    if source_id:
        source_ids.append(source_id)
    for source in OPENNEWS_SOURCES:
        if category != "all" and source.category != category:
            continue
        if source.id not in source_ids:
            source_ids.append(source.id)
    if category == "military" and "dvids" not in source_ids:
        source_ids.insert(0, "dvids")

    collected: list[dict] = []
    for query in queries:
        if len(collected) >= limit:
            break
        for sid in source_ids[:5]:
            if len(collected) >= limit:
                break
            media = discover_related_opennews_media(
                sid,
                query,
                article_url=article_url,
                limit=max(6, min(12, limit - len(collected))),
            )
            for item in media:
                item = dict(item)
                item["related_query"] = query
                item["related_source_id"] = sid
                collected.append(item)
    return _merge_media_items(collected, limit=limit)


def _decode_search_result_url(url: str) -> str:
    url = html.unescape(url or "").strip()
    if not url:
        return ""
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    for key in ("uddg", "url", "u"):
        value = query.get(key, [""])[0]
        if value:
            return unquote(value)
    return url


def _is_allowed_general_web_url(url: str) -> bool:
    parsed = urlparse(url or "")
    if not parsed.scheme.startswith("http") or not parsed.netloc:
        return False
    host = parsed.netloc.lower()
    if any(token in host for token in GENERAL_WEB_BLOCKED_HOST_TOKENS):
        return False
    if any(token in url.lower() for token in ("login", "signup", "account", "subscribe")):
        return False
    return True


def _extract_general_search_links(page_html: str, base_url: str, limit: int = 8) -> list[str]:
    links: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>([\s\S]{4,260}?)</a>', page_html or "", flags=re.I):
        href, label_html = match.groups()
        label = _strip_tags(label_html)
        if len(label) < 4:
            continue
        full_url = _decode_search_result_url(urljoin(base_url, href))
        if not _is_allowed_general_web_url(full_url):
            continue
        if any(skip in full_url.lower() for skip in ("duckduckgo.com", "bing.com/search", "microsoft.com")):
            continue
        key = _media_dedupe_key(full_url)
        if not key or key in seen:
            continue
        seen.add(key)
        links.append(full_url)
        if len(links) >= limit:
            break
    return links


def _decode_embedded_media_url(raw_url: str) -> str:
    value = html.unescape(raw_url or "").strip()
    if not value:
        return ""
    try:
        value = bytes(value, "utf-8").decode("unicode_escape")
    except Exception:
        pass
    return value.replace("\\/", "/").strip()


def _extract_direct_search_media(page_html: str, search_url: str, search_terms: str, limit: int = 60) -> list[dict]:
    """Extract direct media links embedded in public image/video search result pages."""
    media: list[dict] = []
    seen: set[str] = set()
    patterns = (
        r'"(?:murl|mediaurl|contentUrl|thumbnailUrl|imgurl|poster)"\s*:\s*"([^"]+)"',
        r'&quot;(?:murl|mediaurl|contentUrl|thumbnailUrl|imgurl|poster)&quot;\s*:\s*&quot;([^&]+)&quot;',
        r'(https?://[^"\'>\s]+?\.(?:jpg|jpeg|png|webp|mp4|mov|m4v|webm)(?:\?[^"\'>\s]*)?)',
    )
    for pattern in patterns:
        for match in re.finditer(pattern, page_html or "", flags=re.I):
            raw_url = match.group(1)
            url = _decode_embedded_media_url(raw_url)
            if not url.startswith("http") or not _is_allowed_general_web_url(url):
                continue
            lower_path = urlparse(url).path.lower()
            kind = "video" if re.search(r"\.(mp4|mov|m4v|webm)(?:$|\?)", lower_path, flags=re.I) else "image"
            if kind == "image" and not re.search(r"\.(jpg|jpeg|png|webp)(?:$|\?)", lower_path, flags=re.I):
                continue
            if _looks_like_low_quality_media(url, search_terms, kind):
                continue
            identity_keys = _media_identity_keys(url)
            if not identity_keys or identity_keys & seen:
                continue
            seen.update(identity_keys)
            media.append({
                "url": url,
                "kind": kind,
                "title": f"search media: {search_terms}"[:120],
                "source_url": search_url,
                "source": "general_web_search_media",
                "related_query": search_terms,
            })
            if len(media) >= limit:
                return media
    return media


def discover_general_search_media(queries: Iterable[str], *, limit: int = 180) -> list[dict]:
    """直接从公开图片/视频搜索结果中提取媒体直链，作为网页爬取的补充。"""
    collected: list[dict] = []
    query_variants: list[str] = []
    for query in queries:
        query = _compact_query(str(query or ""), max_chars=80)
        if not query:
            continue
        for suffix in ("news photo", "official photo", "press photo", "news image", "news video", "footage", "b-roll"):
            variant = f"{query} {suffix}".strip()
            if variant.lower() not in {item.lower() for item in query_variants}:
                query_variants.append(variant)
    for search_terms in query_variants:
        if len(collected) >= limit:
            break
        for template in GENERAL_WEB_MEDIA_SEARCH_URLS:
            if len(collected) >= limit:
                break
            search_url = template.format(query=quote_plus(search_terms))
            try:
                response = requests.get(search_url, headers=DEFAULT_HEADERS, timeout=12)
            except Exception:
                continue
            if response.status_code >= 400:
                continue
            collected.extend(_extract_direct_search_media(response.text, search_url, search_terms, limit=80))
    return _merge_media_items(collected, limit=limit)


def discover_general_web_media(queries: Iterable[str], *, article_url: str = "", limit: int = 220) -> list[dict]:
    """从普通公开网页搜索结果里抓取图片/视频候选，保留来源供人工审核。"""
    collected: list[dict] = []
    visited_pages: set[str] = set()
    source_host = urlparse(article_url or "").netloc.lower()
    query_variants: list[str] = []
    for query in queries:
        query = _compact_query(str(query or ""), max_chars=80)
        if not query:
            continue
        for suffix in (
            "news photo video official",
            "image",
            "footage",
            "press photo",
            "b-roll",
            "latest photos",
            "official video",
            "official photo",
            "archive footage",
            "news footage",
            "public domain",
        ):
            variant = f"{query} {suffix}".strip()
            if variant.lower() not in {item.lower() for item in query_variants}:
                query_variants.append(variant)
    for search_terms in query_variants:
        if len(collected) >= limit:
            break
        for search_url_template in GENERAL_WEB_SEARCH_URLS:
            if len(collected) >= limit:
                break
            search_url = search_url_template.format(query=quote_plus(search_terms))
            try:
                response = requests.get(search_url, headers=DEFAULT_HEADERS, timeout=12)
            except Exception:
                continue
            if response.status_code >= 400:
                continue
            links = _extract_general_search_links(response.text, search_url, limit=28)
            for link in links:
                if len(collected) >= limit:
                    break
                page_key = _media_dedupe_key(link)
                if not page_key or page_key in visited_pages:
                    continue
                if source_host and urlparse(link).netloc.lower() == source_host:
                    continue
                visited_pages.add(page_key)
                try:
                    page_response = requests.get(link, headers=DEFAULT_HEADERS, timeout=14)
                except Exception:
                    continue
                if page_response.status_code >= 400 or "text/html" not in (page_response.headers.get("Content-Type", "").lower()):
                    continue
                media = extract_article_media(page_response.text, link, limit=36)
                if len(media) < 8:
                    media = _merge_media_items(media, _extract_nested_source_media(page_response.text, link, limit=18), limit=36)
                for item in media:
                    item = dict(item)
                    item["source_url"] = link
                    item["source"] = "general_web"
                    item["related_query"] = search_terms
                    collected.append(item)
                    if len(collected) >= limit:
                        break
    return _merge_media_items(collected, limit=limit)


NEWS_LINK_POSITIVE_TOKENS = (
    "news",
    "article",
    "release",
    "releases",
    "press",
    "statement",
    "briefing",
    "story",
    "media",
    "video",
    "image",
    "photo",
    "content",
    "view",
    "pressite",
    "news_content",
    "news-article",
    "/a/",
)


NEWS_LINK_NEGATIVE_TOKENS = (
    "javascript:",
    "mailto:",
    "#",
    "/search",
    "login",
    "signup",
    "subscribe",
    "privacy",
    "terms",
    "contact",
    "sitemap",
    ".css",
    ".js",
    ".pdf",
    ".zip",
)


def _looks_like_news_candidate_link(url: str, label: str, source: OpenNewsSource) -> bool:
    lowered_url = (url or "").lower()
    lowered_label = (label or "").lower()
    if not lowered_url.startswith("http"):
        return False
    if any(token in lowered_url for token in NEWS_LINK_NEGATIVE_TOKENS):
        return False
    parsed = urlparse(lowered_url)
    source_host = urlparse(source.url).netloc.lower()
    if parsed.netloc and source_host and source_host not in parsed.netloc and parsed.netloc not in source_host:
        return False
    if any(token in lowered_url for token in NEWS_LINK_POSITIVE_TOKENS):
        return True
    if re.search(r"/20\d{2}/\d{1,2}/\d{1,2}/", lowered_url) or re.search(r"/20\d{2}[-_/]\d{1,2}[-_/]\d{1,2}", lowered_url):
        return True
    if re.search(r"(?:article|release|news)[-_]?\d{4,}", lowered_url):
        return True
    if source.id == "voa_zh" and re.search(r"/a/[^/]+/\d+\.html", lowered_url):
        return True
    if source.id == "mnd_tw" and ("news" in lowered_url or "publishing" in lowered_url):
        return True
    if len(_title_tokens(label)) >= 3:
        return True
    return False


def _extract_json_ld_candidates(page_html: str, source: OpenNewsSource, base_url: str, limit: int = 24) -> list[dict]:
    candidates: list[dict] = []
    seen: set[str] = set()
    for match in re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>([\s\S]*?)</script>', page_html or "", flags=re.I):
        raw = html.unescape(match.group(1) or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        queue = data if isinstance(data, list) else [data]
        while queue:
            item = queue.pop(0)
            if isinstance(item, list):
                queue.extend(item)
                continue
            if not isinstance(item, dict):
                continue
            graph = item.get("@graph")
            if isinstance(graph, list):
                queue.extend(graph)
            item_type = item.get("@type") or ""
            item_types = item_type if isinstance(item_type, list) else [item_type]
            type_text = " ".join(str(t) for t in item_types).lower()
            if not any(token in type_text for token in ("newsarticle", "article", "reportage", "blogposting")):
                continue
            title = str(item.get("headline") or item.get("name") or "").strip()
            url = str(item.get("url") or item.get("mainEntityOfPage") or "").strip()
            if isinstance(item.get("mainEntityOfPage"), dict):
                url = str(item["mainEntityOfPage"].get("@id") or item["mainEntityOfPage"].get("url") or url)
            if not title or not url:
                continue
            full_url = urljoin(base_url, url)
            if not _looks_like_news_candidate_link(full_url, title, source):
                continue
            key = _canonical_news_url(full_url)
            if key in seen:
                continue
            seen.add(key)
            published = str(item.get("datePublished") or item.get("dateModified") or "")
            summary = str(item.get("description") or "")
            candidates.append(_candidate_from_source(source, title=title, url=full_url, summary=summary, published_at=published))
            if len(candidates) >= limit:
                return candidates
    return candidates


def _extract_published_from_page(page_html: str) -> str:
    published = _extract_meta_content(
        page_html,
        (
            "article:published_time",
            "article:modified_time",
            "date",
            "pubdate",
            "publishdate",
            "dc.date",
            "dc:date",
            "dcterms.created",
            "dcterms.modified",
            "sailthru.date",
        ),
    )
    if published:
        return published
    for match in re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>([\s\S]*?)</script>', page_html or "", flags=re.I):
        raw = html.unescape(match.group(1) or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        queue = data if isinstance(data, list) else [data]
        while queue:
            item = queue.pop(0)
            if isinstance(item, list):
                queue.extend(item)
                continue
            if not isinstance(item, dict):
                continue
            graph = item.get("@graph")
            if isinstance(graph, list):
                queue.extend(graph)
            published = str(item.get("datePublished") or item.get("dateModified") or "").strip()
            if published:
                return published
    text = _strip_tags(page_html or "")
    match = re.search(
        r"(20\d{2}[-/年]\d{1,2}[-/月]\d{1,2}(?:日)?(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?)",
        text,
    )
    return match.group(1) if match else ""


def _enrich_candidate_timestamp(candidate: dict) -> dict:
    item = dict(candidate)
    if item.get("published_ts"):
        return item
    url = str(item.get("url") or "")
    if not url.startswith("http"):
        return item
    try:
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=12)
    except Exception:
        return item
    if response.status_code >= 400 or "text/html" not in (response.headers.get("Content-Type", "").lower()):
        return item
    published = _extract_published_from_page(response.text)
    published_ts = _parse_timestamp(published)
    if published_ts:
        item["published_at"] = _strip_tags(published)[:100]
        item["published_ts"] = published_ts
        item["is_latest"] = _is_recent_news_candidate(item, days=2)
    if not item.get("summary"):
        summary = _extract_meta_content(response.text, ("og:description", "description", "twitter:description"))
        if summary:
            item["summary"] = _strip_tags(summary)[:420]
    return item


def _extract_html_candidates(page_html: str, source: OpenNewsSource, limit: int = 24, base_url: str | None = None) -> list[dict]:
    candidates: list[dict] = []
    seen: set[str] = set()
    base_url = base_url or source.latest_url or source.url
    candidates.extend(_extract_json_ld_candidates(page_html, source, base_url, limit=limit))
    for item in candidates:
        key = _canonical_news_url(str(item.get("url") or ""))
        if key:
            seen.add(key)
    for match in re.finditer(r'<a([^>]*)href=["\']([^"\']+)["\']([^>]*)>([\s\S]{4,360}?)</a>', page_html or "", flags=re.I):
        before_attrs, href, after_attrs, label_html = match.groups()
        attrs = f"{before_attrs or ''} {after_attrs or ''}"
        label = _strip_tags(label_html)
        title_match = re.search(r'(?:title|aria-label)=["\']([^"\']+)["\']', attrs or "", flags=re.I)
        if title_match and len(_strip_tags(title_match.group(1))) > len(label):
            label = _strip_tags(title_match.group(1))
        if len(label) < 4:
            continue
        href = urljoin(base_url, html.unescape(href or "").strip())
        if not _looks_like_news_candidate_link(href, label, source):
            continue
        key = _canonical_news_url(href)
        if not key or key in seen:
            continue
        seen.add(key)
        context_start = max(0, match.start() - 420)
        context_end = min(len(page_html or ""), match.end() + 420)
        context = page_html[context_start:context_end]
        published_match = re.search(
            r'(20\d{2}[-/年]\d{1,2}[-/月]\d{1,2}(?:日)?(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?)',
            _strip_tags(context),
        )
        published_at = published_match.group(1) if published_match else ""
        candidates.append(_candidate_from_source(source, title=label[:180], url=href, published_at=published_at))
        if len(candidates) >= limit:
            break
    return candidates


def _fetch_rss_candidates(source: OpenNewsSource, limit: int = 8) -> list[dict]:
    if not source.rss_url:
        return []
    try:
        response = requests.get(source.rss_url, headers=DEFAULT_HEADERS, timeout=15)
    except Exception:
        return []
    if response.status_code >= 400:
        return []
    try:
        root = ET.fromstring(response.content)
    except ET.ParseError:
        return []
    items = root.findall(".//item") or root.findall(".//{http://www.w3.org/2005/Atom}entry")
    results = []
    for item in items[:limit]:
        title = item.findtext("title") or item.findtext("{http://www.w3.org/2005/Atom}title") or ""
        link = item.findtext("link") or ""
        if not link:
            link_node = item.find("{http://www.w3.org/2005/Atom}link")
            link = link_node.attrib.get("href", "") if link_node is not None else ""
        description = item.findtext("description") or item.findtext("summary") or ""
        published = item.findtext("pubDate") or item.findtext("published") or ""
        if not title or not link:
            continue
        results.append(_candidate_from_source(source, title=title, url=link, summary=description, published_at=published))
    return results


def _source_crawl_urls(source: OpenNewsSource, query: str, *, pages: int = 5) -> list[str]:
    if query and source.search_url:
        base_url = source.search_url.format(query=quote_plus(query))
    else:
        base_url = source.latest_url
    if not base_url:
        return []
    extra_latest_urls = {
        "dvids": [
            "https://www.dvidshub.net/news",
            "https://www.dvidshub.net/news/list/most-recent",
            "https://www.dvidshub.net/video",
            "https://www.dvidshub.net/image",
        ],
        "dod": [
            "https://www.defense.gov/News/Releases/",
            "https://www.defense.gov/News/News-Stories/",
            "https://www.defense.gov/News/Transcripts/",
            "https://www.defense.gov/News/Contracts/",
        ],
        "indopacom": [
            "https://www.pacom.mil/Media/News/",
            "https://www.pacom.mil/Media/News/News-Article-View/",
            "https://www.pacom.mil/Media/News/Tag/46565/indopacific/",
        ],
        "mod_jp": [
            "https://www.mod.go.jp/j/press/news/",
            "https://www.mod.go.jp/j/press/kisha/",
            "https://www.mod.go.jp/j/press/press_release/",
        ],
        "mofa_jp": [
            "https://www.mofa.go.jp/press/release/index.html",
            "https://www.mofa.go.jp/press/kaiken/index.html",
            "https://www.mofa.go.jp/mofaj/press/release/index.html",
        ],
        "mnd_tw": [
            "https://www.mnd.gov.tw/News.aspx",
            "https://www.mnd.gov.tw/Publish.aspx",
            "https://www.mnd.gov.tw/NewUpload/News.aspx",
        ],
        "voa_zh": [
            "https://www.voachinese.com/z/1739",
            "https://www.voachinese.com/z/1754",
            "https://www.voachinese.com/z/1779",
            "https://www.voachinese.com/z/1785",
        ],
    }
    urls: list[str] = []
    seen: set[str] = set()
    base_urls = [base_url]
    if not query:
        base_urls.extend(extra_latest_urls.get(source.id, []))
    for base in base_urls:
        parsed = urlparse(base)
        for page in range(1, max(1, pages) + 1):
            candidates = [base]
            if page > 1:
                query_items = parse_qsl(parsed.query, keep_blank_values=True)
                query_items_with_page = [(key, value) for key, value in query_items if key.lower() not in {"page", "p", "pg"}]
                query_items_with_page.append(("page", str(page)))
                candidates.append(urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", urlencode(query_items_with_page), "")))
                if parsed.path and not parsed.path.rstrip("/").endswith(str(page)):
                    candidates.append(urlunparse((parsed.scheme, parsed.netloc, f"{parsed.path.rstrip('/')}/page/{page}", "", parsed.query, "")))
                candidates.append(urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", urlencode(query_items_with_page[:-1] + [("Page", str(page))]), "")))
                candidates.append(urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", urlencode(query_items_with_page[:-1] + [("start", str((page - 1) * 20))]), "")))
            for url in candidates:
                key = _canonical_news_url(url)
                if key and key not in seen:
                    seen.add(key)
                    urls.append(url)
    return urls


def search_opennews_candidates_with_stats(
    query: str,
    source_ids: Iterable[str] | None = None,
    *,
    category: str = "all",
    limit_per_source: int = 32,
) -> dict:
    query = (query or "").strip()
    selected = set(source_ids or [])
    category = (category or "all").strip() or "all"
    sources = [
        source
        for source in OPENNEWS_SOURCES
        if (not selected or source.id in selected) and (category == "all" or source.category == category)
    ]
    all_candidates: list[dict] = []
    stats: list[dict] = []
    for source in sources:
        source_raw_count = 0
        source_error = ""
        skipped_error_count = 0
        crawled_urls: list[str] = []
        if source.rss_url and not query:
            rss_candidates = _fetch_rss_candidates(source, limit=limit_per_source * 4)
            source_raw_count += len(rss_candidates)
            all_candidates.extend(rss_candidates)

        crawl_urls = _source_crawl_urls(source, query, pages=5)
        for crawl_url in crawl_urls:
            crawled_urls.append(crawl_url)
            try:
                response = requests.get(crawl_url, headers=DEFAULT_HEADERS, timeout=18)
            except Exception as exc:
                skipped_error_count += 1
                if source_raw_count <= 0:
                    source_error = str(exc)
                continue
            if response.status_code >= 400:
                skipped_error_count += 1
                if source_raw_count <= 0:
                    source_error = f"HTTP {response.status_code}"
                continue
            page_candidates = _extract_html_candidates(response.text, source, limit=limit_per_source, base_url=crawl_url)
            source_raw_count += len(page_candidates)
            all_candidates.extend(page_candidates)
        stats.append({
            "source_id": source.id,
            "source_name": source.name,
            "raw_count": source_raw_count,
            "crawled_pages": len(crawled_urls),
            "skipped_error_count": skipped_error_count,
            "error": source_error if source_raw_count <= 0 else "",
        })
    deduped = _dedupe_opennews_candidates(all_candidates)
    enriched: list[dict] = []
    missing_timestamp_checked = 0
    for item in sorted(deduped, key=lambda value: float(value.get("published_ts") or 0), reverse=True):
        if not item.get("published_ts") and missing_timestamp_checked < 90:
            item = _enrich_candidate_timestamp(item)
            missing_timestamp_checked += 1
        enriched.append(item)
    recent_items = [item for item in enriched if _is_recent_news_candidate(item, days=2)]
    final_items = sorted(recent_items, key=lambda item: float(item.get("published_ts") or 0), reverse=True)[:150]
    final_counts: dict[str, int] = {}
    for item in final_items:
        sid = str(item.get("source_id") or "")
        final_counts[sid] = final_counts.get(sid, 0) + 1
    for stat in stats:
        stat["deduped_count"] = final_counts.get(stat["source_id"], 0)
        stat["recent_window"] = _recent_news_window_label(days=2)
    return {
        "candidates": final_items,
        "stats": stats,
        "raw_count": len(all_candidates),
        "deduped_count": len(deduped),
        "recent_count": len(recent_items),
        "recent_window": _recent_news_window_label(days=2),
        "missing_timestamp_checked": missing_timestamp_checked,
    }


def search_opennews_candidates(
    query: str,
    source_ids: Iterable[str] | None = None,
    *,
    category: str = "all",
    limit_per_source: int = 32,
) -> list[dict]:
    return search_opennews_candidates_with_stats(
        query,
        source_ids=source_ids,
        category=category,
        limit_per_source=limit_per_source,
    ).get("candidates", [])


def fetch_article_bundle(url: str, source_id: str = "") -> dict:
    response = requests.get(url, headers=DEFAULT_HEADERS, timeout=20)
    if response.status_code >= 400:
        raise RuntimeError(f"新闻正文抓取失败：HTTP {response.status_code} {url}")
    page_html = response.text
    source = _source_by_id(source_id) if source_id else None
    published = _extract_meta_content(page_html, ("article:published_time", "date", "pubdate", "publishdate", "dc.date"))
    title = _extract_meta_content(page_html, ("og:title", "twitter:title"))
    summary = _extract_meta_content(page_html, ("og:description", "description", "twitter:description"))
    article_media = _merge_media_items(
        extract_article_media(page_html, url, limit=16),
        _extract_nested_source_media(page_html, url, limit=8),
        limit=24,
    )
    return {
        "text": _strip_tags(page_html)[:12000],
        "media": article_media,
        "published_at": published,
        "published_ts": _parse_timestamp(published),
        "title": title,
        "summary": summary,
        "source": source.__dict__ if source else {},
    }


def fetch_article_text(url: str) -> str:
    return str(fetch_article_bundle(url).get("text") or "")


def _collect_related_article_media(related_articles: list[dict], *, limit: int = 48) -> list[dict]:
    collected: list[dict] = []
    for item in related_articles or []:
        if not isinstance(item, dict):
            continue
        url = str(item.get("url") or "").strip()
        if not url:
            continue
        try:
            bundle = fetch_article_bundle(url, str(item.get("source_id") or ""))
        except Exception:
            continue
        for media in bundle.get("media") or []:
            if not isinstance(media, dict):
                continue
            enriched = dict(media)
            enriched["source_url"] = url
            enriched["source"] = enriched.get("source") or "related_article"
            enriched["title"] = enriched.get("title") or item.get("title") or item.get("source_name") or ""
            enriched["related_query"] = item.get("title") or ""
            collected.append(enriched)
            if len(collected) >= limit:
                return _merge_media_items(collected, limit=limit)
    return _merge_media_items(collected, limit=limit)


def generate_opennews_draft(*, article: dict, target_market: str = "cn", notes: str = "") -> dict:
    url = str(article.get("url") or "")
    article_fetch_warning = ""
    try:
        article_bundle = fetch_article_bundle(url, str(article.get("source_id") or "")) if url else {}
    except Exception as exc:
        article_fetch_warning = str(exc)
        article_bundle = {}
    article_text = str(article_bundle.get("text") or "")
    article_media = list(article_bundle.get("media") or [])
    published_at = article.get("published_at") or article_bundle.get("published_at") or ""
    related_articles = article.get("related_articles") if isinstance(article.get("related_articles"), list) else []
    related_article_media = _collect_related_article_media(related_articles, limit=64)
    related_context = "\n".join(
        f"- {item.get('source_name') or item.get('trend_domain') or 'source'}｜{item.get('published_at') or ''}｜{item.get('title') or ''}｜{item.get('url') or ''}"
        for item in related_articles[:8]
        if isinstance(item, dict)
    )
    language = "繁體中文" if target_market == "tw" else ("日本語" if target_market == "jp" else "简体中文")
    prompt = f"""
你是 iHouse 的 OpenNews 新闻视频编辑。请根据公开新闻源生成短视频新闻口播稿。

输出语言：{language}
目标长度：新闻视频口播，约 45-60 秒。简体/繁体中文约 220-320 字；日语约 420-560 字。
来源名称：{article.get("source_name")}
授权：{article.get("license")}
标题：{article.get("title")}
新闻发布时间：{published_at or "未知"}
链接：{url}
管理员补充要求：{notes or "无"}

原始网页正文节选：
{article_text or article.get("summary") or article.get("title") or "无正文。"}

正文抓取状态：
{article_fetch_warning or "正文抓取成功。"}

相关英文报道列表（如果有，用于判断热点是否被多源报道，不要把未证实内容写成确定事实）：
{related_context or "无"}

要求：
1. 只根据来源正文、候选摘要、相关报道和管理员补充写，不要编造未出现的事实。
2. 涉及军事、外交、台海、战争议题时，语气保持新闻说明，不煽动，不下定论。
3. 必须明确体现新闻来源和发布时间；如果发布时间未知，要写“来源页面未标注明确发布时间”。
4. 口播稿只写一段视频文案，像新闻主播口播一样，把事件、背景、影响和下一步看点讲清楚，但不要扩展成长篇评论。
5. 口播稿建议 5-7 句：第一句直接说“谁做了什么”；中间 2-3 句补关键背景、数据、现场或相关方；后面说明为什么值得关注；最后一句自然收尾。
6. 开头不要空泛，必须出现新闻里的具体主体，例如公司、机构、人物、国家、产品、政策或事件名。
7. 不要写“这条新闻主要涉及”“引发关注”“后续仍需关注”“具体细节仍需以原始报道为准”“这条新闻值得关注的地方”“从已经公开的信息看”“这件事不只是单一事件”“后续还要看当事方说明”这类模板句。
8. 口播稿不要写成提纲、栏目、小节或说明文，不要出现“一句话看事件”“背景方面”“影响方面”“首先/其次/最后”这类结构提示语，也不要用项目符号。
9. 新闻来源和发布时间要自然融入口播正文里；事实边界只用一句轻轻带过，不要单独分成“来源标注”“背景分析”“影响分析”式段落。
10. 句子要短而顺，适合字幕切分；整篇读起来要像完整新闻视频播出稿，不要像摘要拼接，也不要少于目标长度太多。
11. 必须全部使用“输出语言”生成：标题、摘要、口播稿、素材关键词、事实核验提醒、来源标注、新闻时间标注。
12. 输出 JSON：
{{
  "video_title": "...",
  "summary": "...",
  "script": "...",
  "material_keywords": ["舰艇", "军演"],
  "material_visual_plan": [
    {{
      "title": "这一组画面的中文名称",
      "script_context": "对应哪一句或哪一段口播内容",
      "visual_need": "要找什么具体画面，不要写抽象概念",
      "queries": ["英文检索词1", "英文检索词2", "英文检索词3"]
    }}
  ],
  "fact_check_notes": ["..."],
  "source_credit": "...",
  "news_time_label": "..."
}}

素材计划要求：
- material_visual_plan 必须围绕口播顺序拆成 4-8 组具体画面。
- 每组 queries 必须是具体英文画面检索词，优先包含人物、机构、地点、产品、装备、事件名。
- 不要使用泛化词，例如 generic politics、government meeting、press briefing，除非文案确实在讲发布会。
- 如果文案讲 SpaceX/IPO/马斯克，画面计划应包含 SpaceX rocket、Elon Musk、IPO stock market、investors 等，不要写 White House。
- 如果文案讲中国芯片/科技，画面计划应包含 semiconductor、chip factory、technology company、China tech 等。
- 如果文案讲白宫发言人，才使用 White House press briefing、spokesperson 等。
""".strip()
    draft = _request_opennews_model_json(prompt, max_output_tokens=4096)
    draft = _normalize_opennews_draft_payload(
        draft,
        article=article,
        target_market=target_market,
        published_at=str(published_at or ""),
    )
    draft = _polish_opennews_broadcast_copy(
        draft=draft,
        article=article,
        article_text=article_text,
        related_context=related_context,
        target_market=target_market,
        published_at=str(published_at or ""),
    )
    if _looks_like_generic_opennews_script(str(draft.get("script") or ""), article):
        replacement = _local_opennews_language_fallback(
            article=article,
            target_market=target_market,
            published_at=str(published_at or ""),
        )
        draft["video_title"] = replacement.get("video_title") or draft.get("video_title")
        draft["summary"] = replacement.get("summary") or draft.get("summary")
        draft["script"] = replacement.get("script") or draft.get("script")
    keyword_values = draft.get("material_keywords") or []
    if not isinstance(keyword_values, list):
        keyword_values = [str(keyword_values)]
    related_query = " ".join(
        part
        for part in [
            str(article.get("title") or ""),
            str(article.get("summary") or ""),
            " ".join(str(item).strip() for item in keyword_values[:4] if str(item).strip()),
        ]
        if part.strip()
    )
    category = str(article.get("category") or "all")
    broad_queries = _expanded_media_queries(
        str(article.get("title") or ""),
        str(article.get("summary") or ""),
        str(draft.get("summary") or ""),
        " ".join(str(item).strip() for item in keyword_values if str(item).strip()),
        str(draft.get("script") or "")[:600],
        category=category,
        limit=12,
    )
    related_media = discover_broad_opennews_media(
        source_id=str(article.get("source_id") or ""),
        category=category,
        queries=broad_queries or [related_query],
        article_url=url,
        limit=80,
    )
    article_media = _merge_media_items(article_media, related_article_media, related_media, limit=180)
    draft["_meta"] = {
        "model": os.getenv("ANTHROPIC_OPENNEWS_MODEL", "claude-sonnet-4-6") if ANTHROPIC_CLIENT else _get_openai_relay_model(),
        "model_provider": "Claude" if ANTHROPIC_CLIENT else "API中转模型",
        "source_url": url,
        "source_name": article.get("source_name"),
        "license": article.get("license"),
        "category": article.get("category"),
        "category_name": article.get("category_name"),
        "published_at": published_at,
        "article_media": article_media,
        "material_search_queries": broad_queries,
        "related_article_media_count": len(related_article_media),
        "related_source_media_count": len(related_media),
        "general_web_media_count": 0,
        "direct_search_media_count": 0,
        "strict_news_media_only": True,
        "created_at": time.time(),
    }
    return draft


def _split_script_sentences(script: str) -> list[str]:
    parts = re.split(r"(?<=[。！？!?；;])\s*", script or "")
    sentences = [part.strip() for part in parts if part and part.strip()]
    if sentences:
        return sentences
    fallback = re.split(r"[\n\r]+", script or "")
    return [part.strip() for part in fallback if part and part.strip()]


def _join_sentences(sentences: list[str], start: int, end: int) -> str:
    text = "".join(sentences[start:end]).strip()
    return text or "请关注这条新闻的最新公开信息与后续进展。"


def _estimate_duration_seconds(text: str, *, minimum: int = 6, maximum: int = 38) -> int:
    # 中文新闻口播大约 4-5 字/秒，给数字人和素材段留一点呼吸空间。
    visible_chars = len(re.sub(r"\s+", "", text or ""))
    return max(minimum, min(maximum, int(round(visible_chars / 4.2)) or minimum))


def _rank_media_for_segment(media: list[dict], *, segment_text: str, keyword_text: str, limit: int = 24) -> list[dict]:
    segment_terms = _expanded_media_queries(segment_text, keyword_text, category="all", limit=12)
    lowered_terms = [term.lower() for term in segment_terms if term]
    compact_terms = [re.sub(r"[^a-z0-9\u4e00-\u9fffぁ-んァ-ヶ一-龯]+", "", term.lower()) for term in lowered_terms]
    ranked: list[tuple[int, dict]] = []
    for index, item in enumerate(media or []):
        haystack = " ".join(
            str(item.get(field) or "")
            for field in ("url", "title", "source_url", "related_query", "theme_title")
        ).lower()
        compact_haystack = re.sub(r"[^a-z0-9\u4e00-\u9fffぁ-んァ-ヶ一-龯]+", "", haystack)
        score = 100 if str(item.get("kind") or "").lower() == "video" else 0
        for term in lowered_terms:
            if term and term in haystack:
                score += 55
        for term in compact_terms:
            if term and len(term) >= 4 and term in compact_haystack:
                score += 35
        related_query = str(item.get("related_query") or "").lower()
        if related_query:
            score += 18
            for term in lowered_terms:
                if term and (term in related_query or related_query in term):
                    score += 65
        source = str(item.get("source") or "")
        if source == "general_web_search_media":
            score += 20
        if source == "general_web":
            score += 12
        score -= index
        ranked.append((score, dict(item)))
    ranked.sort(key=lambda item: item[0], reverse=True)
    return [item for _, item in ranked[:limit]]


def _dedupe_media_items(media: list[dict]) -> list[dict]:
    deduped: list[dict] = []
    seen: set[str] = set()
    for item in media or []:
        if not isinstance(item, dict) or not item.get("url"):
            continue
        identity_keys = _media_identity_keys(str(item.get("url") or ""))
        if not identity_keys or identity_keys & seen:
            continue
        seen.update(identity_keys)
        deduped.append(dict(item))
    return deduped


def _strict_news_media_items(media: list[dict]) -> list[dict]:
    blocked_sources = {"general_web", "general_web_search_media"}
    kept: list[dict] = []
    for item in media or []:
        if not isinstance(item, dict):
            continue
        source = str(item.get("source") or "").strip().lower()
        if source in blocked_sources:
            continue
        kept.append(dict(item))
    return kept


def _build_opennews_theme_plan(script: str, keyword_text: str, category: str, *, max_themes: int = 6) -> list[dict]:
    sentences = _split_script_sentences(script)
    if not sentences:
        return []
    theme_count = min(max_themes, max(3, len(sentences)))
    chunk_size = max(1, round(len(sentences) / theme_count))
    themes: list[dict] = []
    cursor = 0
    while cursor < len(sentences):
        chunk = "".join(sentences[cursor:cursor + chunk_size]).strip()
        cursor += chunk_size
        if not chunk:
            continue
        queries = _expanded_media_queries(chunk, category=category, limit=5)
        if not queries:
            queries = _expanded_media_queries(chunk, keyword_text, category=category, limit=5)
        themes.append({
            "title": queries[0] if queries else _compact_query(chunk, max_chars=24),
            "script": chunk,
            "queries": queries,
        })
        if len(themes) >= max_themes:
            if cursor < len(sentences):
                tail = "".join(sentences[cursor:]).strip()
                if tail:
                    themes[-1]["script"] = f"{themes[-1]['script']}{tail}"
            break
    return themes


def _theme_plan_from_visual_plan(draft: dict, category: str) -> list[dict]:
    visual_plan = draft.get("material_visual_plan") or draft.get("visual_plan") or []
    if not isinstance(visual_plan, list):
        return []
    themes: list[dict] = []
    for item in visual_plan[:8]:
        if not isinstance(item, dict):
            continue
        title = _compact_query(str(item.get("title") or item.get("visual_need") or ""), max_chars=36)
        script_context = _strip_tags(str(item.get("script_context") or item.get("script") or ""))
        visual_need = _strip_tags(str(item.get("visual_need") or item.get("description") or ""))
        raw_queries = item.get("queries") or item.get("search_queries") or []
        if not isinstance(raw_queries, list):
            raw_queries = [str(raw_queries)]
        queries: list[str] = []
        seen: set[str] = set()
        for query in raw_queries:
            compact = _compact_query(str(query or ""), max_chars=70)
            key = compact.lower()
            if compact and key not in seen:
                seen.add(key)
                queries.append(compact)
        if len(queries) < 2:
            queries.extend(_expanded_media_queries(title, visual_need, script_context, category=category, limit=5))
        deduped_queries: list[str] = []
        seen.clear()
        for query in queries:
            key = query.lower()
            if len(query) < 2 or key in seen:
                continue
            seen.add(key)
            deduped_queries.append(query)
            if len(deduped_queries) >= 4:
                break
        if not deduped_queries:
            continue
        themes.append({
            "title": title or deduped_queries[0],
            "script": script_context or visual_need or title or deduped_queries[0],
            "visual_need": visual_need,
            "queries": deduped_queries,
            "source": "ai_visual_plan",
        })
    return themes


def _rank_media_by_theme_plan(
    media: list[dict],
    theme_plan: list[dict],
    keyword_text: str,
    *,
    per_theme_limit: int = 2,
    total_limit: int = 60,
) -> list[dict]:
    if not theme_plan:
        return _dedupe_media_items(_rank_media_for_segment(media, segment_text=keyword_text, keyword_text=keyword_text, limit=total_limit))
    used: set[str] = set()
    ordered: list[dict] = []
    for theme_index, theme in enumerate(theme_plan):
        theme_text = " ".join([str(theme.get("script") or ""), str(theme.get("visual_need") or ""), " ".join(theme.get("queries") or [])])
        ranked = _rank_media_for_segment(media, segment_text=theme_text, keyword_text=keyword_text, limit=60)
        picked = 0
        for item in ranked:
            identity_keys = _media_identity_keys(str(item.get("url") or ""))
            if not identity_keys or identity_keys & used:
                continue
            used.update(identity_keys)
            enriched = dict(item)
            enriched["theme_index"] = theme_index
            enriched["theme_title"] = theme.get("title") or ""
            ordered.append(enriched)
            picked += 1
            if picked >= per_theme_limit or len(ordered) >= total_limit:
                break
        if len(ordered) >= total_limit:
            break
    for item in _rank_media_for_segment(media, segment_text=keyword_text, keyword_text=keyword_text, limit=120):
        if len(ordered) >= total_limit:
            break
        identity_keys = _media_identity_keys(str(item.get("url") or ""))
        if not identity_keys or identity_keys & used:
            continue
        used.update(identity_keys)
        ordered.append(dict(item))
    return ordered


def _enrich_theme_plan_media(
    theme_plan: list[dict],
    category: str,
    *,
    article_url: str = "",
    source_id: str = "",
    limit_per_theme: int = 10,
) -> list[dict]:
    """For each script theme, crawl only configured news/official sources."""
    collected: list[dict] = []
    for theme_index, theme in enumerate(theme_plan or []):
        queries = [str(query).strip() for query in (theme.get("queries") or []) if str(query).strip()]
        if not queries:
            queries = _expanded_media_queries(str(theme.get("script") or ""), category=category, limit=3)
        if not queries:
            continue
        theme_media = _merge_media_items(
            discover_broad_opennews_media(
                source_id=source_id,
                category=category,
                queries=queries[:2],
                article_url=article_url,
                limit=max(10, limit_per_theme // 2),
            ),
            discover_general_search_media(queries[:2], limit=limit_per_theme),
            discover_general_web_media(queries[:2], article_url=article_url, limit=limit_per_theme),
            limit=limit_per_theme,
        )
        for item in theme_media:
            enriched = dict(item)
            enriched["theme_index"] = theme_index
            enriched["theme_title"] = theme.get("title") or ""
            collected.append(enriched)
    return _merge_media_items(collected, limit=max(limit_per_theme, len(theme_plan or []) * limit_per_theme))


def _distribute_ranked_media(ranked_groups: list[list[dict]], *, per_segment_limit: int = 12) -> list[list[dict]]:
    """把同一个媒体 URL 尽量只分配给一个素材段。"""
    used: set[str] = set()
    normalized_groups = [_dedupe_media_items(group) for group in ranked_groups]
    assigned_groups: list[list[dict]] = [[] for _ in normalized_groups]
    cursors = [0 for _ in normalized_groups]
    while True:
        changed = False
        for group_index, group in enumerate(normalized_groups):
            if len(assigned_groups[group_index]) >= per_segment_limit:
                continue
            while cursors[group_index] < len(group):
                item = group[cursors[group_index]]
                cursors[group_index] += 1
                identity_keys = _media_identity_keys(str(item.get("url") or ""))
                if not identity_keys or identity_keys & used:
                    continue
                used.update(identity_keys)
                assigned_groups[group_index].append(item)
                changed = True
                break
        if not changed:
            break
    return assigned_groups


def build_opennews_script_data(*, draft: dict, article: dict | None = None, target_market: str = "cn") -> dict:
    """把 OpenNews 新闻稿转换成纯素材新闻视频脚本。"""
    article = article or {}
    script = str(draft.get("script") or "").strip()
    if not script:
        raise ValueError("新闻稿草稿缺少口播稿，无法生成视频")

    keywords = draft.get("material_keywords") or []
    if not isinstance(keywords, list):
        keywords = [str(keywords)]
    keyword_text = "、".join(str(item).strip() for item in keywords if str(item).strip()) or str(draft.get("video_title") or article.get("title") or "news")
    expanded_queries = list(draft.get("_meta", {}).get("material_search_queries") or [])
    search_keyword = _opennews_entity_search_query(keywords, article, draft, limit=6)
    if not search_keyword:
        search_keyword = " ".join(str(item).strip() for item in keywords[:4] if str(item).strip()) or str(article.get("title") or "news")
    source_name = article.get("source_name") or draft.get("_meta", {}).get("source_name") or "OpenNews"
    article_media = _strict_news_media_items(list(draft.get("_meta", {}).get("article_media") or article.get("media") or []))
    news_time_label = str(draft.get("news_time_label") or article.get("published_at") or draft.get("_meta", {}).get("published_at") or "来源页面未标注明确发布时间").strip()
    category_name = str(article.get("category_name") or draft.get("_meta", {}).get("category_name") or "新闻").strip()
    category_id = str(article.get("category") or draft.get("_meta", {}).get("category") or "all")
    theme_plan = _theme_plan_from_visual_plan(draft, category_id) or _build_opennews_theme_plan(script, keyword_text, category_id, max_themes=6)
    theme_extra_media = _enrich_theme_plan_media(
        theme_plan,
        category_id,
        article_url=str(article.get("url") or draft.get("_meta", {}).get("source_url") or ""),
        source_id=str(article.get("source_id") or ""),
        limit_per_theme=10,
    )
    ranked_media = _rank_media_by_theme_plan(
        _merge_media_items(article_media, theme_extra_media, limit=90),
        theme_plan,
        keyword_text,
        per_theme_limit=2,
        total_limit=60,
    )

    segments = [
        {
            "type": "material",
            "start": 0,
            "duration": _estimate_duration_seconds(script, minimum=35, maximum=180),
            "script": script,
            "material_keyword": keyword_text,
            "material_search_keyword": search_keyword,
            "material_desc": f"严格使用与 {source_name} 这条新闻正文、相关报道或同类官方新闻源直接相关的图片/视频素材；宁可素材少，也不要使用和新闻事实不对应的泛化画面。",
            "source_materials": ranked_media,
            "material_theme_plan": theme_plan,
            "theme_extra_media_count": len(theme_extra_media),
            "disable_free_material_fallback": True,
            "opennews_material_only": True,
            "strict_news_media_only": True,
        },
    ]

    cursor = 0
    for seg in segments:
        seg["start"] = cursor
        cursor += int(seg.get("duration") or 0)
        seg["end"] = cursor

    title = str(draft.get("video_title") or article.get("title") or "OpenNews 新闻视频").strip()
    summary = str(draft.get("summary") or "").strip()
    source_credit = str(draft.get("source_credit") or article.get("url") or "").strip()
    social_post = "\n".join(part for part in [title, f"类别：{category_name}", f"时间：{news_time_label}", summary, f"来源：{source_credit}" if source_credit else ""] if part)
    return {
        "title": title,
        "cover_title": title[:28],
        "total_duration": cursor,
        "segments": segments,
        "social_post": social_post,
        "opennews": {
            "article": article,
            "draft_meta": draft.get("_meta") or {},
            "source_credit": source_credit,
            "news_time_label": news_time_label,
            "category_name": category_name,
            "fact_check_notes": draft.get("fact_check_notes") or [],
            "material_keywords": keywords,
            "article_media": article_media,
        },
    }


def save_opennews_payload(root: Path, name: str, payload: dict) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{name}_{int(time.time())}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
