import json
import re
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import parse_qs, quote_plus, unquote, urljoin, urlparse
from xml.etree import ElementTree as ET

import requests

from material_library import register_material_file
from source_ingest import DEFAULT_HEADERS


BASE_DIR = Path(__file__).resolve().parent
HARVEST_DIR = BASE_DIR / "material_harvest"
HARVEST_DIR.mkdir(exist_ok=True)
HARVEST_JOBS_PATH = HARVEST_DIR / "jobs.json"
HARVEST_CANDIDATES_PATH = HARVEST_DIR / "candidates.json"
HARVEST_LOCK = threading.Lock()

URL_RE = re.compile(r"(https?://[^\s<>'\"）)]+)", re.IGNORECASE)
IMG_RE = re.compile(r"<img[^>]+src=['\"]([^'\"]+)['\"]", re.IGNORECASE)
SOURCE_RE = re.compile(r"<source[^>]+src=['\"]([^'\"]+)['\"]", re.IGNORECASE)
VIDEO_RE = re.compile(r"<video[^>]+src=['\"]([^'\"]+)['\"]", re.IGNORECASE)
ANCHOR_RE = re.compile(r"<a[^>]+href=['\"]([^'\"]+)['\"][^>]*>(.*?)</a>", re.IGNORECASE | re.DOTALL)
META_CONTENT_RE_TPL = r"<meta[^>]+(?:property|name)=['\"]{name}['\"][^>]+content=['\"]([^'\"]+)['\"]"
TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
WHITESPACE_RE = re.compile(r"\s+")

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".m4v", ".webm"}

NEWS_HARVEST_PRESETS = {
    "军事": {
        "topic": "military defense news b-roll warship fighter jet drone missile military exercise press briefing",
        "notes": "优先官方军方、政府、通讯社和新闻机构页面；避免血腥、尸体、受伤人员和暴力近景。",
        "tags": ["军事", "国防", "军演", "舰艇", "战机", "无人机", "导弹"],
    },
    "政治": {
        "topic": "politics government news b-roll White House parliament congress press conference diplomacy leaders",
        "notes": "优先政府官网、议会、白宫、外交部门、新闻发布会和官方会议画面。",
        "tags": ["政治", "政府", "白宫", "国会", "外交", "记者会"],
    },
    "科技": {
        "topic": "technology news b-roll semiconductor chip data center robotics laboratory innovation conference",
        "notes": "优先芯片、数据中心、机器人、实验室、发布会、科技公司办公场景。",
        "tags": ["科技", "芯片", "数据中心", "机器人", "实验室"],
    },
    "AI": {
        "topic": "artificial intelligence AI news b-roll data center GPU servers robot machine learning technology",
        "notes": "优先 AI 数据中心、GPU、服务器机房、机器人、企业 AI 工具、科技会议画面。",
        "tags": ["AI", "人工智能", "GPU", "服务器", "数据中心", "机器人"],
    },
    "金融": {
        "topic": "finance market news b-roll stock exchange central bank trading floor oil price economy banking",
        "notes": "优先交易所、央行、银行、交易屏幕、油价、财经新闻背景。",
        "tags": ["金融", "股市", "央行", "银行", "交易所", "油价"],
    },
    "房产": {
        "topic": "real estate housing market news b-roll apartment building homes mortgage city skyline property",
        "notes": "优先住宅、公寓、城市街景、房贷、售楼、房地产市场相关画面。",
        "tags": ["房产", "房地产", "住宅", "公寓", "房贷", "城市街景"],
    },
    "移民": {
        "topic": "immigration visa passport airport border news b-roll migration government office students",
        "notes": "优先签证、护照、机场、移民局、边境、留学、政府窗口类画面；避免敏感人脸特写。",
        "tags": ["移民", "签证", "护照", "机场", "边境", "留学"],
    },
    "通用新闻": {
        "topic": "breaking news b-roll press conference city street newsroom official building map data screen",
        "notes": "优先新闻通用场景：记者会、城市、办公楼、资料图、地图、数据屏。",
        "tags": ["新闻", "记者会", "城市", "资料图", "办公楼"],
    },
}

NEWS_TOPIC_HARVEST_PRESETS = [
    {
        "id": "xai_grok_bedrock",
        "name": "xAI Grok 与 Amazon Bedrock",
        "category": "AI",
        "topic": "xAI Grok Amazon Bedrock AWS generative AI model cloud official news logo data center",
        "notes": "优先 xAI/Grok、Amazon AWS、Amazon Bedrock、云计算产品页、企业AI平台、数据中心和官方发布图；不要混入 OpenAI、英伟达、机器人或手机图。",
        "tags": ["AI", "xAI", "Grok", "Amazon", "AWS", "Bedrock", "云计算", "大模型"],
        "patterns": [r"\bgrok\b", r"\bxai\b", r"\bx\.ai\b", r"\bbedrock\b", r"\baws\b", r"\bamazon\b"],
    },
    {
        "id": "anthropic_claude",
        "name": "Anthropic 与 Claude",
        "category": "AI",
        "topic": "Anthropic Claude AI model company official news logo office product announcement",
        "notes": "优先 Anthropic/Claude 官方图片、公司新闻、产品发布、办公室和AI产品演示图；不要混入 OpenAI、Google、Nvidia 等其他公司主视觉。",
        "tags": ["AI", "Anthropic", "Claude", "大模型", "AI公司"],
        "patterns": [r"\banthropic\b", r"\bclaude\b"],
    },
    {
        "id": "openai_chatgpt",
        "name": "OpenAI 与 ChatGPT",
        "category": "AI",
        "topic": "OpenAI ChatGPT GPT AI model company official news logo office product demo",
        "notes": "优先 OpenAI/ChatGPT 官方图片、产品发布、办公室、应用界面和大模型新闻图；避免混入其他公司主视觉。",
        "tags": ["AI", "OpenAI", "ChatGPT", "GPT", "大模型"],
        "patterns": [r"\bopenai\b", r"\bchatgpt\b", r"\bgpt\b"],
    },
    {
        "id": "google_gemini_ai",
        "name": "Google Gemini 与 AI",
        "category": "AI",
        "topic": "Google Gemini AI model DeepMind official news logo product demo cloud AI",
        "notes": "优先 Google/Gemini/DeepMind 官方图片、产品发布、AI工具演示和云端AI场景；不要混入 OpenAI/Anthropic 主视觉。",
        "tags": ["AI", "Google", "Gemini", "DeepMind", "大模型"],
        "patterns": [r"\bgemini\b", r"\bgoogle\b", r"\bdeepmind\b", r"\balphabet\b"],
    },
    {
        "id": "meta_ai",
        "name": "Meta AI 与 Llama",
        "category": "AI",
        "topic": "Meta AI Llama artificial intelligence model official news logo product demo",
        "notes": "优先 Meta AI、Llama、Meta 官方新闻图、产品发布和AI工具画面；避免无关社交媒体截图。",
        "tags": ["AI", "Meta", "Llama", "大模型"],
        "patterns": [r"\bmeta\b", r"\bllama\b", r"\bfacebook\b"],
    },
    {
        "id": "microsoft_copilot_ai",
        "name": "Microsoft Copilot 与 Azure AI",
        "category": "AI",
        "topic": "Microsoft Copilot Azure AI official news logo product demo cloud artificial intelligence",
        "notes": "优先 Microsoft Copilot、Azure AI、微软官方发布、企业办公AI和云计算场景。",
        "tags": ["AI", "Microsoft", "Copilot", "Azure", "云计算"],
        "patterns": [r"\bmicrosoft\b", r"\bcopilot\b", r"\bazure\b"],
    },
    {
        "id": "deepseek_ai",
        "name": "DeepSeek 与中国大模型",
        "category": "AI",
        "topic": "DeepSeek AI model Chinese artificial intelligence company official news logo product screenshot",
        "notes": "优先 DeepSeek 官方、产品界面、模型发布、中国AI公司和数据中心场景；避免泛化机器人图。",
        "tags": ["AI", "DeepSeek", "中国AI", "大模型"],
        "patterns": [r"\bdeepseek\b"],
    },
    {
        "id": "ai_nvidia_chip",
        "name": "AI芯片与英伟达",
        "category": "AI",
        "topic": "Nvidia Jensen Huang AI chip GPU data center semiconductor artificial intelligence news official photo b-roll",
        "notes": "优先英伟达新闻室、数据中心、GPU服务器、芯片晶圆、AI大会、黄仁勋公开采访与发布会画面；避免泛化政治人物和无关商业配图。",
        "tags": ["AI", "英伟达", "黄仁勋", "GPU", "AI芯片", "数据中心", "半导体"],
        "patterns": [r"\bnvidia\b", r"\bjensen\s+huang\b", r"英伟达", r"黄仁勋", r"黃仁勳"],
    },
    {
        "id": "ai_model_companies",
        "name": "AI大模型与科技公司",
        "category": "AI",
        "topic": "OpenAI Anthropic Google AI Meta Microsoft Apple artificial intelligence model news office conference product demo",
        "notes": "优先 OpenAI、Anthropic、Google、Meta、微软、苹果等公司新闻室、产品发布、办公室、AI工具演示、科技会议画面。",
        "tags": ["AI", "大模型", "OpenAI", "Anthropic", "Google AI", "Meta AI", "微软AI", "苹果AI"],
        "patterns": [r"\bai model\b", r"\bgenerative ai\b", r"大模型", r"人工智能"],
    },
    {
        "id": "data_center_servers",
        "name": "数据中心与服务器机房",
        "category": "科技",
        "topic": "data center server racks cloud computing GPU server room AI infrastructure electricity cooling official photo",
        "notes": "优先服务器机柜、云计算数据中心、GPU集群、冷却系统、电力基础设施画面，适合 AI 能源、电力、算力新闻兜底。",
        "tags": ["科技", "数据中心", "服务器", "云计算", "算力", "电力"],
        "patterns": [r"\bdata center\b", r"\bserver\b", r"\bcloud\b", r"数据中心", r"服务器", r"算力"],
    },
    {
        "id": "robotics_humanoid",
        "name": "机器人与自动化",
        "category": "科技",
        "topic": "humanoid robot robotics automation warehouse industrial robot laboratory robot conference official photo video",
        "notes": "优先人形机器人、机械臂、仓储自动化、工业机器人、实验室测试和机器人发布会画面；避免玩具、动漫或无关科幻图。",
        "tags": ["科技", "机器人", "人形机器人", "工业机器人", "自动化", "实验室"],
        "patterns": [r"\brobot", r"\bhumanoid\b", r"机器人", r"人形机器人"],
    },
    {
        "id": "white_house_us_politics",
        "name": "白宫与美国政治",
        "category": "政治",
        "topic": "White House US politics press briefing president congress official photo government meeting diplomacy news",
        "notes": "优先白宫、新闻发布厅、国会、政府会议、外交会谈和官方记者会画面；适合美国政策、总统讲话、监管新闻。",
        "tags": ["政治", "白宫", "美国政府", "国会", "记者会", "外交"],
        "patterns": [r"\bwhite house\b", r"\bcongress\b", r"\bgovernment\b", r"白宫", r"国会"],
    },
    {
        "id": "trump_us_election",
        "name": "特朗普与美国大选",
        "category": "政治",
        "topic": "Donald Trump US election campaign rally White House policy press conference official photo news",
        "notes": "优先特朗普公开活动、竞选集会、政策讲话、白宫/国会相关画面；不要抓娱乐八卦或明显恶搞图。",
        "tags": ["政治", "特朗普", "美国大选", "竞选", "政策"],
        "patterns": [r"\btrump\b", r"特朗普"],
    },
    {
        "id": "military_conflict",
        "name": "军事冲突与北约",
        "category": "军事",
        "topic": "military conflict NATO Ukraine Russia defense warship fighter jet missile drone army exercise official photo b-roll",
        "notes": "优先官方军方、DVIDS、NATO、国防部、军演、舰艇、战机、无人机、防空系统画面；禁止血腥、尸体、伤者近景和裸露画面。",
        "tags": ["军事", "北约", "乌克兰", "俄罗斯", "军舰", "战机", "导弹", "无人机"],
        "patterns": [r"\bnato\b", r"\bukraine\b", r"\brussia\b", r"\bmissile\b", r"\bdrone\b", r"北约", r"乌克兰", r"俄罗斯"],
    },
    {
        "id": "middle_east_iran_israel",
        "name": "中东与伊朗以色列",
        "category": "军事",
        "topic": "Middle East Iran Israel conflict diplomacy military oil Strait of Hormuz official photo news map warship",
        "notes": "优先中东地图、外交会谈、军舰、油轮、霍尔木兹海峡、政府发布会画面；避免爆炸伤亡近景。",
        "tags": ["军事", "中东", "伊朗", "以色列", "霍尔木兹", "油轮", "外交"],
        "patterns": [r"\biran\b", r"\bisrael\b", r"\bhormuz\b", r"伊朗", r"以色列", r"霍尔木兹"],
    },
    {
        "id": "oil_energy",
        "name": "石油能源与油价",
        "category": "金融",
        "topic": "oil price energy market crude oil tanker refinery OPEC gas station pipeline Strait of Hormuz news photo",
        "notes": "优先油井、油轮、炼油厂、加油站、输油管道、能源设施和油价市场图；适合油价、能源安全和中东影响新闻。",
        "tags": ["金融", "能源", "石油", "油价", "OPEC", "炼油厂", "油轮"],
        "patterns": [r"\boil\b", r"\bcrude\b", r"\bopec\b", r"\benergy\b", r"石油", r"油价"],
    },
    {
        "id": "fed_inflation_markets",
        "name": "美联储与金融市场",
        "category": "金融",
        "topic": "Federal Reserve inflation interest rate stock market trading floor Wall Street central bank economy official photo",
        "notes": "优先美联储大楼、央行发布会、交易所、交易屏幕、华尔街、银行、经济数据图画面。",
        "tags": ["金融", "美联储", "通胀", "降息", "股市", "华尔街", "央行"],
        "patterns": [r"\bfed\b", r"\bfederal reserve\b", r"\bpowell\b", r"\bstock market\b", r"美联储", r"通胀", r"股市"],
    },
    {
        "id": "real_estate_us_housing",
        "name": "美国房产与住宅市场",
        "category": "房产",
        "topic": "US housing market real estate homes apartment mortgage suburb city skyline property sign news photo",
        "notes": "优先美国住宅区、公寓楼、房产经纪牌、城市天际线、房贷合同、看房场景；适合房价、租金、百万美元首套房新闻。",
        "tags": ["房产", "美国房产", "住宅", "公寓", "房贷", "城市街景"],
        "patterns": [r"\bhousing\b", r"\breal estate\b", r"\bmortgage\b", r"\bhome prices\b", r"房产", r"房地产", r"房贷"],
    },
    {
        "id": "immigration_visa",
        "name": "移民签证与机场",
        "category": "移民",
        "topic": "immigration visa passport airport border government office students migration policy official photo news",
        "notes": "优先护照、签证窗口、机场、移民局、边境、大学校园和政府窗口画面；避免敏感人脸特写。",
        "tags": ["移民", "签证", "护照", "机场", "边境", "留学"],
        "patterns": [r"\bimmigration\b", r"\bvisa\b", r"\bpassport\b", r"\bborder\b", r"移民", r"签证", r"护照"],
    },
    {
        "id": "general_press_briefing",
        "name": "通用记者会与新闻兜底",
        "category": "通用新闻",
        "topic": "news press conference official building newsroom city street map data screen public statement photo b-roll",
        "notes": "优先记者会、新闻发布厅、官方建筑、城市街景、地图、数据屏、新闻编辑室；用于没有明确实体图时兜底。",
        "tags": ["通用新闻", "记者会", "新闻发布", "城市街景", "数据屏"],
        "patterns": [r"\bpress briefing\b", r"\bnews conference\b", r"记者会", r"新闻发布"],
    },
]

CATEGORY_SEARCH_FALLBACKS = {
    "军事": [
        "DVIDS military exercise b-roll",
        "defense news warship fighter jet drone missile official photo",
        "US Department of Defense military press briefing photo",
        "NATO military exercise official photo",
    ],
    "政治": [
        "government press conference official photo",
        "White House parliament congress diplomacy official photo",
        "political leaders summit press briefing news photo",
    ],
    "科技": [
        "technology semiconductor chip data center robotics news photo",
        "tech conference laboratory innovation official photo",
    ],
    "AI": [
        "artificial intelligence data center GPU servers robot news photo",
        "AI conference machine learning technology official photo",
    ],
    "金融": [
        "stock exchange central bank trading floor economy news photo",
        "finance market oil price banking official photo",
    ],
    "房产": [
        "real estate housing market apartment mortgage city skyline news photo",
        "property market homes residential building official photo",
    ],
    "移民": [
        "immigration visa passport airport border government office news photo",
        "migration students visa office official photo",
    ],
    "通用新闻": [
        "breaking news press conference city street newsroom official building photo",
    ],
}

CATEGORY_SEED_SOURCE_URLS = {
    "军事": [
        "https://www.dvidshub.net/search?q=military+exercise&type=image",
        "https://www.defense.gov/Multimedia/Photos/",
        "https://www.nato.int/cps/en/natohq/photos.htm",
    ],
    "政治": [
        "https://www.whitehouse.gov/briefing-room/",
        "https://www.state.gov/press-releases/",
        "https://www.gov.uk/search/news-and-communications",
    ],
    "科技": [
        "https://www.nasa.gov/images/",
        "https://www.nist.gov/news-events/news",
        "https://www.energy.gov/listings/articles",
    ],
    "AI": [
        "https://openai.com/news/",
        "https://www.nvidia.com/en-us/about-nvidia/newsroom/",
        "https://blog.google/technology/ai/",
    ],
    "金融": [
        "https://www.federalreserve.gov/newsevents.htm",
        "https://www.ecb.europa.eu/press/html/index.en.html",
        "https://www.nyse.com/news",
    ],
    "房产": [
        "https://www.nar.realtor/newsroom",
        "https://www.redfin.com/news/",
        "https://www.zillow.com/research/",
    ],
    "移民": [
        "https://www.uscis.gov/newsroom",
        "https://www.dhs.gov/news-releases",
        "https://www.canada.ca/en/immigration-refugees-citizenship/news.html",
    ],
    "通用新闻": [
        "https://www.reuters.com/pictures/",
        "https://apnews.com/",
    ],
}


def _news_item_text(item: dict) -> str:
    parts = [
        item.get("title"),
        item.get("title_zh"),
        item.get("translated_title"),
        item.get("original_title"),
        item.get("english_title"),
        item.get("summary"),
        item.get("summary_zh"),
        item.get("translated_summary"),
        item.get("description"),
        item.get("content"),
        item.get("category"),
        item.get("batch_category"),
        item.get("source_name"),
    ]
    return " ".join(str(part or "") for part in parts if str(part or "").strip())


def suggest_hotspot_material_topics(news_items: list[dict], *, limit: int = 12) -> list[dict]:
    """Pick concrete material topics from recent OpenNews items.

    This is intentionally entity-first. Broad categories like "AI" are useful only
    after specific entities such as Grok, Claude, Nvidia or White House have been
    considered, because those are what make visual matching precise.
    """
    scored: list[tuple[int, int, dict]] = []
    joined_items = [_news_item_text(item) for item in news_items if isinstance(item, dict)]
    all_text = "\n".join(joined_items)
    for index, preset in enumerate(NEWS_TOPIC_HARVEST_PRESETS):
        patterns = list(preset.get("patterns") or [])
        if patterns:
            hit_count = 0
            for text in joined_items:
                if any(re.search(pattern, text, flags=re.I) for pattern in patterns):
                    hit_count += 1
            if hit_count <= 0:
                continue
            score = hit_count * 100 + max(0, 30 - index)
        else:
            topic_terms = [term for term in re.split(r"\s+", str(preset.get("topic") or "").lower()) if len(term) >= 4]
            hit_count = sum(1 for term in topic_terms[:16] if term in all_text.lower())
            if hit_count <= 0:
                continue
            score = hit_count * 10 + max(0, 10 - index)
        scored.append((score, -index, preset))
    scored.sort(reverse=True, key=lambda row: (row[0], row[1]))
    selected = [dict(row[2]) for row in scored[: max(1, int(limit or 12))]]
    if selected:
        return selected
    # If the latest batch is thin or the source text is too generic, seed the most
    # useful evergreen topics so the library still grows in the right direction.
    fallback_ids = {
        "xai_grok_bedrock",
        "anthropic_claude",
        "openai_chatgpt",
        "ai_nvidia_chip",
        "data_center_servers",
        "white_house_us_politics",
        "fed_inflation_markets",
        "real_estate_us_housing",
    }
    return [dict(item) for item in NEWS_TOPIC_HARVEST_PRESETS if item.get("id") in fallback_ids][: max(1, int(limit or 12))]


def _now() -> float:
    return time.time()


def _clean_text(text: str) -> str:
    return WHITESPACE_RE.sub(" ", str(text or "").strip())


def _extract_urls(text: str) -> list[str]:
    seen = set()
    urls = []
    for match in URL_RE.findall(str(text or "")):
        url = str(match or "").rstrip(".,;，。；、】）)]")
        if not url or url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def _load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_jobs() -> list[dict]:
    data = _load_json(HARVEST_JOBS_PATH, {"jobs": []})
    return list(data.get("jobs") or [])


def _save_jobs(rows: list[dict]) -> None:
    _save_json(HARVEST_JOBS_PATH, {"jobs": rows})


def _load_candidates() -> list[dict]:
    data = _load_json(HARVEST_CANDIDATES_PATH, {"candidates": []})
    return list(data.get("candidates") or [])


def _save_candidates(rows: list[dict]) -> None:
    _save_json(HARVEST_CANDIDATES_PATH, {"candidates": rows})


def _normalize_job(job: dict) -> dict:
    category = _clean_text(job.get("category") or "")
    return {
        "id": str(job.get("id") or uuid.uuid4().hex[:12]),
        "topic": _clean_text(job.get("topic") or ""),
        "category": category,
        "source_urls": _extract_urls("\n".join(job.get("source_urls") or [])),
        "discovered_source_urls": _extract_urls("\n".join(job.get("discovered_source_urls") or [])),
        "search_notes": _clean_text(job.get("search_notes") or ""),
        "status": str(job.get("status") or "queued"),
        "message": _clean_text(job.get("message") or ""),
        "error": _clean_text(job.get("error") or ""),
        "candidate_count": int(job.get("candidate_count") or 0),
        "created_at": float(job.get("created_at") or _now()),
        "updated_at": float(job.get("updated_at") or _now()),
        "created_by_username": _clean_text(job.get("created_by_username") or ""),
        "created_by_display_name": _clean_text(job.get("created_by_display_name") or ""),
    }


def _normalize_candidate(candidate: dict) -> dict:
    asset_url = str(candidate.get("asset_url") or "").strip()
    parsed = urlparse(asset_url)
    return {
        "id": str(candidate.get("id") or uuid.uuid4().hex[:12]),
        "job_id": str(candidate.get("job_id") or "").strip(),
        "topic": _clean_text(candidate.get("topic") or ""),
        "category": _clean_text(candidate.get("category") or ""),
        "tags": list(candidate.get("tags") or []),
        "kind": str(candidate.get("kind") or "image"),
        "title": _clean_text(candidate.get("title") or ""),
        "page_title": _clean_text(candidate.get("page_title") or ""),
        "page_excerpt": _clean_text(candidate.get("page_excerpt") or ""),
        "source_url": str(candidate.get("source_url") or "").strip(),
        "asset_url": asset_url,
        "domain": parsed.netloc.lower(),
        "source_site": parsed.netloc.lower(),
        "source_type": _clean_text(candidate.get("source_type") or "web"),
        "safety_status": _clean_text(candidate.get("safety_status") or "needs_review"),
        "license_note": _clean_text(candidate.get("license_note") or "网页公开候选素材，导入前请管理员确认来源和画面安全。"),
        "status": str(candidate.get("status") or "pending"),
        "notes": _clean_text(candidate.get("notes") or ""),
        "created_at": float(candidate.get("created_at") or _now()),
        "updated_at": float(candidate.get("updated_at") or _now()),
        "imported_material_id": str(candidate.get("imported_material_id") or "").strip(),
    }


def _extract_meta(html_text: str, names: list[str]) -> str:
    for name in names:
        pattern = re.compile(META_CONTENT_RE_TPL.format(name=re.escape(name)), re.IGNORECASE | re.DOTALL)
        match = pattern.search(html_text or "")
        if match:
            return _clean_text(match.group(1))
    return ""


def _extract_page_title(html_text: str) -> str:
    title = _extract_meta(html_text, ["og:title", "twitter:title"])
    if title:
        return title
    match = TITLE_RE.search(html_text or "")
    return _clean_text(match.group(1)) if match else ""


def _extract_excerpt(html_text: str) -> str:
    excerpt = _extract_meta(html_text, ["description", "og:description", "twitter:description"])
    if excerpt:
        return excerpt
    paragraphs = re.findall(r"<p[^>]*>(.*?)</p>", html_text or "", flags=re.IGNORECASE | re.DOTALL)
    for paragraph in paragraphs:
        text = _clean_text(re.sub(r"<[^>]+>", " ", paragraph))
        if len(text) >= 24:
            return text[:220]
    return ""


def _looks_like_asset(url: str) -> bool:
    suffix = Path(urlparse(url).path).suffix.lower()
    return suffix in IMAGE_EXTENSIONS | VIDEO_EXTENSIONS


def _kind_for_asset_url(url: str) -> str:
    suffix = Path(urlparse(url).path).suffix.lower()
    if suffix in VIDEO_EXTENSIONS:
        return "video"
    return "image"


def _extract_asset_urls(page_url: str, html_text: str) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    seen = set()

    def add(url: str, kind: str):
        normalized = urljoin(page_url, str(url or "").strip())
        if not normalized.startswith(("http://", "https://")):
            return
        if normalized in seen:
            return
        if kind == "image" and not (_looks_like_asset(normalized) or "image" in normalized or "img" in normalized):
            return
        seen.add(normalized)
        found.append((normalized, kind))

    for meta_name in ["og:image", "twitter:image", "og:image:url"]:
        value = _extract_meta(html_text, [meta_name])
        if value:
            add(value, "image")
    for meta_name in ["og:video", "twitter:player:stream"]:
        value = _extract_meta(html_text, [meta_name])
        if value:
            add(value, "video")

    for value in IMG_RE.findall(html_text or "")[:20]:
        add(value, "image")
    for value in VIDEO_RE.findall(html_text or "")[:10]:
        add(value, "video")
    for value in SOURCE_RE.findall(html_text or "")[:10]:
        add(value, _kind_for_asset_url(value))
    return found[:24]


def _discover_source_urls_from_bing_news(query: str, limit: int = 8) -> list[str]:
    if not query.strip():
        return []
    rss_url = f"https://www.bing.com/news/search?q={quote_plus(query)}&format=rss"
    response = requests.get(rss_url, headers=DEFAULT_HEADERS, timeout=20, allow_redirects=True)
    response.raise_for_status()
    root = ET.fromstring(response.text or "")
    seen = set()
    urls = []
    for item in root.findall(".//item"):
        link = _clean_text(item.findtext("link", ""))
        if not link or link in seen:
            continue
        seen.add(link)
        urls.append(link)
        if len(urls) >= limit:
            break
    return urls


def _discover_source_urls_from_duckduckgo(query: str, limit: int = 8) -> list[str]:
    if not query.strip():
        return []
    search_url = f"https://duckduckgo.com/html/?q={quote_plus(query)}"
    response = requests.get(search_url, headers=DEFAULT_HEADERS, timeout=20, allow_redirects=True)
    response.raise_for_status()
    html_text = response.text or ""
    seen = set()
    urls = []
    for href, label in ANCHOR_RE.findall(html_text):
        url = _clean_text(urljoin(search_url, href))
        parsed = urlparse(url)
        if "duckduckgo.com" in parsed.netloc.lower():
            params = parse_qs(parsed.query)
            redirect_target = (params.get("uddg") or params.get("u") or [""])[0]
            if redirect_target:
                url = unquote(redirect_target)
        if not url.startswith(("http://", "https://")):
            continue
        host = urlparse(url).netloc.lower()
        if "duckduckgo.com" in host:
            continue
        if url in seen:
            continue
        seen.add(url)
        urls.append(url)
        if len(urls) >= limit:
            break
    return urls


def _preset_for_category(category: str) -> dict:
    return NEWS_HARVEST_PRESETS.get(str(category or "").strip(), {})


def build_harvest_query(topic: str, search_notes: str = "", category: str = "") -> str:
    preset = _preset_for_category(category)
    query_parts = [topic.strip(), preset.get("topic", "").strip(), search_notes.strip(), "image video news official source"]
    return " ".join(part for part in query_parts if part)


def _harvest_query_variants(topic: str, search_notes: str = "", category: str = "") -> list[str]:
    preset = _preset_for_category(category)
    variants = [
        build_harvest_query(topic, search_notes, category),
        " ".join(part for part in [topic.strip(), "news photo b-roll official source"] if part),
        " ".join(part for part in [preset.get("topic", "").strip(), "photo video b-roll"] if part),
    ]
    variants.extend(CATEGORY_SEARCH_FALLBACKS.get(str(category or "").strip(), []))
    deduped: list[str] = []
    seen: set[str] = set()
    for query in variants:
        query = _clean_text(query)
        if not query or query in seen:
            continue
        seen.add(query)
        deduped.append(query)
    return deduped


def _fallback_search_source_urls(topic: str, category: str = "", limit: int = 10) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    queries = _harvest_query_variants(topic, "", category)
    for query in queries[:4]:
        for url in (
            f"https://www.bing.com/images/search?q={quote_plus(query)}",
            f"https://www.bing.com/news/search?q={quote_plus(query)}",
        ):
            if url not in seen:
                seen.add(url)
                urls.append(url)
            if len(urls) >= limit:
                return urls
    for url in CATEGORY_SEED_SOURCE_URLS.get(str(category or "").strip(), []):
        if url not in seen:
            seen.add(url)
            urls.append(url)
        if len(urls) >= limit:
            break
    return urls[:limit]


def discover_source_urls(topic: str, search_notes: str = "", limit: int = 10, category: str = "") -> list[str]:
    queries = _harvest_query_variants(topic, search_notes, category)
    if not queries:
        return []
    candidates = []
    errors = []
    for query in queries:
        remaining = max(limit - len(candidates), 1)
        for fn in (_discover_source_urls_from_bing_news, _discover_source_urls_from_duckduckgo):
            try:
                candidates.extend(fn(query, limit=remaining))
            except Exception as exc:
                errors.append(str(exc))
        if len(candidates) >= limit:
            break
    deduped = []
    seen = set()
    for url in candidates:
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)
        if len(deduped) >= limit:
            break
    if not deduped:
        deduped = _fallback_search_source_urls(topic, category=category, limit=limit)
    return deduped[:limit]


def _fetch_candidate_rows(topic: str, source_url: str, *, category: str = "", tags: list[str] | None = None) -> list[dict]:
    response = requests.get(source_url, headers=DEFAULT_HEADERS, timeout=20, allow_redirects=True)
    response.raise_for_status()
    html_text = response.text or ""
    page_title = _extract_page_title(html_text)
    page_excerpt = _extract_excerpt(html_text)
    rows = []
    for index, (asset_url, kind) in enumerate(_extract_asset_urls(source_url, html_text), start=1):
        rows.append(
            _normalize_candidate(
                {
                    "topic": topic,
                    "category": category,
                    "tags": tags or [],
                    "kind": kind,
                    "title": page_title or f"{topic or '候选素材'} {index}",
                    "page_title": page_title,
                    "page_excerpt": page_excerpt,
                    "source_url": source_url,
                    "asset_url": asset_url,
                    "source_type": "web_page_asset",
                    "safety_status": "needs_review",
                    "status": "pending",
                }
            )
        )
    return rows


def create_harvest_job(
    *,
    topic: str,
    source_text: str,
    search_notes: str,
    category: str = "",
    created_by_username: str,
    created_by_display_name: str,
) -> dict:
    preset = _preset_for_category(category)
    if category and not topic.strip():
        topic = preset.get("topic", category)
    if preset.get("notes") and preset.get("notes") not in search_notes:
        search_notes = " ".join(part for part in [search_notes, preset.get("notes")] if part)
    job = _normalize_job(
        {
            "topic": topic,
            "category": category,
            "source_urls": _extract_urls(source_text),
            "search_notes": search_notes,
            "status": "queued",
            "message": "等待开始采集",
            "created_by_username": created_by_username,
            "created_by_display_name": created_by_display_name,
        }
    )
    with HARVEST_LOCK:
        jobs = _load_jobs()
        jobs = [row for row in jobs if str(row.get("id")) != job["id"]]
        jobs.append(job)
        _save_jobs(jobs)
    return job


def _update_job(job_id: str, updates: dict) -> dict:
    with HARVEST_LOCK:
        jobs = _load_jobs()
        index = next((idx for idx, row in enumerate(jobs) if str(row.get("id")) == str(job_id)), -1)
        if index < 0:
            raise FileNotFoundError("采集任务不存在")
        merged = dict(jobs[index] or {})
        merged.update(updates or {})
        merged["updated_at"] = _now()
        normalized = _normalize_job(merged)
        jobs[index] = normalized
        _save_jobs(jobs)
    return normalized


def list_harvest_jobs() -> list[dict]:
    with HARVEST_LOCK:
        jobs = [_normalize_job(row) for row in _load_jobs()]
    return sorted(jobs, key=lambda row: row.get("created_at", 0), reverse=True)


def list_harvest_candidates(*, status: str = "", job_id: str = "") -> list[dict]:
    with HARVEST_LOCK:
        rows = [_normalize_candidate(row) for row in _load_candidates()]
    if status:
        rows = [row for row in rows if row.get("status") == status]
    if job_id:
        rows = [row for row in rows if row.get("job_id") == job_id]
    return sorted(rows, key=lambda row: row.get("created_at", 0), reverse=True)


def _append_candidates(job_id: str, rows: list[dict]) -> list[dict]:
    normalized_rows = []
    with HARVEST_LOCK:
        candidates = _load_candidates()
        existing_urls = {str(row.get("asset_url") or "").strip() for row in candidates if str(row.get("job_id") or "") == str(job_id)}
        for row in rows:
            candidate = _normalize_candidate({**row, "job_id": job_id})
            if candidate["asset_url"] in existing_urls:
                continue
            existing_urls.add(candidate["asset_url"])
            candidates.append(candidate)
            normalized_rows.append(candidate)
        _save_candidates(candidates)
    return normalized_rows


def run_harvest_job(job_id: str) -> dict:
    job = _update_job(job_id, {"status": "running", "message": "正在抓取网页素材候选"})
    source_urls = list(job.get("source_urls") or [])
    category = str(job.get("category") or "").strip()
    preset_tags = list((_preset_for_category(category).get("tags") or []))
    discovered_source_urls = []
    if not source_urls:
        discovered_source_urls = discover_source_urls(job.get("topic", ""), job.get("search_notes", ""), limit=10, category=category)
        if discovered_source_urls:
            job = _update_job(
                job_id,
                {
                    "message": f"已自动发现 {len(discovered_source_urls)} 条来源，正在抓取候选素材",
                    "discovered_source_urls": discovered_source_urls,
                },
            )
            source_urls = list(discovered_source_urls)
    if not source_urls:
        return _update_job(job_id, {"status": "failed", "error": "请至少提供一条来源链接，或填写可搜索的采集主题", "message": "没有可采集的来源链接"})

    collected: list[dict] = []
    errors: list[str] = []
    for source_url in source_urls:
        try:
            collected.extend(_fetch_candidate_rows(job.get("topic", ""), source_url, category=category, tags=preset_tags))
        except Exception as exc:
            errors.append(f"{source_url}: {exc}")

    added = _append_candidates(job_id, collected)
    if not added and errors:
        return _update_job(job_id, {"status": "failed", "candidate_count": 0, "error": "；".join(errors[:3]), "message": "采集失败"})
    final_message = f"已抓取 {len(added)} 条候选素材"
    if errors:
        final_message += f"，另有 {len(errors)} 条来源抓取失败"
    return _update_job(
        job_id,
        {
            "status": "done",
            "message": final_message,
            "error": "；".join(errors[:3]),
            "candidate_count": len(added),
            "discovered_source_urls": discovered_source_urls or job.get("discovered_source_urls") or [],
        },
    )


def run_harvest_job_async(job_id: str) -> None:
    worker = threading.Thread(target=run_harvest_job, args=(job_id,), daemon=True, name=f"harvest-{job_id}")
    worker.start()


def update_harvest_candidate(candidate_id: str, updates: dict) -> dict:
    with HARVEST_LOCK:
        rows = _load_candidates()
        index = next((idx for idx, row in enumerate(rows) if str(row.get("id")) == str(candidate_id)), -1)
        if index < 0:
            raise FileNotFoundError("候选素材不存在")
        merged = dict(rows[index] or {})
        merged.update(updates or {})
        merged["updated_at"] = _now()
        normalized = _normalize_candidate(merged)
        rows[index] = normalized
        _save_candidates(rows)
    return normalized


def delete_harvest_candidate(candidate_id: str) -> dict:
    with HARVEST_LOCK:
        rows = _load_candidates()
        index = next((idx for idx, row in enumerate(rows) if str(row.get("id")) == str(candidate_id)), -1)
        if index < 0:
            raise FileNotFoundError("候选素材不存在")
        deleted = _normalize_candidate(rows[index])
        rows.pop(index)
        _save_candidates(rows)
    return deleted


def clear_harvest_candidates(*, keep_imported: bool = True) -> int:
    with HARVEST_LOCK:
        rows = _load_candidates()
        if keep_imported:
            kept = [row for row in rows if str((row or {}).get("status") or "") == "imported"]
        else:
            kept = []
        removed_count = max(0, len(rows) - len(kept))
        _save_candidates(kept)
    return removed_count


def import_harvest_candidate_to_material_library(
    candidate_id: str,
    *,
    uploader_username: str,
    uploader_display_name: str,
    category: str = "",
    notes: str = "",
) -> dict:
    candidates = list_harvest_candidates()
    candidate = next((row for row in candidates if str(row.get("id")) == str(candidate_id)), None)
    if not candidate:
        raise FileNotFoundError("候选素材不存在")
    asset_url = str(candidate.get("asset_url") or "").strip()
    if not asset_url:
        raise ValueError("候选素材没有可导入的资源链接")
    response = requests.get(asset_url, headers=DEFAULT_HEADERS, timeout=60, stream=True)
    response.raise_for_status()
    suffix = Path(urlparse(asset_url).path).suffix.lower()
    if suffix not in IMAGE_EXTENSIONS | VIDEO_EXTENSIONS:
        content_type = str(response.headers.get("content-type") or "").lower()
        if "png" in content_type:
            suffix = ".png"
        elif "webp" in content_type:
            suffix = ".webp"
        elif "jpeg" in content_type or "jpg" in content_type:
            suffix = ".jpg"
        elif "webm" in content_type:
            suffix = ".webm"
        elif "quicktime" in content_type:
            suffix = ".mov"
        elif "mp4" in content_type:
            suffix = ".mp4"
        else:
            suffix = ".jpg" if candidate.get("kind") == "image" else ".mp4"
    temp_root = HARVEST_DIR / "downloads"
    temp_root.mkdir(parents=True, exist_ok=True)
    temp_path = temp_root / f"{uuid.uuid4().hex[:12]}{suffix}"
    with temp_path.open("wb") as handle:
        for chunk in response.iter_content(chunk_size=1024 * 64):
            if chunk:
                handle.write(chunk)
    # External asset URLs are often redirect links or dynamic paths without a usable suffix.
    # For crawler imports, always persist with a normalized filename derived from the
    # downloaded asset so material_library can safely accept it.
    original_name = f"harvest_{candidate_id}{suffix}"
    item = register_material_file(
        temp_path=str(temp_path),
        original_filename=original_name or f"harvest_{candidate_id}{suffix}",
        title=candidate.get("page_title") or candidate.get("title") or f"候选素材 {candidate_id}",
        category=category or candidate.get("category") or "",
        tags=candidate.get("tags") or [],
        notes=notes or candidate.get("page_excerpt") or candidate.get("notes") or "",
        uploader_username=uploader_username,
        uploader_display_name=uploader_display_name,
        source="ai_harvest_import",
        source_url=candidate.get("source_url") or "",
        source_site=candidate.get("source_site") or candidate.get("domain") or "",
        license_note=candidate.get("license_note") or "",
        safety_status=candidate.get("safety_status") or "needs_review",
        news_topics=[candidate.get("topic") or "", candidate.get("category") or ""],
    )
    update_harvest_candidate(candidate_id, {"status": "imported", "imported_material_id": item.get("id", "")})
    return item


def run_harvest_job_and_import_pending(
    job_id: str,
    *,
    uploader_username: str,
    uploader_display_name: str,
    max_import: int = 12,
) -> dict:
    """Run a harvest job and copy downloaded candidates into material-library pending review."""
    job = run_harvest_job(job_id)
    if str(job.get("status") or "") != "done":
        return {"job": job, "imported_count": 0}
    imported_count = 0
    candidates = list_harvest_candidates(status="pending", job_id=job_id)
    for candidate in candidates:
        if imported_count >= max(0, int(max_import or 0)):
            break
        try:
            import_harvest_candidate_to_material_library(
                str(candidate.get("id") or ""),
                uploader_username=uploader_username,
                uploader_display_name=uploader_display_name,
                category=str(candidate.get("category") or job.get("category") or ""),
                notes=(
                    "热点补库候选，需管理员审核后才进入正式素材库。\n"
                    f"采集主题：{candidate.get('topic') or job.get('topic') or ''}\n"
                    f"来源：{candidate.get('source_url') or ''}"
                ),
            )
            imported_count += 1
        except Exception as exc:
            update_harvest_candidate(str(candidate.get("id") or ""), {"notes": f"自动导入 pending 失败：{exc}"})
    _update_job(job_id, {"message": f"{job.get('message') or '采集完成'}；已导入 {imported_count} 条到素材库待审核区"})
    return {"job": list_harvest_jobs()[0] if list_harvest_jobs() else job, "imported_count": imported_count}


def run_harvest_job_and_import_pending_async(
    job_id: str,
    *,
    uploader_username: str,
    uploader_display_name: str,
    max_import: int = 12,
) -> None:
    worker = threading.Thread(
        target=run_harvest_job_and_import_pending,
        kwargs={
            "job_id": job_id,
            "uploader_username": uploader_username,
            "uploader_display_name": uploader_display_name,
            "max_import": max_import,
        },
        daemon=True,
        name=f"hotspot-harvest-{job_id}",
    )
    worker.start()
