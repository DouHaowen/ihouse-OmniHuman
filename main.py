"""
视频自动化生产系统 - 主程序
iHouse 内容生产流水线

使用方式：
  python main.py "为什么日本的房子是永久产权"
  python main.py "一家五口移居日本，签证怎么规划"
"""

import os
import sys
import json
import time
from pathlib import Path
from dotenv import load_dotenv

from generate_script import generate_script
from generate_audio import generate_all_audio
from generate_digital_human import generate_all_digital_human_videos
from fetch_materials import fetch_all_materials

load_dotenv(override=True)

# 数字人形象图片路径（你的主播图片）
DIGITAL_HUMAN_IMAGE = os.getenv(
    "DIGITAL_HUMAN_IMAGE",
    "./assets/anchor.jpg"  # 替换为你的主播图片路径
)

# MiniMax 配音音色 voice_id
TTS_VOICE = os.getenv("TTS_VOICE", "Chinese (Mandarin)_Warm_Bestie")


def run_pipeline(topic: str):
    """
    完整流水线：选题 → 文案 → 配音 → 数字人 → 素材
    """
    print("\n" + "="*60)
    print(f"🚀 开始生产视频：{topic}")
    print("="*60)
    
    # 创建输出目录
    timestamp = int(time.time())
    safe_topic = "".join(c for c in topic[:20] if c.isalnum() or c in "，。_")
    output_dir = f"./output/{timestamp}_{safe_topic}"
    os.makedirs(output_dir, exist_ok=True)
    print(f"📁 输出目录：{output_dir}")
    
    # ============================================================
    # 第一步：生成文案
    # ============================================================
    print("\n【第一步】生成文案...")
    script_data = generate_script(topic)
    
    # 保存文案JSON
    script_path = os.path.join(output_dir, "script.json")
    with open(script_path, "w", encoding="utf-8") as f:
        json.dump(script_data, f, ensure_ascii=False, indent=2)
    print(f"💾 文案已保存：{script_path}")
    
    # 保存可读版文案
    readable_path = os.path.join(output_dir, "script_readable.txt")
    _save_readable_script(script_data, readable_path)
    
    # 保存小红书/FB文案
    post_path = os.path.join(output_dir, "social_posts.txt")
    _save_social_posts(script_data, post_path)
    
    # ============================================================
    # 第二步：生成配音
    # ============================================================
    print("\n【第二步】生成配音...")
    segments_with_audio = generate_all_audio(
        segments=script_data["segments"],
        output_dir=output_dir,
        voice=TTS_VOICE
    )
    
    # ============================================================
    # 第三步：生成数字人视频
    # ============================================================
    print("\n【第三步】生成数字人视频...")
    
    if not os.path.exists(DIGITAL_HUMAN_IMAGE):
        print(f"⚠️ 数字人图片不存在：{DIGITAL_HUMAN_IMAGE}")
        print("   请在 .env 中设置 DIGITAL_HUMAN_IMAGE 路径")
        segments_with_dh = segments_with_audio
    else:
        segments_with_dh = generate_all_digital_human_videos(
            segments=segments_with_audio,
            image_path=DIGITAL_HUMAN_IMAGE,
            output_dir=output_dir
        )
    
    # ============================================================
    # 第四步：搜索素材
    # ============================================================
    print("\n【第四步】搜索素材...")
    final_segments = fetch_all_materials(
        segments=segments_with_dh,
        output_dir=output_dir
    )
    
    # ============================================================
    # 完成，输出摘要
    # ============================================================
    _print_summary(script_data, final_segments, output_dir)
    
    # 保存最终结果
    result_path = os.path.join(output_dir, "result.json")
    with open(result_path, "w", encoding="utf-8") as f:
        json.dump({
            "topic": topic,
            "script": script_data,
            "segments": final_segments,
        }, f, ensure_ascii=False, indent=2, default=str)
    
    print(f"\n🎉 生产完成！所有文件在：{output_dir}")
    return output_dir


def _save_readable_script(script_data: dict, output_path: str):
    """保存可读版文案（方便人工检查）"""
    lines = []
    lines.append(f"标题：{script_data.get('title', '')}")
    lines.append(f"封面：{script_data.get('cover_title', '')}")
    lines.append(f"总时长：{script_data.get('total_duration', 0)}秒")
    lines.append("\n" + "="*50)
    lines.append("【播报稿+时间轴】")
    lines.append("="*50)
    
    for i, seg in enumerate(script_data.get("segments", [])):
        seg_type = "数字人" if seg["type"] == "digital_human" else "素材"
        lines.append(f"\n【{seg_type} | {seg['start']}s~{seg['end']}s】")
        lines.append(seg.get("script", ""))
        
        if seg["type"] == "digital_human":
            lines.append(f"动作描述：{seg.get('action', '')}")
        else:
            lines.append(f"素材关键词：{seg.get('material_keyword', '')}")
            lines.append(f"素材说明：{seg.get('material_desc', '')}")
    
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    
    print(f"💾 可读文案已保存：{output_path}")


def _save_social_posts(script_data: dict, output_path: str):
    """保存社交媒体发布文案"""
    lines = []
    lines.append("="*50)
    lines.append("【小红书发布文案】")
    lines.append("="*50)
    lines.append(script_data.get("xiaohongshu_post", ""))
    lines.append("\n")
    lines.append("="*50)
    lines.append("【Facebook发布文案（繁体中文）】")
    lines.append("="*50)
    lines.append(script_data.get("facebook_post", ""))
    
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    
    print(f"💾 社交文案已保存：{output_path}")


def _print_summary(script_data: dict, final_segments: list, output_dir: str):
    """打印生产摘要"""
    print("\n" + "="*60)
    print("📊 生产摘要")
    print("="*60)
    
    dh_count = sum(1 for s in final_segments if s.get("type") == "digital_human")
    mat_count = sum(1 for s in final_segments if s.get("type") == "material")
    dh_done = sum(1 for s in final_segments if s.get("video_path"))
    mat_done = sum(1 for s in final_segments if s.get("material_paths"))
    
    print(f"  视频标题：{script_data.get('title', '')}")
    print(f"  封面标题：{script_data.get('cover_title', '')}")
    print(f"  总时长：{script_data.get('total_duration', 0)}秒")
    print(f"  数字人段落：{dh_done}/{dh_count} 个")
    print(f"  素材段落：{mat_done}/{mat_count} 个已下载素材")
    print(f"\n  输出目录结构：")
    print(f"  {output_dir}/")
    print(f"  ├── script_readable.txt   ← 完整文案+时间轴")
    print(f"  ├── social_posts.txt      ← 小红书+FB文案")
    print(f"  ├── script.json           ← 原始数据")
    print(f"  ├── audio/                ← 所有配音音频")
    print(f"  ├── digital_human/        ← 数字人视频片段")
    print(f"  └── materials/            ← 素材图片")
    print(f"\n  👉 将 digital_human/ 和 materials/ 导入剪映剪辑即可")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("使用方式：python main.py '选题内容'")
        print("示例：python main.py '为什么日本的房子是永久产权'")
        sys.exit(1)
    
    topic = sys.argv[1]
    run_pipeline(topic)
