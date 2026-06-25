#!/usr/bin/env python3
"""
One-command builder for the OpenNews local material-image bank.

Default behavior now creates the review folder structure and official source
guide only. Automatic image crawling is intentionally opt-in because broad or
semi-curated crawling produced inaccurate news images in practice.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
PREPARE_SCRIPT = ROOT_DIR / "tools" / "prepare_opennews_material_library_folders.py"
DEFAULT_OUTPUT = Path.home() / "Desktop" / "OpenNews本地素材库建设"


def _run(command: list[str]) -> None:
    print("\n$ " + " ".join(command))
    subprocess.run(command, cwd=str(ROOT_DIR), check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Create OpenNews local material-library review folders.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output review folder. Default: Desktop/OpenNews本地素材库建设")
    parser.add_argument("--per-topic", type=int, default=12, help="Max images to download per topic.")
    parser.add_argument("--limit-topics", type=int, default=0, help="Only process first N topics.")
    parser.add_argument("--source-mode", choices=["curated", "mixed", "bing"], default="curated", help="curated is safest; mixed may find more but is noisier.")
    parser.add_argument("--fill-existing", action="store_true", help="Add images even if a topic folder already contains images.")
    parser.add_argument(
        "--download-candidates",
        action="store_true",
        help="Opt in to automatic candidate image crawling. Off by default because accuracy is not production-grade.",
    )
    args = parser.parse_args()

    output = Path(args.output).expanduser()
    template = output / "tag_template.csv"
    if not template.exists():
        prepare_cmd = [
            sys.executable,
            str(PREPARE_SCRIPT),
            "--output",
            str(output),
        ]
        if args.limit_topics:
            prepare_cmd.extend(["--limit", str(args.limit_topics)])
        _run(prepare_cmd)
    else:
        print(f"已存在素材库目录：{output}")
        print(f"已存在标签模板：{template}")

    if args.download_candidates:
        populate_script = ROOT_DIR / "tools" / "populate_opennews_material_library_folders.py"
        populate_cmd = [
            sys.executable,
            str(populate_script),
            str(output),
            "--per-topic",
            str(args.per_topic),
            "--source-mode",
            args.source_mode,
        ]
        if args.limit_topics:
            populate_cmd.extend(["--limit-topics", str(args.limit_topics)])
        if args.fill_existing:
            populate_cmd.append("--fill-existing")
        _run(populate_cmd)
    else:
        print("\n已关闭自动爬图。请按每个专题 README.md 和 official_sources.csv 手动挑选准确图片。")
        print("如果只是实验性抓候选图，才追加 --download-candidates。")

    print("\n完成。下一步：打开下面目录，按专题放入你确认准确的图片：")
    print(output)
    print("\n确认后导入素材库并同步 5090：")
    print(
        f'python3 tools/import_harvested_images_to_material_library.py "{output}" --sync-vector'
    )


if __name__ == "__main__":
    main()
