"""
OmniHuman 数字人生成模块
输入：图片 URL + 音频 URL
输出：数字人视频
调用火山引擎即梦AI OmniHuman 1.5 API
"""

import os
import time

import requests
from dotenv import load_dotenv

load_dotenv(override=False)

REQ_KEY = "jimeng_realman_avatar_picture_omni_v15"


def _get_visual_service():
    from volcengine.visual.VisualService import VisualService

    visual_service = VisualService()
    visual_service.set_ak(os.getenv("VOLC_ACCESS_KEY"))
    visual_service.set_sk(os.getenv("VOLC_SECRET_KEY"))
    visual_service.service_info.scheme = "https"
    visual_service.service_info.connection_timeout = int(os.getenv("VOLC_CONNECT_TIMEOUT", "30"))
    visual_service.service_info.socket_timeout = int(os.getenv("VOLC_SOCKET_TIMEOUT", "180"))
    return visual_service


def _download_file(url: str, output_path: str):
    """下载文件到本地"""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    response = requests.get(url, stream=True, timeout=300)
    response.raise_for_status()

    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)


def submit_video_task(
    image_url: str,
    audio_url: str,
    prompt: str = "",
    mask_urls: list[str] | None = None,
    output_resolution: int = 1080,
    pe_fast_mode: bool | None = None,
    seed: int = -1,
) -> str:
    """提交 OmniHuman1.5 任务并返回 task_id"""
    print("🎬 提交 OmniHuman1.5 视频任务中...")

    if pe_fast_mode is None:
        pe_fast_mode = output_resolution == 720

    payload = {
        "req_key": REQ_KEY,
        "image_url": image_url,
        "audio_url": audio_url,
        "seed": seed,
        "output_resolution": output_resolution,
        "pe_fast_mode": pe_fast_mode,
    }
    if prompt:
        payload["prompt"] = prompt
    if mask_urls:
        payload["mask_url"] = mask_urls

    response = _get_visual_service().cv_submit_task(payload)
    code = response.get("code")
    if code != 10000:
        raise Exception(
            f"提交任务失败: code={code}, message={response.get('message')}, request_id={response.get('request_id')}"
        )

    task_id = response.get("data", {}).get("task_id")
    if not task_id:
        raise Exception(f"提交任务成功但未返回 task_id: {response}")

    print(f"📋 任务已提交，task_id: {task_id}")
    return task_id


def _is_retryable_submit_error(exc: Exception) -> bool:
    message = str(exc)
    return "504 Gateway Time-out" in message or "TLB" in message


def poll_task_result(task_id: str, max_wait: int = 600) -> str:
    """轮询任务状态，等待完成并返回视频 URL"""
    start_time = time.time()
    visual_service = _get_visual_service()

    while time.time() - start_time < max_wait:
        result = visual_service.cv_get_result(
            {
                "req_key": REQ_KEY,
                "task_id": task_id,
            }
        )

        code = result.get("code")
        if code != 10000:
            raise Exception(
                f"查询任务失败: code={code}, message={result.get('message')}, request_id={result.get('request_id')}"
            )

        data = result.get("data", {})
        status = data.get("status")

        if status == "done":
            video_url = data.get("video_url")
            if video_url:
                return video_url
            raise Exception(f"任务完成但未找到视频 URL: {result}")

        if status in ("expired", "not_found"):
            raise Exception(f"任务状态异常: status={status}, response={result}")

        print(f"⏳ 等待生成中... (状态: {status})")
        time.sleep(5)

    raise Exception(f"任务超时（等待超过 {max_wait} 秒）")


def generate_digital_human_video(
    image_url: str,
    audio_url: str,
    output_path: str,
    prompt: str = "",
    mask_urls: list[str] | None = None,
    output_resolution: int = 1080,
    pe_fast_mode: bool | None = None,
    seed: int = -1,
) -> str:
    """
    完整流程：图片 URL + 音频 URL -> 数字人视频
    """
    print("\n🤖 开始生成数字人视频")
    print(f"   图片URL：{image_url}")
    print(f"   音频URL：{audio_url}")

    retries = int(os.getenv("OMNIHUMAN_SUBMIT_RETRIES", "2"))
    last_error = None

    for attempt in range(1, retries + 2):
        try:
            task_id = submit_video_task(
                image_url=image_url,
                audio_url=audio_url,
                prompt=prompt,
                mask_urls=mask_urls,
                output_resolution=output_resolution,
                pe_fast_mode=pe_fast_mode,
                seed=seed,
            )
            break
        except Exception as exc:
            last_error = exc
            if not _is_retryable_submit_error(exc) or attempt > retries:
                raise
            wait_seconds = attempt * 3
            print(f"⚠️ 提交任务超时，{wait_seconds} 秒后重试 ({attempt}/{retries})...")
            time.sleep(wait_seconds)
    else:
        raise last_error

    video_url = poll_task_result(task_id)
    _download_file(video_url, output_path)

    print(f"✅ 数字人视频已保存：{output_path}")
    return output_path


def generate_all_digital_human_videos(
    segments: list,
    image_url: str,
    output_dir: str,
    output_resolution: int = 1080,
) -> list:
    """
    批量生成所有数字人段落的视频
    """
    results = []

    for i, seg in enumerate(segments):
        if seg.get("type") != "digital_human":
            results.append(seg)
            continue

        audio_url = seg.get("audio_url")
        if not audio_url:
            print(f"⚠️ 段落 {i} 缺少音频 URL，跳过")
            results.append(seg)
            continue

        output_path = os.path.join(output_dir, "digital_human", f"dh_{i:02d}.mp4")
        os.makedirs(os.path.join(output_dir, "digital_human"), exist_ok=True)

        try:
            video_path = generate_digital_human_video(
                image_url=image_url,
                audio_url=audio_url,
                output_path=output_path,
                prompt=seg.get("action", ""),
                output_resolution=output_resolution,
            )
            seg_with_video = seg.copy()
            seg_with_video["video_path"] = video_path
            results.append(seg_with_video)
        except Exception as e:
            print(f"❌ 段落 {i} 生成失败：{e}")
            results.append(seg)

    dh_count = sum(1 for s in results if s.get("video_path"))
    print(f"\n✅ 数字人视频生成完成，共 {dh_count} 个")
    return results
