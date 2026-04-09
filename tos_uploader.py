"""
TOS 上传工具
将本地文件上传到火山 TOS，并生成短时可访问链接
"""

import mimetypes
import os
import time
import uuid
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Callable

import tos
from dotenv import load_dotenv
from PIL import Image

load_dotenv(override=False)


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def _get_client() -> tos.TosClientV2:
    ak = _env("TOS_ACCESS_KEY", _env("VOLC_ACCESS_KEY"))
    sk = _env("TOS_SECRET_KEY", _env("VOLC_SECRET_KEY"))
    endpoint = _env("TOS_ENDPOINT", "tos-cn-beijing.volces.com")
    region = _env("TOS_REGION", "cn-beijing")

    if not ak or not sk:
        raise RuntimeError("缺少 TOS_ACCESS_KEY / TOS_SECRET_KEY（或 VOLC_ACCESS_KEY / VOLC_SECRET_KEY）")

    return tos.TosClientV2(
        ak=ak,
        sk=sk,
        endpoint=endpoint,
        region=region,
        connection_time=int(_env("TOS_CONNECTION_TIMEOUT", "30")),
        socket_timeout=int(_env("TOS_SOCKET_TIMEOUT", "180")),
        request_timeout=int(_env("TOS_REQUEST_TIMEOUT", "180")),
        max_retry_count=int(_env("TOS_MAX_RETRY_COUNT", "3")),
    )


def _get_bucket() -> str:
    bucket = _env("TOS_BUCKET", "ark-auto-2108199052-cn-beijing-default")
    if not bucket:
        raise RuntimeError("缺少 TOS_BUCKET 配置")
    return bucket


def _is_retryable_tos_error(exc: Exception) -> bool:
    text = str(exc).lower()
    retry_tokens = [
        "timeout",
        "timed out",
        "connection aborted",
        "connection reset",
        "temporarily unavailable",
        "503",
        "504",
        "500",
        "slow down",
        "broken pipe",
    ]
    return any(token in text for token in retry_tokens)


def _prepare_upload_file(local_path: str) -> tuple[str, Callable | None]:
    file_path = Path(local_path)
    suffix = file_path.suffix.lower()

    if suffix not in {".jpg", ".jpeg", ".png", ".webp"}:
        return local_path, None

    image = Image.open(file_path)
    image = image.convert("RGB")

    max_side = int(_env("TOS_IMAGE_MAX_SIDE", "1600"))
    image.thumbnail((max_side, max_side))

    temp = NamedTemporaryFile(suffix=".jpg", delete=False)
    temp_path = temp.name
    temp.close()
    image.save(
        temp_path,
        format="JPEG",
        quality=int(_env("TOS_IMAGE_QUALITY", "82")),
        optimize=True,
    )

    def _cleanup():
        try:
            os.unlink(temp_path)
        except FileNotFoundError:
            pass

    return temp_path, _cleanup


def upload_file_and_get_url(local_path: str, key_prefix: str = "avatar-tests", expires: int = 3600) -> str:
    prepared_path, cleanup = _prepare_upload_file(local_path)
    file_path = Path(prepared_path)
    if not file_path.exists():
        raise FileNotFoundError(f"文件不存在: {prepared_path}")

    suffix = file_path.suffix.lower()
    object_key = f"{key_prefix}/{time.strftime('%Y%m%d')}/{uuid.uuid4().hex}{suffix}"
    content_type = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"

    bucket = _get_bucket()
    attempts = max(1, int(_env("TOS_UPLOAD_ATTEMPTS", "5") or 5))
    base_delay = max(1.0, float(_env("TOS_UPLOAD_RETRY_BASE_DELAY_SECONDS", "3") or 3))
    try:
        last_error = None
        for attempt in range(1, attempts + 1):
            client = _get_client()
            try:
                with open(file_path, "rb") as f:
                    client.put_object(
                        bucket=bucket,
                        key=object_key,
                        content=f,
                        content_type=content_type,
                    )

                signed = client.pre_signed_url(
                    http_method=tos.HttpMethodType.Http_Method_Get,
                    bucket=bucket,
                    key=object_key,
                    expires=expires,
                )
                return signed.signed_url
            except Exception as exc:
                last_error = exc
                if attempt >= attempts or not _is_retryable_tos_error(exc):
                    raise
                wait_seconds = base_delay * attempt
                print(f"⚠️ TOS 上传超时，{wait_seconds:.0f} 秒后重试 ({attempt}/{attempts})：{file_path.name}")
                time.sleep(wait_seconds)
        if last_error:
            raise last_error
        raise RuntimeError("TOS 上传失败")
    finally:
        if cleanup:
            cleanup()
