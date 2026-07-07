import gc
import os
import sys
import uuid
from pathlib import Path
from threading import Lock
from typing import Optional

import soundfile as sf
import torch
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from qwen_tts import Qwen3TTSModel


BASE_DIR = Path("/home/saita/qwen3-tts-service")
MODEL_DIR = BASE_DIR / "models" / "Qwen3-TTS-12Hz-1.7B-CustomVoice"
OUTPUT_DIR = BASE_DIR / "outputs"
API_TOKEN = os.getenv("QWEN3_TTS_API_TOKEN", "local-qwen3-tts-5090")
MAX_REQUESTS_BEFORE_RECYCLE = int(os.getenv("QWEN3_TTS_MAX_REQUESTS_BEFORE_RECYCLE", "36") or "36")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="iHouse Qwen3 TTS Test Service")
_model = None
_model_lock = Lock()
_generate_lock = Lock()
_request_count = 0
_request_count_lock = Lock()


class TTSRequest(BaseModel):
    text: str
    language: str = "Chinese"
    speaker: str = "Dylan"
    instruct: Optional[str] = "用自然、清晰、专业的新闻播报语气朗读。"
    max_new_tokens: Optional[int] = None


def _auth(x_token: Optional[str]) -> None:
    if x_token != API_TOKEN:
        raise HTTPException(status_code=401, detail="invalid token")


def _fatal_cuda_error(exc: Exception | str) -> bool:
    text = str(exc).lower()
    return any(
        token in text
        for token in (
            "cuda",
            "accelerator",
            "cublas",
            "cudnn",
            "device-side",
            "unspecified launch failure",
            "illegal memory access",
        )
    )


def _cuda_sync_or_exit(stage: str) -> None:
    if not torch.cuda.is_available():
        return
    try:
        torch.cuda.synchronize()
    except Exception as exc:
        print(f"[qwen3-tts] CUDA {stage} failed; exiting to reset CUDA context: {exc}", file=sys.stderr, flush=True)
        os._exit(70)


def _post_request_cleanup() -> None:
    try:
        gc.collect()
    except Exception:
        pass
    if not torch.cuda.is_available():
        return
    try:
        torch.cuda.empty_cache()
    except Exception:
        pass
    try:
        torch.cuda.ipc_collect()
    except Exception:
        pass


def _current_request_count() -> int:
    with _request_count_lock:
        return int(_request_count)


def _increment_request_count() -> int:
    global _request_count
    with _request_count_lock:
        _request_count += 1
        return int(_request_count)


def _maybe_recycle_before_request() -> None:
    if MAX_REQUESTS_BEFORE_RECYCLE <= 0:
        return
    served = _current_request_count()
    if served >= MAX_REQUESTS_BEFORE_RECYCLE:
        print(
            f"[qwen3-tts] request budget reached ({served}/{MAX_REQUESTS_BEFORE_RECYCLE}); exiting for clean restart",
            file=sys.stderr,
            flush=True,
        )
        os._exit(75)


def _load_model() -> Qwen3TTSModel:
    global _model
    if _model is None:
        with _model_lock:
            if _model is None:
                if not MODEL_DIR.exists():
                    raise RuntimeError(f"model not found: {MODEL_DIR}")
                kwargs = {
                    "device_map": "cuda:0" if torch.cuda.is_available() else "cpu",
                    "dtype": torch.bfloat16 if torch.cuda.is_available() else torch.float32,
                }
                try:
                    kwargs["attn_implementation"] = "flash_attention_2"
                    _model = Qwen3TTSModel.from_pretrained(str(MODEL_DIR), **kwargs)
                except Exception:
                    kwargs.pop("attn_implementation", None)
                    _model = Qwen3TTSModel.from_pretrained(str(MODEL_DIR), **kwargs)
    return _model


@app.get("/health")
def health(x_token: Optional[str] = Header(None)):
    _auth(x_token)
    if _model is not None and torch.cuda.is_available():
        _cuda_sync_or_exit("health")
    return {
        "ok": True,
        "service": "qwen3-tts-service",
        "model_dir": str(MODEL_DIR),
        "model_exists": MODEL_DIR.exists(),
        "loaded": _model is not None,
        "cuda": torch.cuda.is_available(),
        "gpu": torch.cuda.get_device_name(0) if torch.cuda.is_available() else None,
        "requests_served": _current_request_count(),
        "max_requests_before_recycle": MAX_REQUESTS_BEFORE_RECYCLE,
    }


@app.get("/voices")
def voices(x_token: Optional[str] = Header(None)):
    _auth(x_token)
    model = _load_model()
    return {
        "ok": True,
        "speakers": model.get_supported_speakers(),
        "languages": model.get_supported_languages(),
    }


@app.post("/tts")
def synthesize(req: TTSRequest, x_token: Optional[str] = Header(None)):
    _auth(x_token)
    text = (req.text or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="text is required")
    _maybe_recycle_before_request()
    with _generate_lock:
        _cuda_sync_or_exit("preflight")
        model = _load_model()
        generate_kwargs = {}
        if req.max_new_tokens:
            generate_kwargs["max_new_tokens"] = req.max_new_tokens
        try:
            with torch.inference_mode():
                wavs, sample_rate = model.generate_custom_voice(
                    text=text,
                    language=req.language,
                    speaker=req.speaker,
                    instruct=req.instruct or "",
                    **generate_kwargs,
                )
            _cuda_sync_or_exit("post-generate")
        except Exception as exc:
            _post_request_cleanup()
            if _fatal_cuda_error(exc):
                print(f"[qwen3-tts] fatal CUDA error; exiting to reset CUDA context: {exc}", file=sys.stderr, flush=True)
                os._exit(70)
            raise
    if not wavs:
        raise RuntimeError("model returned empty audio")
    name = f"qwen3_tts_{uuid.uuid4().hex}.wav"
    path = OUTPUT_DIR / name
    sf.write(str(path), wavs[0], sample_rate)
    served = _increment_request_count()
    _post_request_cleanup()
    return {
        "ok": True,
        "file": name,
        "url": f"/files/{name}",
        "sample_rate": sample_rate,
        "speaker": req.speaker,
        "language": req.language,
        "requests_served": served,
        "max_requests_before_recycle": MAX_REQUESTS_BEFORE_RECYCLE,
    }


@app.get("/files/{name}")
def file(name: str, x_token: Optional[str] = Header(None)):
    _auth(x_token)
    path = OUTPUT_DIR / Path(name).name
    if not path.exists():
        raise HTTPException(status_code=404, detail="file not found")
    return FileResponse(str(path), media_type="audio/wav", filename=path.name)
