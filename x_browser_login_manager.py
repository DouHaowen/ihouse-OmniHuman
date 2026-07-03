import json
import os
import shutil
import signal
import socket
import subprocess
import time
from pathlib import Path
from typing import Any

from x_browser_publisher import X_BROWSER_STATE_DIR, x_browser_profile_dir


X_BROWSER_LOGIN_RUNTIME_DIR = Path(
    os.getenv("X_BROWSER_LOGIN_RUNTIME_DIR", str(X_BROWSER_STATE_DIR / "login_runtime"))
).resolve()
X_BROWSER_LOGIN_DISPLAY = (os.getenv("X_BROWSER_LOGIN_DISPLAY", ":99") or ":99").strip()
X_BROWSER_LOGIN_SCREEN = (os.getenv("X_BROWSER_LOGIN_SCREEN", "1365x900x24") or "1365x900x24").strip()
X_BROWSER_LOGIN_NOVNC_PORT = max(1024, int(os.getenv("X_BROWSER_LOGIN_NOVNC_PORT", "6090") or "6090"))
X_BROWSER_LOGIN_VNC_PORT = max(1024, int(os.getenv("X_BROWSER_LOGIN_VNC_PORT", "5901") or "5901"))
X_BROWSER_LOGIN_PUBLIC_BASE_URL = (os.getenv("X_BROWSER_LOGIN_PUBLIC_BASE_URL", "") or "").strip()
X_BROWSER_LOGIN_START_URL = (
    os.getenv("X_BROWSER_LOGIN_START_URL", "https://x.com/compose/post") or "https://x.com/compose/post"
).strip()
X_BROWSER_LOGIN_CHROMIUM_PATH = (os.getenv("X_BROWSER_LOGIN_CHROMIUM_PATH", "/usr/bin/chromium") or "/usr/bin/chromium").strip()
X_BROWSER_LOGIN_NOVNC_WEB_DIR = (os.getenv("X_BROWSER_LOGIN_NOVNC_WEB_DIR", "/usr/share/novnc") or "/usr/share/novnc").strip()


class XBrowserLoginError(RuntimeError):
    pass


def _pid_file(name: str) -> Path:
    return X_BROWSER_LOGIN_RUNTIME_DIR / f"{name}.pid"


def _log_file(name: str) -> Path:
    return X_BROWSER_LOGIN_RUNTIME_DIR / f"{name}.log"


def _load_pid(name: str) -> int:
    path = _pid_file(name)
    if not path.exists():
        return 0
    try:
        return int(path.read_text("utf-8").strip() or "0")
    except Exception:
        return 0


def _save_pid(name: str, pid: int) -> None:
    _pid_file(name).write_text(str(pid), encoding="utf-8")


def _process_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _terminate_pid(pid: int) -> None:
    if pid <= 0:
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        return
    deadline = time.time() + 5
    while time.time() < deadline:
        if not _process_alive(pid):
            return
        time.sleep(0.2)
    try:
        os.kill(pid, signal.SIGKILL)
    except OSError:
        pass


def _resolve_cmd(name: str) -> str:
    if name == "chromium":
        path = Path(X_BROWSER_LOGIN_CHROMIUM_PATH)
        if path.is_file():
            return str(path)
    resolved = shutil.which(name)
    if resolved:
        return resolved
    raise XBrowserLoginError(f"服务器缺少 {name}，需要重建镜像后才能启用服务器端 X 登录。")


def _public_login_url() -> str:
    if X_BROWSER_LOGIN_PUBLIC_BASE_URL:
        return f"{X_BROWSER_LOGIN_PUBLIC_BASE_URL.rstrip('/')}/vnc.html?autoconnect=1&resize=remote"
    hostname = socket.gethostname()
    return f"http://{hostname}:{X_BROWSER_LOGIN_NOVNC_PORT}/vnc.html?autoconnect=1&resize=remote"


def _tail_log(name: str, limit: int = 40) -> str:
    path = _log_file(name)
    if not path.exists():
        return ""
    try:
        lines = path.read_text("utf-8", errors="ignore").splitlines()
    except Exception:
        return ""
    return "\n".join(lines[-limit:])


def x_browser_login_status() -> dict[str, Any]:
    X_BROWSER_LOGIN_RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    processes = {}
    running = False
    for name in ("xvfb", "openbox", "x11vnc", "websockify", "chromium"):
        pid = _load_pid(name)
        alive = _process_alive(pid)
        processes[name] = {"pid": pid, "alive": alive, "log": str(_log_file(name))}
        running = running or alive
    return {
        "running": running,
        "display": X_BROWSER_LOGIN_DISPLAY,
        "screen": X_BROWSER_LOGIN_SCREEN,
        "novnc_port": X_BROWSER_LOGIN_NOVNC_PORT,
        "vnc_port": X_BROWSER_LOGIN_VNC_PORT,
        "public_url": _public_login_url(),
        "profile_dir": str(x_browser_profile_dir()),
        "runtime_dir": str(X_BROWSER_LOGIN_RUNTIME_DIR),
        "processes": processes,
        "recent_log": _tail_log("chromium") or _tail_log("websockify") or _tail_log("x11vnc"),
    }


def stop_x_browser_login() -> dict[str, Any]:
    X_BROWSER_LOGIN_RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    for name in ("chromium", "websockify", "x11vnc", "openbox", "xvfb"):
        _terminate_pid(_load_pid(name))
        try:
            _pid_file(name).unlink()
        except Exception:
            pass
    return x_browser_login_status()


def _spawn(name: str, argv: list[str], extra_env: dict[str, str] | None = None) -> int:
    X_BROWSER_LOGIN_RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    log_handle = open(_log_file(name), "ab")
    proc = subprocess.Popen(
        argv,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
        env=env,
    )
    _save_pid(name, proc.pid)
    return proc.pid


def start_x_browser_login() -> dict[str, Any]:
    stop_x_browser_login()
    profile_dir = x_browser_profile_dir()
    profile_dir.mkdir(parents=True, exist_ok=True)

    xvfb = _resolve_cmd("Xvfb")
    x11vnc = _resolve_cmd("x11vnc")
    websockify = _resolve_cmd("websockify")
    chromium = _resolve_cmd("chromium")
    openbox = shutil.which("openbox") or shutil.which("fluxbox")

    _spawn(
        "xvfb",
        [
            xvfb,
            X_BROWSER_LOGIN_DISPLAY,
            "-screen",
            "0",
            X_BROWSER_LOGIN_SCREEN,
            "-ac",
            "+extension",
            "RANDR",
        ],
    )
    time.sleep(1.2)
    if openbox:
        _spawn("openbox", [openbox], extra_env={"DISPLAY": X_BROWSER_LOGIN_DISPLAY})
        time.sleep(0.5)
    _spawn(
        "x11vnc",
        [
            x11vnc,
            "-display",
            X_BROWSER_LOGIN_DISPLAY,
            "-rfbport",
            str(X_BROWSER_LOGIN_VNC_PORT),
            "-forever",
            "-shared",
            "-nopw",
        ],
    )
    time.sleep(0.5)
    novnc_web = Path(X_BROWSER_LOGIN_NOVNC_WEB_DIR)
    if not novnc_web.exists():
        raise XBrowserLoginError(f"服务器缺少 noVNC 静态目录：{novnc_web}")
    _spawn(
        "websockify",
        [
            websockify,
            "--web",
            str(novnc_web),
            str(X_BROWSER_LOGIN_NOVNC_PORT),
            f"127.0.0.1:{X_BROWSER_LOGIN_VNC_PORT}",
        ],
    )
    time.sleep(0.5)
    _spawn(
        "chromium",
        [
            chromium,
            f"--user-data-dir={profile_dir}",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-dev-shm-usage",
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--window-size=1365,900",
            X_BROWSER_LOGIN_START_URL,
        ],
        extra_env={"DISPLAY": X_BROWSER_LOGIN_DISPLAY},
    )
    time.sleep(1.5)
    status = x_browser_login_status()
    if not status.get("running"):
        raise XBrowserLoginError("服务器端 X 登录会话启动失败。")
    return status


def x_browser_login_env_config() -> dict[str, Any]:
    return {
        "display": X_BROWSER_LOGIN_DISPLAY,
        "screen": X_BROWSER_LOGIN_SCREEN,
        "novnc_port": X_BROWSER_LOGIN_NOVNC_PORT,
        "vnc_port": X_BROWSER_LOGIN_VNC_PORT,
        "public_url": _public_login_url(),
        "novnc_web_dir": X_BROWSER_LOGIN_NOVNC_WEB_DIR,
        "chromium_path": X_BROWSER_LOGIN_CHROMIUM_PATH,
    }
