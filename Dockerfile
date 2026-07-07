FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    APP_HOME=/app \
    PORT=3010

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        ffmpeg \
        fonts-noto-cjk \
        chromium \
        chromium-driver \
        openbox \
        novnc \
        procps \
        websockify \
        x11vnc \
        xauth \
        xvfb \
        libasound2 \
        libatk-bridge2.0-0 \
        libatk1.0-0 \
        libc6 \
        libcairo2 \
        libcups2 \
        libdbus-1-3 \
        libdrm2 \
        libexpat1 \
        libfontconfig1 \
        libgbm1 \
        libgcc1 \
        libglib2.0-0 \
        libgtk-3-0 \
        libnspr4 \
        libnss3 \
        libpango-1.0-0 \
        libpangocairo-1.0-0 \
        libstdc++6 \
        libx11-6 \
        libx11-xcb1 \
        libxcb1 \
        libxcomposite1 \
        libxdamage1 \
        libxext6 \
        libxfixes3 \
        libxkbcommon0 \
        libxrandr2 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# 安装 Playwright 自带且版本匹配的 chromium，供 X 浏览器自动发布使用。
# 不依赖系统 apt 的 chromium —— 系统版本常与 Playwright 协议不兼容导致启动崩溃。
RUN python -m playwright install chromium

COPY . .

RUN mkdir -p /app/output /app/assets

EXPOSE 3010

CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT:-3010}"]
