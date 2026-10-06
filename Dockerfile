FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=Asia/Shanghai

WORKDIR /app

# 依赖里带 tzdata（纯 Python 时区库），无需 apt 安装系统包
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .

RUN mkdir -p /app/data

EXPOSE 7701

HEALTHCHECK --interval=30s --timeout=6s --start-period=20s --retries=3 \
    CMD ["python", "-c", "import os,sys,urllib.request; port=os.environ.get('APP_PORT','7701'); sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:'+port+'/healthz', timeout=5).status == 200 else 1)"]

CMD ["sh", "-c", "python -m uvicorn main:app --host ${APP_HOST:-0.0.0.0} --port ${APP_PORT:-7701} --no-access-log"]
