# EDGE container - the dry-run executor for a free PaaS box (Render / Koyeb, Frankfurt).
# Build context = machine/edge/render (the deploy script copies the copybot modules in first).
FROM python:3.13-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
RUN apt-get update -y && apt-get install -y --no-install-recommends curl ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY copybot/ /app/copybot/
COPY edge_web.py edge_apply_seed.py /app/
ENV HOME=/tmp COPYBOT_DB=/tmp/lab/copybot.db COPYBOT_HIST=/tmp/lab/history.db WS_URL=wss://solana-rpc.publicnode.com \
    SEED_URL=https://raw.githubusercontent.com/bschriver92-blip/wallet-lab-seed/main/seed.db PORT=10000
EXPOSE 10000
CMD ["python", "/app/edge_web.py"]
