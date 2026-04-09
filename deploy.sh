#!/bin/bash
# MyndClaw Deploy Script — Run on CP VPS
set -e

MYNDCLAW_DIR="/opt/myndclaw"
DATA_DIR="/data/myndclaw"
COMPOSE_FILE="docker-compose.myndclaw.yml"

echo "=== MyndClaw Deploy ==="

# 1. Pull latest code
cd $MYNDCLAW_DIR
git pull origin main

# 2. Ensure data directory exists
mkdir -p $DATA_DIR/state $DATA_DIR/users

# 3. Build and start
docker compose -f $COMPOSE_FILE build --no-cache
docker compose -f $COMPOSE_FILE up -d

# 4. Wait for health
echo "Waiting for MyndClaw health..."
for i in {1..30}; do
    if curl -sf http://localhost:18789 > /dev/null 2>&1; then
        echo "MyndClaw is healthy!"
        docker compose -f $COMPOSE_FILE logs --tail=5
        exit 0
    fi
    sleep 2
done

echo "WARNING: MyndClaw did not become healthy in 60s"
docker compose -f $COMPOSE_FILE logs --tail=20
exit 1
