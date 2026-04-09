FROM node:24-slim

WORKDIR /app

# Install pnpm + python3 for llm_helper
RUN apt-get update && apt-get install -y python3 python3-pip --no-install-recommends && rm -rf /var/lib/apt/lists/* && \
    corepack enable && corepack prepare pnpm@latest --activate

# Copy dependency files first (cache layer)
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY runtime_package.json ./

# Install production dependencies (skip postinstall — stripped scripts)
RUN pnpm install --frozen-lockfile --prod --ignore-scripts || pnpm install --prod --ignore-scripts

# Copy OpenClaw core
COPY src/ ./src/
COPY packages/ ./packages/
COPY extensions/ ./extensions/
COPY skills/ ./skills/
COPY data/ ./data/
COPY openclaw.mjs ./

# Copy MyndLens REST API
COPY myndlens-api.mjs ./

# Copy MyndLens Channel & Tenant services (production files)
COPY myndlens_channel_adapter.js ./
COPY myndlens_channel_service.js ./
COPY tenant_worker.js ./
COPY tenant_ds_service.mjs ./
COPY tenant_llm.mjs ./
COPY tenant_prompts.mjs ./
COPY tenant_crypto.mjs ./
COPY tenant_vectorizer.mjs ./
COPY llm_helper.py ./

# Copy WhatsApp services
COPY wa_pair.mjs ./
COPY wa_session.mjs ./
COPY wa_ds_extractor.mjs ./
COPY wa_history_bootstrap.mjs ./

# Copy entrypoint
COPY entrypoint.sh ./
RUN chmod +x entrypoint.sh

# Create data directories
RUN mkdir -p /data/myndclaw/users /data/myndclaw/state

EXPOSE 18789 18790 18791 18792 10000-10999

ENV NODE_ENV=production
ENV OPENCLAW_STATE_DIR=/data/myndclaw/state
ENV MYNDLENS_API_PORT=18790

# Run both: MyndLens API (port 18790) + OpenClaw Gateway (port 18789)
CMD ["sh", "-c", "node myndlens-api.mjs & node openclaw.mjs gateway"]

