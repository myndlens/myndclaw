FROM node:24-slim

WORKDIR /app

# Install pnpm
RUN corepack enable && corepack prepare pnpm@latest --activate

# Copy dependency files first (cache layer)
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./

# Install production dependencies
RUN pnpm install --frozen-lockfile --prod || pnpm install --prod

# Copy application code
COPY src/ ./src/
COPY packages/ ./packages/
COPY extensions/ ./extensions/
COPY skills/ ./skills/
COPY data/ ./data/
COPY openclaw.mjs ./
COPY myndlens-api.mjs ./

# Create data directory
RUN mkdir -p /data/myndclaw/users

EXPOSE 18789 18790

ENV NODE_ENV=production
ENV OPENCLAW_STATE_DIR=/data/myndclaw/state
ENV MYNDLENS_API_PORT=18790

# Run both: MyndLens API (port 18790) + OpenClaw Gateway (port 18789)
CMD ["sh", "-c", "node myndlens-api.mjs & node openclaw.mjs gateway"]

