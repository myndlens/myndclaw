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

# Create data directory
RUN mkdir -p /data/myndclaw/users

EXPOSE 18789

ENV NODE_ENV=production
ENV OPENCLAW_STATE_DIR=/data/myndclaw/state

CMD ["node", "openclaw.mjs", "gateway"]
