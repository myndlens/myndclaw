# Use the pre-built OpenClaw image as base (has gateway build output + all production files)
FROM ghcr.io/myndlens/openclaw:ds-thinpod-latest

# Add MyndLens REST API server (claw-spawn + health on port 18790)
COPY myndlens-api.mjs /app/myndlens-api.mjs

# Update entrypoint to also start the MyndLens REST API
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

EXPOSE 18790

ENV MYNDLENS_API_PORT=18790

