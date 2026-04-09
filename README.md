# MyndClaw

**Multi-tenant agent runtime for MyndLens standard users.**

Forked from [OpenClaw](https://github.com/openclaw/openclaw) (MIT License).

## What is MyndClaw?

MyndClaw is a stripped, hardened OpenClaw gateway purpose-built for the MyndLens platform. It serves as the agent execution runtime for standard tier users (65% of user base).

- **25 users per instance** (1 agent + 1 WhatsApp number per user)
- **Receives Claw.Spawn** from MyndLens BE mandate engine
- **Integrates with ObeGee** for auth, billing, tenant management
- **Horizontal scaling** — add CP VPS nodes as users grow

## Architecture



## What's Kept from OpenClaw
- Gateway core (message routing, WebSocket)
- Agent runtime (pi-agent-core loop)
- WhatsApp channel (Baileys + multi-account)
- Session manager + compaction
- Tool system + Skills system
- Queue + concurrency control

## What's Added for MyndLens
- Claw.Spawn receiver (mandate → agent execution)
- ObeGee auth bridge (JWT validation)
- Billing hooks (token metering)
- Auto-provisioning (new user → agent config)
- MyndLens default skills + tool restrictions

## License
MIT (inherited from OpenClaw)
