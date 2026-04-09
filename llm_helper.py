#!/usr/bin/env /root/.venv/bin/python3
"""
ObeGee LLM Helper for Tenant Runtime

This script provides a bridge between the Node.js tenant worker and the 
Emergent LLM integrations library. It's called as a subprocess by the
tenant_worker.js to handle LLM calls.

Provider selection:
- ObeGee-managed: Uses Emergent LLM Key (DeepSeek default via OpenAI-compatible API)
- BYOK: Uses tenant's own OpenAI/Anthropic key

The key is passed via EMERGENT_LLM_KEY environment variable.
Control Plane NEVER calls this - only Runtime does.

Usage:
    /root/.venv/bin/python3 llm_helper.py --message "Hello" --session-id "session123" \
        [--provider openai] [--model gpt-5.2] [--history '[]']
"""

import argparse
import asyncio
import json
import sys
import os

sys.path.insert(0, '/app/backend')

async def call_llm(message: str, session_id: str, provider: str, model: str, history: list = None) -> dict:
    """Call LLM and return response"""
    try:
        from emergentintegrations.llm.chat import LlmChat, UserMessage
        
        api_key = os.environ.get('EMERGENT_LLM_KEY', '')
        if not api_key:
            return {"success": False, "error": "LLM API key not provided", "content": ""}
        
        is_byok = os.environ.get('IS_BYOK', 'false').lower() == 'true'
        
        system_message = """You are ObeGee AI Assistant, a helpful and professional AI for the ObeGee managed OpenClaw service.
When users ask you to perform actions that could have real-world impact (like sending emails, making API calls, etc.), 
describe what action you would take and note that it requires approval before execution.
Be concise, helpful, and professional."""

        # For BYOK keys, we might need different handling
        # For now, Emergent integration handles both via OpenAI-compatible API
        chat = LlmChat(
            api_key=api_key,
            session_id=session_id,
            system_message=system_message
        ).with_model(provider, model)
        
        response = await chat.send_message(UserMessage(text=message))
        
        return {
            "success": True,
            "content": response,
            "session_id": session_id,
            "model": model,
            "provider": provider,
            "is_byok": is_byok
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "content": f"LLM call failed: {str(e)}"
        }

def main():
    parser = argparse.ArgumentParser(description='ObeGee LLM Helper')
    parser.add_argument('--message', '-m', required=True, help='Message to send to LLM')
    parser.add_argument('--session-id', '-s', required=True, help='Session ID')
    parser.add_argument('--provider', '-p', default='openai', help='LLM provider (openai, anthropic)')
    parser.add_argument('--model', default='gpt-5.2', help='Model name')
    parser.add_argument('--history', default='[]', help='JSON array of history messages')
    
    args = parser.parse_args()
    
    try:
        history = json.loads(args.history) if args.history else []
    except Exception:
        history = []
    
    result = asyncio.run(call_llm(args.message, args.session_id, args.provider, args.model, history))
    print(json.dumps(result))

if __name__ == '__main__':
    main()
