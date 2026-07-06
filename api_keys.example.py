# Copy this file to api_keys.py and fill in your credentials.
# api_keys.py is excluded from version control (.gitignore).
#
# Required for Document Analyzer:
#   openai_api_key       - OpenAI API key (primary AI provider)
#   anthropic_api_key    - Anthropic API key (fallback AI provider)
#   govinfo_api_key      - GovInfo API key (for downloading CFR/USC source documents)
#
# Optional (only needed if you enable models on that provider in config.json):
#   deepinfra_api_key    - DeepInfra token (for OpenAI-compatible models like GLM-5.2)
#
# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License


secrets = {
    'openai_api_key': 'sk-proj-YOUR_OPENAI_API_KEY',
    'anthropic_api_key': 'sk-ant-YOUR_ANTHROPIC_API_KEY',
    'govinfo_api_key': 'YOUR_GOVINFO_API_KEY',
    'deepinfra_api_key': 'YOUR_DEEPINFRA_TOKEN'
    }
