"""LARA STOCK Core Modular Architecture (`lara_core`).

Provides clean, modular subsystems for:
- `runtime_handoff`: Single-active deployment coordination on Render.
- `ban_guard`: Unified Bot + Website + Email + Google OAuth + Session ban enforcement.
- `ton_engine`: Multi-provider TON/LTC/USDT verification, canonical address matching & fast admin check.
- `security_vault`: AES-256-GCM secret encryption, SSRF guards, atomic OTP & HTTP hardening.
- `bot_theme`: Luxury Telegram Bot UI, automatic Premium Custom Emoji slots & crisp Safari fallbacks.
- `referral_auto_rewards`, `translation_repair`, `wwc_platform`, `wwc_manager_bridge`,
  `channel_broadcast_upgrade`, `admin_performance`, `traffic_efficiency`, `network_report_fixes`.
"""
