# ProfitBot Terminal - Cloudflare Worker


Private TradersPost alternative for futures prop firm accounts.

## Endpoints
- `/trading/webhook/{strategyId}/{password}` - TradingView webhooks
- `/api/ping` - Health check
- `/api/*` - Internal API

## Storage
- KV Namespace: `PROFITBOT_KV`
- Auto-flat scheduled: 16:55 ET daily

## Deployment
Cloudflare auto-deploys from main branch.
