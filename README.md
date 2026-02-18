# Overwatch Discord Queue Bot

Button-driven inhouse/PUG queue with admin-only slash commands.

## Behavior

- The queue panel lives in one configured text channel.
- Players do not use slash commands. They only use buttons:
  - `Join Tank`
  - `Join DPS`
  - `Join Support`
  - `Join Fill` (always available in role mode)
  - `Join Queue` (open queue mode)
  - `Leave Queue`
- If a user sends a message in the queue channel, the bot deletes that message and reposts the queue panel with the same state.
- When queue size reaches `players_per_match`, a match is created automatically and teams are posted.
- In role mode, `Fill` is a wildcard preference; fill players are assigned to missing Tank/DPS/Support slots during match creation.

## Admin Slash Commands

All slash commands are intended for admins (`Manage Server`):

- `/queue_admin_channel` set queue channel and post panel
- `/queue_admin_mode` switch between `role` and `open`
- `/queue_admin_rules` set:
  - `players_per_match`
  - `tank_per_team`
  - `dps_per_team`
  - `support_per_team`
- `/queue_admin_remove` remove a specific player from queue
- `/queue_admin_clear` clear queue
- `/queue_admin_refresh` repost queue panel

## Setup

1. Create a Discord app and bot in the [Discord Developer Portal](https://discord.com/developers/applications).
2. Invite the bot with scopes:
   - `bot`
   - `applications.commands`
3. Grant bot permissions in the queue channel:
   - View Channel
   - Send Messages
   - Manage Messages
   - Read Message History
4. Create env file:

   ```bash
   cp .env.example .env
   ```

5. Fill in `DISCORD_TOKEN` in `.env`.
6. Install and run:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   python -m src.main
   ```

## Environment Variables

- `DISCORD_TOKEN` (required)
- `COMMAND_GUILD_ID` (optional; faster command sync during development)
- `QUEUE_CHANNEL_ID` (optional; initial queue channel)
- `SQLITE_PATH` (default: `bot.db`)
- `PLAYERS_PER_MATCH` (default: `10`, must be even)
- `TANK_PER_TEAM` (default: `1`)
- `DPS_PER_TEAM` (default: `2`)
- `SUPPORT_PER_TEAM` (default: `2`)
- `DEFAULT_MMR` (default: `2500`)
- `DEFAULT_ROLE` (default: `flex`)
