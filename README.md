# Overwatch Discord Queue Bot

Button-driven inhouse/PUG queue with admin-only slash commands.

## Behavior

- The queue panel lives in one configured text channel.
- Optional modmail panel lives in one configured text channel (`MODMAIL_CHANNEL_ID`).
- Optional leaderboard panel lives in one configured text channel (`LEADERBOARD_CHANNEL_ID`).
- Players do not use slash commands. They only use buttons:
  - `Join Tank`
  - `Join DPS`
  - `Join Support`
  - `Join Fill` (always available in role mode)
  - `Join Queue` (open queue mode)
  - `Leave Queue`
- On first queue join, the bot asks the player for BattleTag and stores it.
- If a user sends a message in the queue channel, the bot deletes that message and reposts the queue panel with the same state.
- Modmail flow:
  - Users click `Open Ticket` on the modmail embed.
  - Bot creates private ticket threads (users can have multiple open tickets).
  - Ticket can be closed with the `Close Ticket` button or `/ticket_close`.
  - On close, bot posts ticket logs (including attachment/image files and a transcript) to `MODMAIL_LOGS_CHANNEL_ID`.
- When queue size reaches `players_per_match`, exactly one active match is created (no concurrent matches).
- Match flow:
  - Players use `Ready Up` (no fixed countdown).
  - Bot checks Team A / Team B voice channels when players ready.
  - If players are in `MAIN_VOICE_CHANNEL_ID`, bot auto-moves them to their team VC at start.
  - If players are elsewhere, match still starts and waits for them normally.
  - Match lifecycle updates are written into the active match embed (reduced channel spam).
  - Active match embed includes BattleTags, ready states, and a VC checklist (`in VC`, `missing`, `disconnected`).
  - Active match panel has `We Won`, `We Lost`, and `Escalate Dispute` buttons.
  - Once both teams submit reports, result buttons lock and embed shows report timestamps + first reporter.
  - Dispute escalation does not use `@here`.
  - When result is finalized, the active panel is replaced with a clean match-complete summary showing winner and per-player MMR changes.
  - Leaderboard image auto-regenerates and reposts whenever match MMR updates.
- Ready-check no-shows/disconnects are tracked per player in DB stats.
- In role mode, `Fill` is a wildcard preference; fill players are assigned to missing Tank/DPS/Support slots during match creation.
- New players get calibration MMR adjustments for their first 5 completed in-house matches (larger deltas), then normal Elo deltas apply.

## Admin Slash Commands

Most slash commands are intended for admins (`Manage Server`), except `/ticket_close`:

- `/queue_channel` set queue channel and post panel
- `/modmail_channel` set modmail channel and post panel
- `/modmail_logs_channel` set ticket logs channel
- `/modmail_logs_channel_id` set ticket logs channel by raw channel ID
- `/modmail_refresh` repost modmail panel
- `/queue_vc` set main/team voice channels for ready check and auto-move
- `/vc_finish` force-complete current VC check and start the match immediately (optionally treating synthetic test players as VC-ready)
- `/vc_private` toggle Team A/Team B VC private mode for manual joins
- `/queue_mode` switch between `role` and `open`
- `/queue_rules` set:
  - `players_per_match`
  - `tank_per_team`
  - `dps_per_team`
  - `support_per_team`
- `/queue_remove` remove a specific player from queue
- `/player_stats` show all stored DB stats for a player
- `/recent_matches` list recent matches and result status
- `/match_result` set winner (`Team A`, `Team B`, or `Draw`)
  - for the active match, this finalizes it immediately, posts the final summary, and attempts to start the next match if enough players are queued
- `/match_cancel` cancel active match (optional player requeue)
- `/match_remake` cancel active match, requeue players, and attempt immediate remake
- `/queue_clear` clear queue
- `/queue_refresh` repost queue panel
- `/ticket_close` close the current modmail ticket thread (ticket owner or staff)
- `/queue_admin_test_scenario` load synthetic test scenarios
- `/queue_admin_test_add` add synthetic test players by role
- `/queue_admin_test_results` apply synthetic win/loss/draw patterns to recent matches

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
4. If using modmail, grant bot permissions in the modmail panel channel:
   - View Channel
   - Send Messages
   - Create Private Threads
   - Send Messages in Threads
   - Manage Threads
   - Read Message History
5. If using modmail logs, grant bot permissions in the logs channel:
   - View Channel
   - Send Messages
   - Attach Files
   - Read Message History
6. Create env file:

   ```bash
   cp .env.example .env
   ```

7. Fill in `DISCORD_TOKEN` in `.env`.
8. Install and run:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   python -m src.main
   ```

## Docker Deployment

Use this when running on another machine with auto-restart behavior.

1. Copy env file and fill values:

   ```bash
   cp .env.example .env
   ```

2. Start container:

   ```bash
   docker compose up -d --build
   ```

3. View logs:

   ```bash
   docker compose logs -f overwatch-bot
   ```

4. Stop:

   ```bash
   docker compose down
   ```

Notes:
- Container restart policy is `unless-stopped`.
- SQLite DB is persisted in Docker volume `overwatch_bot_data`.
- `SQLITE_PATH` is set to `/data/bot.db` in compose.

## Environment Variables

- `DISCORD_TOKEN` (required)
- `COMMAND_GUILD_ID` (optional; faster command sync during development)
- `QUEUE_CHANNEL_ID` (optional; initial queue channel)
- `MODMAIL_CHANNEL_ID` (optional; initial modmail panel channel)
- `MODMAIL_LOGS_CHANNEL_ID` (optional; closed ticket log channel)
- `LEADERBOARD_CHANNEL_ID` (optional; channel where leaderboard image is posted)
- `MAIN_VOICE_CHANNEL_ID` (optional; waiting voice channel for queued players)
- `TEAM_A_VOICE_CHANNEL_ID` (optional; Team A match voice channel)
- `TEAM_B_VOICE_CHANNEL_ID` (optional; Team B match voice channel)
- `SQLITE_PATH` (default: `bot.db`)
- `PLAYERS_PER_MATCH` (default: `10`, must be even)
- `TANK_PER_TEAM` (default: `1`)
- `DPS_PER_TEAM` (default: `2`)
- `SUPPORT_PER_TEAM` (default: `2`)
- `DEFAULT_MMR` (default: `2500`, valid range: `0-5000`)
- `DEFAULT_ROLE` (default: `flex`)
