from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from io import BytesIO
import logging
import random
import re

import discord
from discord import app_commands
from discord.ext import commands
from PIL import Image, ImageDraw, ImageFont

from .config import Settings, load_settings
from .matchmaking import make_match
from .models import AssignedPlayer, MatchMmrChange, ModmailConfig, ModmailTicket, QueueConfig, QueuedPlayer, Team
from .storage import Database


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("ow-bot")

QUEUE_MODE_CHOICES = [
    app_commands.Choice(name="Role Queue", value="role"),
    app_commands.Choice(name="Open Queue", value="open"),
]
RESULT_TEAM_CHOICES = [
    app_commands.Choice(name="Team A", value="Team A"),
    app_commands.Choice(name="Team B", value="Team B"),
    app_commands.Choice(name="Draw", value="Draw"),
]
TEST_SCENARIO_CHOICES = [
    app_commands.Choice(name="Role standard full", value="role_standard"),
    app_commands.Choice(name="Role with fill", value="role_with_fill"),
    app_commands.Choice(name="Role all fill", value="role_all_fill"),
    app_commands.Choice(name="Role fallback test", value="role_fallback"),
    app_commands.Choice(name="Role partial", value="role_partial"),
    app_commands.Choice(name="Open full", value="open_full"),
]
TEST_RESULT_MODE_CHOICES = [
    app_commands.Choice(name="Team A wins", value="team_a"),
    app_commands.Choice(name="Team B wins", value="team_b"),
    app_commands.Choice(name="Alternating A/B", value="alternating"),
    app_commands.Choice(name="All draws", value="draw"),
    app_commands.Choice(name="Clear results", value="clear"),
]
TEST_ROLE_CHOICES = [
    app_commands.Choice(name="Tank", value="tank"),
    app_commands.Choice(name="DPS", value="dps"),
    app_commands.Choice(name="Support", value="support"),
    app_commands.Choice(name="Fill", value="fill"),
    app_commands.Choice(name="Open", value="open"),
]
TEST_BOT_ID_BASE = 980_000_000_000_000_000
RESULT_EMBED_TITLE_RE = re.compile(r"^Match #(\d+) Completed$")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _discord_ts(value: str | None) -> str:
    dt = _parse_iso(value)
    if dt is None:
        return "n/a"
    return f"<t:{int(dt.timestamp())}:R>"


def _channel_ref(channel_id: int | None) -> str:
    if channel_id is None or channel_id <= 0:
        return "not set"
    return f"<#{channel_id}>"


def _format_player(player: AssignedPlayer) -> str:
    role = player.assigned_role.upper()
    return f"<@{player.discord_id}> | `{role}` | MMR `{player.mmr}`"


def _format_team(team: Team) -> str:
    lines = [_format_player(player) for player in team.players]
    lines.append(f"Average MMR: `{team.average_mmr}`")
    return "\n".join(lines)


def _format_role_distribution(role_counts: dict[str, int]) -> str:
    if not role_counts:
        return "None"
    ordered_roles = ["tank", "dps", "support", "fill", "open"]
    parts: list[str] = []
    for role in ordered_roles:
        count = role_counts.get(role)
        if count:
            parts.append(f"{role}:{count}")
    for role, count in role_counts.items():
        if role not in ordered_roles:
            parts.append(f"{role}:{count}")
    return ", ".join(parts) if parts else "None"


def _format_delta(delta: int) -> str:
    if delta > 0:
        return f"+{delta}"
    return str(delta)


def _load_font(size: int, *, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = [
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf" if bold else "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size=size)
        except OSError:
            continue
    return ImageFont.load_default()


def _is_admin(interaction: discord.Interaction) -> bool:
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    return bool(member and member.guild_permissions.manage_guild)


def _is_ticket_staff(interaction: discord.Interaction) -> bool:
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    return bool(member and (member.guild_permissions.manage_guild or member.guild_permissions.manage_threads))


class QueuePanelView(discord.ui.View):
    def __init__(self, bot: OverwatchBot, config: QueueConfig) -> None:
        super().__init__(timeout=None)
        self.bot = bot
        is_open_queue = config.queue_mode == "open"
        if is_open_queue:
            self.remove_item(self.join_tank)
            self.remove_item(self.join_dps)
            self.remove_item(self.join_support)
            self.remove_item(self.join_fill)
        else:
            self.remove_item(self.join_open)

    @discord.ui.button(label="Join Tank", style=discord.ButtonStyle.secondary, custom_id="queue_join_tank", row=0)
    async def join_tank(self, interaction: discord.Interaction, _: discord.ui.Button[QueuePanelView]) -> None:
        await self.bot.queue_service.handle_join(interaction, "tank")

    @discord.ui.button(label="Join DPS", style=discord.ButtonStyle.secondary, custom_id="queue_join_dps", row=0)
    async def join_dps(self, interaction: discord.Interaction, _: discord.ui.Button[QueuePanelView]) -> None:
        await self.bot.queue_service.handle_join(interaction, "dps")

    @discord.ui.button(
        label="Join Support",
        style=discord.ButtonStyle.secondary,
        custom_id="queue_join_support",
        row=0,
    )
    async def join_support(self, interaction: discord.Interaction, _: discord.ui.Button[QueuePanelView]) -> None:
        await self.bot.queue_service.handle_join(interaction, "support")

    @discord.ui.button(label="Join Fill", style=discord.ButtonStyle.secondary, custom_id="queue_join_fill", row=0)
    async def join_fill(self, interaction: discord.Interaction, _: discord.ui.Button[QueuePanelView]) -> None:
        await self.bot.queue_service.handle_join(interaction, "fill")

    @discord.ui.button(label="Join Queue", style=discord.ButtonStyle.secondary, custom_id="queue_join_open", row=1)
    async def join_open(self, interaction: discord.Interaction, _: discord.ui.Button[QueuePanelView]) -> None:
        await self.bot.queue_service.handle_join(interaction, "open")

    @discord.ui.button(label="Leave Queue", style=discord.ButtonStyle.danger, custom_id="queue_leave", row=1)
    async def leave_queue(self, interaction: discord.Interaction, _: discord.ui.Button[QueuePanelView]) -> None:
        await self.bot.queue_service.handle_leave(interaction)


class ActiveMatchView(discord.ui.View):
    def __init__(
        self,
        bot: OverwatchBot,
        *,
        reports_locked: bool = False,
        captain_claim_enabled: bool = False,
    ) -> None:
        super().__init__(timeout=None)
        self.bot = bot
        if reports_locked:
            self.we_won.disabled = True
            self.we_lost.disabled = True
        if not captain_claim_enabled:
            self.remove_item(self.claim_captain)

    @discord.ui.button(label="We Won", style=discord.ButtonStyle.success, custom_id="active_match_we_won", row=0)
    async def we_won(self, interaction: discord.Interaction, _: discord.ui.Button[ActiveMatchView]) -> None:
        await self.bot.queue_service.handle_match_report(interaction, report_type="win")

    @discord.ui.button(label="We Lost", style=discord.ButtonStyle.secondary, custom_id="active_match_we_lost", row=0)
    async def we_lost(self, interaction: discord.Interaction, _: discord.ui.Button[ActiveMatchView]) -> None:
        await self.bot.queue_service.handle_match_report(interaction, report_type="loss")

    @discord.ui.button(
        label="Claim Captain",
        style=discord.ButtonStyle.secondary,
        custom_id="active_match_claim_captain",
        row=1,
    )
    async def claim_captain(self, interaction: discord.Interaction, _: discord.ui.Button[ActiveMatchView]) -> None:
        await self.bot.queue_service.handle_claim_captain(interaction)


class MatchResultView(discord.ui.View):
    def __init__(self, bot: OverwatchBot) -> None:
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(
        label="Dispute Winner",
        style=discord.ButtonStyle.danger,
        custom_id="match_result_dispute",
    )
    async def dispute_winner(self, interaction: discord.Interaction, _: discord.ui.Button[MatchResultView]) -> None:
        await self.bot.queue_service.handle_result_dispute(interaction)


class BattleTagModal(discord.ui.Modal, title="Set BattleTag"):
    battletag = discord.ui.TextInput(
        label="BattleTag",
        placeholder="Player#12345",
        min_length=3,
        max_length=32,
        required=True,
    )

    def __init__(self, bot: OverwatchBot, requested_role: str) -> None:
        super().__init__(timeout=120)
        self.bot = bot
        self.requested_role = requested_role

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.bot.queue_service.handle_join_after_battletag(
            interaction,
            requested_role=self.requested_role,
            battletag=str(self.battletag).strip(),
        )


class ModmailPanelView(discord.ui.View):
    def __init__(self, bot: OverwatchBot) -> None:
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(label="Open Ticket", style=discord.ButtonStyle.primary, custom_id="modmail_open_ticket")
    async def open_ticket(self, interaction: discord.Interaction, _: discord.ui.Button[ModmailPanelView]) -> None:
        await self.bot.modmail_service.handle_open_ticket(interaction)


class TicketThreadView(discord.ui.View):
    def __init__(self, bot: OverwatchBot) -> None:
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(label="Close Ticket", style=discord.ButtonStyle.danger, custom_id="modmail_close_ticket")
    async def close_ticket(self, interaction: discord.Interaction, _: discord.ui.Button[TicketThreadView]) -> None:
        await self.bot.modmail_service.handle_close_ticket(interaction)


class ModmailService:
    def __init__(self, bot: OverwatchBot) -> None:
        self.bot = bot
        self.lock = asyncio.Lock()

    async def _resolve_panel_channel(self, config: ModmailConfig) -> discord.TextChannel | None:
        if not config.panel_channel_id:
            return None
        channel = self.bot.get_channel(config.panel_channel_id)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(config.panel_channel_id)
            except discord.DiscordException:
                logger.warning("Unable to fetch modmail channel %s", config.panel_channel_id)
                return None
        if isinstance(channel, discord.TextChannel):
            return channel
        logger.warning("Configured modmail channel is not a text channel: %s", config.panel_channel_id)
        return None

    async def _resolve_logs_channel(self, config: ModmailConfig) -> discord.TextChannel | None:
        if not config.logs_channel_id:
            return None
        channel = self.bot.get_channel(config.logs_channel_id)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(config.logs_channel_id)
            except discord.DiscordException:
                logger.warning("Unable to fetch modmail logs channel %s", config.logs_channel_id)
                return None
        if isinstance(channel, discord.TextChannel):
            return channel
        logger.warning("Configured modmail logs channel is not a text channel: %s", config.logs_channel_id)
        return None

    async def _resolve_thread_by_id(self, thread_id: int) -> discord.Thread | None:
        channel = self.bot.get_channel(thread_id)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(thread_id)
            except discord.DiscordException:
                return None
        if isinstance(channel, discord.Thread):
            return channel
        return None

    def _is_modmail_panel_message(self, message: discord.Message) -> bool:
        if self.bot.user is None or message.author.id != self.bot.user.id:
            return False
        has_title = any((embed.title or "").strip() == "Modmail / Tickets" for embed in message.embeds)
        if not has_title:
            return False
        for row in message.components:
            for child in row.children:
                if getattr(child, "custom_id", None) == "modmail_open_ticket":
                    return True
        return False

    async def _find_modmail_panel_messages(
        self,
        channel: discord.TextChannel,
        *,
        limit: int = 200,
    ) -> list[discord.Message]:
        if self.bot.user is None:
            return []
        matches: list[discord.Message] = []
        try:
            async for message in channel.history(limit=limit):
                if self._is_modmail_panel_message(message):
                    matches.append(message)
        except discord.DiscordException:
            return matches
        return matches

    async def _cleanup_duplicate_panels(
        self,
        channel: discord.TextChannel,
        *,
        keep_message_id: int,
        candidates: list[discord.Message] | None = None,
    ) -> None:
        messages = candidates if candidates is not None else await self._find_modmail_panel_messages(channel)
        for message in messages:
            if message.id == keep_message_id:
                continue
            try:
                await message.delete()
            except discord.DiscordException:
                continue

    def _ticket_name(self, member: discord.Member) -> str:
        raw = member.display_name.lower().strip()
        cleaned = "".join(ch if ch.isalnum() else "-" for ch in raw)
        compact = "-".join(part for part in cleaned.split("-") if part)[:42]
        if not compact:
            compact = "user"
        return f"ticket-{compact}-{member.id}"

    def _panel_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="Modmail / Tickets",
            description="Need help from staff? Press **Open Ticket** to create a private support thread.",
            color=discord.Color.blurple(),
        )
        return embed

    def _ticket_embed(self, ticket_id: int, member: discord.Member, created_at: str) -> discord.Embed:
        embed = discord.Embed(
            title=f"Ticket #{ticket_id}",
            description="Use this thread for private support with moderators.",
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="User", value=f"{member.mention} (`{member.id}`)", inline=False)
        embed.add_field(name="Opened", value=_discord_ts(created_at), inline=True)
        embed.set_footer(text="Moderators can close with the button below.")
        return embed

    def _build_transcript_text(
        self,
        *,
        ticket_id: int,
        thread_id: int,
        opener_id: int,
        closed_by_id: int,
        messages: list[discord.Message],
    ) -> str:
        lines: list[str] = [
            f"Ticket #{ticket_id}",
            f"Thread ID: {thread_id}",
            f"Opened by: {opener_id}",
            f"Closed by: {closed_by_id}",
            f"Message count: {len(messages)}",
            "",
        ]
        for message in messages:
            timestamp = message.created_at.astimezone(timezone.utc).replace(microsecond=0).isoformat()
            lines.append(f"[{timestamp}] {message.author} ({message.author.id})")
            lines.append(message.content if message.content else "<no text>")
            if message.attachments:
                for attachment in message.attachments:
                    lines.append(f"Attachment: {attachment.filename} | {attachment.url}")
            if message.embeds:
                lines.append(f"Embeds: {len(message.embeds)}")
            if message.stickers:
                lines.append(f"Stickers: {len(message.stickers)}")
            lines.append("-" * 72)
        return "\n".join(lines)

    async def _send_ticket_log(self, *, ticket: ModmailTicket, thread: discord.Thread, closed_by_id: int) -> str:
        config = self.bot.db.get_modmail_config()
        logs_channel = await self._resolve_logs_channel(config)
        if logs_channel is None:
            return "Log not posted (set a logs channel with `/modmail_logs_channel`)."

        messages: list[discord.Message] = []
        try:
            async for message in thread.history(limit=None, oldest_first=True):
                messages.append(message)
        except discord.DiscordException:
            return f"Log channel set, but failed to read thread history for ticket #{ticket.ticket_id}."

        summary_embed = discord.Embed(
            title=f"Ticket #{ticket.ticket_id} Log",
            color=discord.Color.gold(),
            timestamp=datetime.now(timezone.utc),
        )
        summary_embed.add_field(name="Ticket User", value=f"<@{ticket.user_id}> (`{ticket.user_id}`)", inline=False)
        summary_embed.add_field(name="Closed By", value=f"<@{closed_by_id}> (`{closed_by_id}`)", inline=False)
        summary_embed.add_field(name="Source Thread", value=f"<#{thread.id}>", inline=True)
        summary_embed.add_field(name="Messages", value=f"`{len(messages)}`", inline=True)

        transcript = self._build_transcript_text(
            ticket_id=ticket.ticket_id,
            thread_id=thread.id,
            opener_id=ticket.user_id,
            closed_by_id=closed_by_id,
            messages=messages,
        )
        transcript_file = discord.File(
            fp=BytesIO(transcript.encode("utf-8")),
            filename=f"ticket-{ticket.ticket_id}-transcript.txt",
        )

        try:
            await logs_channel.send(embed=summary_embed, file=transcript_file)
        except discord.DiscordException:
            return f"Failed to send transcript for ticket #{ticket.ticket_id} to logs channel."

        for message in messages:
            if not message.attachments:
                continue
            files: list[discord.File] = []
            failed_urls: list[str] = []
            for attachment in message.attachments:
                try:
                    files.append(await attachment.to_file(use_cached=True))
                except discord.DiscordException:
                    failed_urls.append(attachment.url)
                except ValueError:
                    failed_urls.append(attachment.url)

            if files:
                for index in range(0, len(files), 10):
                    chunk = files[index : index + 10]
                    caption = (
                        f"Ticket #{ticket.ticket_id} attachments from <@{message.author.id}> "
                        f"at <t:{int(message.created_at.timestamp())}:f>"
                    )
                    if index > 0:
                        caption = f"Ticket #{ticket.ticket_id} attachments (continued)"
                    try:
                        await logs_channel.send(content=caption, files=chunk)
                    except discord.DiscordException:
                        continue
            if failed_urls:
                for index in range(0, len(failed_urls), 10):
                    chunk_urls = failed_urls[index : index + 10]
                    try:
                        await logs_channel.send(
                            content=(
                                f"Ticket #{ticket.ticket_id} attachment URLs from <@{message.author.id}>:\n"
                                + "\n".join(chunk_urls)
                            )
                        )
                    except discord.DiscordException:
                        continue

        return f"Log posted in {logs_channel.mention}."

    async def sync_panel(self, *, repost: bool = False) -> None:
        config = self.bot.db.get_modmail_config()
        channel = await self._resolve_panel_channel(config)
        if channel is None:
            return

        embed = self._panel_embed()
        view = ModmailPanelView(self.bot)
        message_id = config.panel_message_id or 0

        known_message: discord.Message | None = None
        if message_id > 0:
            try:
                candidate = await channel.fetch_message(message_id)
                if self._is_modmail_panel_message(candidate):
                    known_message = candidate
            except discord.DiscordException:
                known_message = None
        discovered = await self._find_modmail_panel_messages(channel)
        if known_message is None and discovered:
            known_message = discovered[0]

        if repost:
            if known_message is None:
                known_message = await channel.send(embed=embed, view=view)
            else:
                await known_message.edit(content=None, embed=embed, view=view)
            self.bot.db.set_modmail_message(known_message.id)
            await self._cleanup_duplicate_panels(
                channel,
                keep_message_id=known_message.id,
                candidates=discovered,
            )
            return

        if known_message is None:
            known_message = await channel.send(embed=embed, view=view)
        else:
            try:
                await known_message.edit(content=None, embed=embed, view=view)
            except discord.DiscordException as exc:
                logger.warning("Unable to sync modmail panel: %s", exc)
                return
        self.bot.db.set_modmail_message(known_message.id)
        await self._cleanup_duplicate_panels(
            channel,
            keep_message_id=known_message.id,
            candidates=discovered,
        )

    async def admin_set_channel(self, channel_id: int) -> None:
        async with self.lock:
            self.bot.db.set_modmail_channel(channel_id)
            await self.sync_panel(repost=True)

    async def admin_set_logs_channel(self, channel_id: int) -> None:
        async with self.lock:
            self.bot.db.set_modmail_logs_channel(channel_id)

    async def handle_open_ticket(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True, thinking=False)
        if interaction.guild is None or not isinstance(interaction.user, discord.Member):
            await interaction.followup.send("Tickets can only be opened from inside a server.", ephemeral=True)
            return

        async with self.lock:
            config = self.bot.db.get_modmail_config()
            channel = await self._resolve_panel_channel(config)
            if channel is None:
                await interaction.followup.send("Modmail channel is not configured.", ephemeral=True)
                return

            try:
                thread = await channel.create_thread(
                    name=self._ticket_name(interaction.user),
                    type=discord.ChannelType.private_thread,
                    auto_archive_duration=1440,
                    invitable=False,
                    reason=f"Modmail ticket for {interaction.user.id}",
                )
            except discord.DiscordException:
                await interaction.followup.send(
                    "I couldn't create a private ticket thread. Check channel permissions for private threads.",
                    ephemeral=True,
                )
                return

            try:
                await thread.add_user(interaction.user)
            except discord.DiscordException:
                # If this fails, keep the thread and let staff handle it.
                pass

            try:
                ticket = self.bot.db.create_modmail_ticket(
                    guild_id=interaction.guild.id,
                    user_id=interaction.user.id,
                    thread_id=thread.id,
                )
            except Exception:
                try:
                    await thread.delete(reason="Failed to persist ticket")
                except discord.DiscordException:
                    pass
                await interaction.followup.send("Failed to create a ticket record. Try again.", ephemeral=True)
                return

            await thread.send(
                content=f"{interaction.user.mention} opened a ticket.",
                embed=self._ticket_embed(ticket.ticket_id, interaction.user, ticket.created_at),
                view=TicketThreadView(self.bot),
            )
            await interaction.followup.send(f"Ticket opened: {thread.mention}", ephemeral=True)

    async def handle_close_ticket(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(thinking=False)
        channel = interaction.channel
        if not isinstance(channel, discord.Thread):
            await interaction.followup.send("Use this in a ticket thread.", ephemeral=True)
            return

        ticket = self.bot.db.get_modmail_ticket_by_thread(channel.id)
        if ticket is None or ticket.status != "open":
            await interaction.followup.send("This thread is not an open ticket.", ephemeral=True)
            return

        allowed = interaction.user.id == ticket.user_id or _is_ticket_staff(interaction)
        if not allowed:
            await interaction.followup.send("Only the ticket owner or staff can close this ticket.", ephemeral=True)
            return

        async with self.lock:
            changed = self.bot.db.close_modmail_ticket_by_thread(thread_id=channel.id, closed_by=interaction.user.id)
            if not changed:
                await interaction.followup.send("Ticket is already closed.", ephemeral=True)
                return
            await self._send_ticket_log(ticket=ticket, thread=channel, closed_by_id=interaction.user.id)

            closed_embed = discord.Embed(
                title=f"Ticket #{ticket.ticket_id} Closed",
                description=f"Closed by <@{interaction.user.id}>",
                color=discord.Color.dark_grey(),
                timestamp=datetime.now(timezone.utc),
            )
            try:
                await channel.send(embed=closed_embed)
            except discord.DiscordException:
                pass
            try:
                await channel.edit(locked=True, archived=True, reason=f"Ticket closed by {interaction.user.id}")
            except discord.DiscordException:
                pass

class QueueService:
    def __init__(self, bot: OverwatchBot) -> None:
        self.bot = bot
        self.lock = asyncio.Lock()
        self._reposting = False
        self._test_counter = 0
        self._ready_check_tasks: dict[int, asyncio.Task[None]] = {}
        self._active_match_updates: dict[int, str] = {}
        self._vc_check_status: dict[int, dict[str, object]] = {}
        self._leaderboard_message_id: int | None = None
        self._battletag_reminder_tasks: dict[int, asyncio.Task[None]] = {}

    def _cancel_battletag_reminder(self, discord_id: int) -> None:
        task = self._battletag_reminder_tasks.pop(discord_id, None)
        if task and not task.done():
            task.cancel()

    async def _send_battletag_reminder(self, interaction: discord.Interaction, user_id: int) -> None:
        try:
            await asyncio.sleep(12)
            player = self.bot.db.get_player(user_id)
            queued = self.bot.db.get_queue_entry(user_id)
            if queued is not None:
                return
            if player is not None and player.battletag:
                return
            await interaction.followup.send(
                "BattleTag setup was cancelled. Press a join button again to enter queue.",
                ephemeral=True,
            )
        except asyncio.CancelledError:
            return
        except discord.DiscordException:
            return
        finally:
            current = self._battletag_reminder_tasks.get(user_id)
            if current and current.done():
                self._battletag_reminder_tasks.pop(user_id, None)

    async def _resolve_text_channel_by_id(self, channel_id: int) -> discord.TextChannel | None:
        channel = self.bot.get_channel(channel_id)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(channel_id)
            except Exception:
                return None
        if isinstance(channel, discord.TextChannel):
            return channel
        return None

    async def _safe_fetch_member(self, guild: discord.Guild, discord_id: int) -> discord.Member | None:
        member = guild.get_member(discord_id)
        if member is not None:
            return member
        try:
            return await guild.fetch_member(discord_id)
        except discord.DiscordException:
            return None

    def _team_ids_from_payload(self, payload: list[dict[str, object]]) -> list[int]:
        ids: list[int] = []
        for entry in payload:
            try:
                discord_id = int(entry.get("discord_id", -1))
            except (TypeError, ValueError):
                continue
            if discord_id > 0:
                ids.append(discord_id)
        return ids

    def _reports_complete(self, match_id: int) -> bool:
        reports = self.bot.db.get_match_reports(match_id)
        return "Team A" in reports and "Team B" in reports

    def _match_player_ids_from_db(self, match_id: int) -> list[int]:
        teams = self.bot.db.get_match_teams(match_id)
        if teams is None:
            return []
        team_a_payload, team_b_payload = teams
        return self._team_ids_from_payload(team_a_payload) + self._team_ids_from_payload(team_b_payload)

    async def _admin_ids_in_match(self, guild: discord.Guild, player_ids: list[int]) -> list[int]:
        admin_ids: list[int] = []
        for discord_id in player_ids:
            member = await self._safe_fetch_member(guild, discord_id)
            if member and member.guild_permissions.manage_guild:
                admin_ids.append(discord_id)
        return admin_ids

    async def _auto_assign_admin_captain(self, match_id: int, guild: discord.Guild, player_ids: list[int]) -> int | None:
        existing = self.bot.db.get_match_captain(match_id)
        if existing is not None:
            return existing.captain_id
        admin_ids = await self._admin_ids_in_match(guild, player_ids)
        if not admin_ids:
            return None
        chosen = random.choice(admin_ids)
        self.bot.db.set_match_captain(
            match_id=match_id,
            captain_id=chosen,
            selected_by=chosen,
            selection_method="admin_auto",
        )
        return chosen

    def _match_id_from_result_message(self, message: discord.Message | None) -> int | None:
        if message is None or not message.embeds:
            return None
        title = (message.embeds[0].title or "").strip()
        match = RESULT_EMBED_TITLE_RE.match(title)
        if match is None:
            return None
        try:
            return int(match.group(1))
        except (TypeError, ValueError):
            return None

    def _summarize_missing_mentions(self, ids: list[int], *, limit: int = 4) -> str:
        if not ids:
            return "none"
        selected = ids[:limit]
        mentions = " ".join(f"<@{discord_id}>" for discord_id in selected)
        remaining = len(ids) - len(selected)
        if remaining > 0:
            return f"{mentions} +{remaining}"
        return mentions

    def _all_match_player_ids(self, team_a: Team, team_b: Team) -> list[int]:
        return [player.discord_id for player in team_a.players] + [player.discord_id for player in team_b.players]

    def _team_roster_block(self, team: Team) -> str:
        if not team.players:
            return "None"
        lines = []
        for player in team.players:
            lines.append(f"<@{player.discord_id}> - `{player.assigned_role.upper()}`, SR `{player.mmr}`")
        lines.append(f"Average SR: `{team.average_mmr}`")
        return "\n".join(lines)

    def _team_battletag_block(self, team: Team, battletags: dict[int, str | None]) -> str:
        if not team.players:
            return "None"
        return "\n".join(f"<@{player.discord_id}>: `{battletags.get(player.discord_id) or 'not set'}`" for player in team.players)

    def _is_synthetic_player(self, discord_id: int) -> bool:
        return discord_id >= TEST_BOT_ID_BASE

    async def _move_members_from_main_vc(
        self,
        guild: discord.Guild,
        player_ids: list[int],
        from_voice_channel_id: int | None,
        to_voice_channel_id: int | None,
    ) -> int:
        if not from_voice_channel_id or not to_voice_channel_id:
            return 0
        target_channel = guild.get_channel(to_voice_channel_id)
        if not isinstance(target_channel, discord.VoiceChannel):
            return 0
        moved = 0
        for discord_id in player_ids:
            member = await self._safe_fetch_member(guild, discord_id)
            if member is None or member.voice is None or member.voice.channel is None:
                continue
            if member.voice.channel.id != from_voice_channel_id:
                continue
            try:
                await member.move_to(target_channel)
                moved += 1
            except discord.DiscordException:
                continue
        return moved

    def _active_match_mentions(self, team_a_ids: list[int], team_b_ids: list[int]) -> str:
        everyone = team_a_ids + team_b_ids
        return " ".join(f"<@{pid}>" for pid in everyone)

    async def _build_active_match_embed(self, active_match_id: int) -> discord.Embed | None:
        active = self.bot.db.get_active_match()
        if active is None or active.match_id != active_match_id:
            return None
        teams = self.bot.db.get_match_teams(active.match_id)
        if teams is None:
            return None
        team_a_payload, team_b_payload = teams
        team_a = Team(
            name="Team A",
            players=[
                AssignedPlayer(
                    discord_id=int(p.get("discord_id", 0)),
                    display_name=str(p.get("display_name", "Unknown")),
                    mmr=int(p.get("mmr", 0)),
                    preferred_role=str(p.get("preferred_role", "fill")),
                    assigned_role=str(p.get("assigned_role", "fill")),
                )
                for p in team_a_payload
            ],
        )
        team_b = Team(
            name="Team B",
            players=[
                AssignedPlayer(
                    discord_id=int(p.get("discord_id", 0)),
                    display_name=str(p.get("display_name", "Unknown")),
                    mmr=int(p.get("mmr", 0)),
                    preferred_role=str(p.get("preferred_role", "fill")),
                    assigned_role=str(p.get("assigned_role", "fill")),
                )
                for p in team_b_payload
            ],
        )
        all_player_ids = self._all_match_player_ids(team_a, team_b)
        battletags = self.bot.db.get_player_battletags(all_player_ids)
        status_label = {
            "waiting_vc": "Waiting For VC Check",
            "live": "Live",
            "disputed": "Disputed",
        }.get(active.status, active.status)
        reports = self.bot.db.get_match_reports(active.match_id)
        report_count = len(reports)

        embed = discord.Embed(
            title=f"Active Match #{active.match_id}",
            description=(
                f"Phase: `{status_label}`\n"
                f"Players: `{len(all_player_ids)}` | Reports: `{report_count}/1`"
            ),
            color=discord.Color.orange() if active.status == "waiting_vc" else discord.Color.green(),
        )
        embed.timestamp = datetime.now(timezone.utc)
        embed.add_field(
            name="Voice Channels",
            value=(
                f"Team A: {_channel_ref(active.team_a_voice_channel_id)}\n"
                f"Team B: {_channel_ref(active.team_b_voice_channel_id)}"
            ),
            inline=False,
        )
        embed.add_field(
            name=f"Team A ({len(team_a.players)} players)",
            value=self._team_roster_block(team_a),
            inline=False,
        )
        embed.add_field(
            name=f"Team B ({len(team_b.players)} players)",
            value=self._team_roster_block(team_b),
            inline=False,
        )

        if active.status == "waiting_vc" and active.ready_deadline:
            embed.add_field(name="Ready Check Deadline", value=_discord_ts(active.ready_deadline), inline=False)
        if active.started_at:
            embed.add_field(name="Started", value=_discord_ts(active.started_at), inline=False)
        embed.add_field(name="Team A BattleTags", value=self._team_battletag_block(team_a, battletags), inline=False)
        embed.add_field(name="Team B BattleTags", value=self._team_battletag_block(team_b, battletags), inline=False)

        captain = self.bot.db.get_match_captain(active.match_id)
        if captain is not None:
            method = "Admin" if captain.selection_method == "admin_auto" else "Player"
            embed.add_field(
                name="Lobby Captain",
                value=f"<@{captain.captain_id}> ({method} selection)",
                inline=False,
            )
        elif active.status in {"live", "disputed"}:
            captain_text = "Captain selection is open. First match player to press `Claim Captain` becomes captain."
            embed.add_field(
                name="Lobby Captain",
                value=captain_text,
                inline=False,
            )

        if active.escalated:
            embed.add_field(name="Dispute", value="Escalated to admins for resolution.", inline=False)

        return embed

    async def resolve_queue_channel(self, config: QueueConfig) -> discord.TextChannel | None:
        if not config.queue_channel_id:
            return None
        channel = self.bot.get_channel(config.queue_channel_id)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(config.queue_channel_id)
            except discord.DiscordException:
                logger.warning("Unable to fetch queue channel %s", config.queue_channel_id)
                return None
        if isinstance(channel, discord.TextChannel):
            return channel
        logger.warning("Configured queue channel is not a text channel: %s", config.queue_channel_id)
        return None

    def _mention_list(self, players: list[QueuedPlayer], limit: int = 25) -> str:
        if not players:
            return "None"
        selected = players[:limit]
        body = " ".join(f"<@{player.discord_id}>" for player in selected)
        remaining = len(players) - len(selected)
        if remaining > 0:
            body = f"{body} +{remaining} more"
        return body

    def _next_test_player(self, role: str) -> tuple[int, str]:
        self._test_counter += 1
        discord_id = TEST_BOT_ID_BASE + self._test_counter
        display_name = f"Test {self._test_counter:03d} {role.upper()}"
        return discord_id, display_name

    def _leaderboard_entries(self) -> list[dict[str, object]]:
        rows = self.bot.db.list_role_rating_rows()
        entries: list[dict[str, object]] = []
        for row in rows:
            games_played = int(row["games_played"])
            if games_played <= 0:
                continue
            discord_id = int(row["discord_id"])
            tank = int(row["tank_mmr"])
            dps = int(row["dps_mmr"])
            support = int(row["support_mmr"])
            entries.append(
                {
                    "discord_id": discord_id,
                    "display_name": str(row["display_name"]),
                    "tank_sr": tank,
                    "dps_sr": dps,
                    "support_sr": support,
                    "global_sr": tank + dps + support,
                    "games_played": games_played,
                }
            )
        non_synthetic = [entry for entry in entries if not self._is_synthetic_player(int(entry["discord_id"]))]
        return non_synthetic if non_synthetic else entries

    def _render_leaderboard_image(self, *, limit: int = 10) -> BytesIO:
        entries = self._leaderboard_entries()
        sorted_boards = {
            "Global": sorted(entries, key=lambda item: (-int(item["global_sr"]), str(item["display_name"]).lower(), int(item["discord_id"])))[:limit],
            "Tank": sorted(entries, key=lambda item: (-int(item["tank_sr"]), str(item["display_name"]).lower(), int(item["discord_id"])))[:limit],
            "DPS": sorted(entries, key=lambda item: (-int(item["dps_sr"]), str(item["display_name"]).lower(), int(item["discord_id"])))[:limit],
            "Support": sorted(entries, key=lambda item: (-int(item["support_sr"]), str(item["display_name"]).lower(), int(item["discord_id"])))[:limit],
        }
        key_by_board = {
            "Global": "global_sr",
            "Tank": "tank_sr",
            "DPS": "dps_sr",
            "Support": "support_sr",
        }

        image = Image.new("RGB", (1500, 1080), (14, 18, 26))
        draw = ImageDraw.Draw(image)
        title_font = _load_font(52, bold=True)
        section_font = _load_font(34, bold=True)
        row_font = _load_font(26)
        value_font = _load_font(28, bold=True)
        sub_font = _load_font(22)

        draw.text((44, 24), "In-House Leaderboard", fill=(235, 240, 250), font=title_font)
        draw.text((46, 84), f"Updated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}", fill=(130, 145, 170), font=sub_font)

        margin_x = 40
        top = 130
        gap = 28
        panel_w = (1500 - (margin_x * 2) - gap) // 2
        panel_h = (1080 - top - 40 - gap) // 2
        panels = [
            ("Global", margin_x, top),
            ("Tank", margin_x + panel_w + gap, top),
            ("DPS", margin_x, top + panel_h + gap),
            ("Support", margin_x + panel_w + gap, top + panel_h + gap),
        ]

        def _text_width(text: str, font: ImageFont.FreeTypeFont | ImageFont.ImageFont) -> int:
            box = draw.textbbox((0, 0), text, font=font)
            return box[2] - box[0]

        for board_name, x, y in panels:
            draw.rounded_rectangle(
                (x, y, x + panel_w, y + panel_h),
                radius=18,
                fill=(22, 30, 43),
                outline=(52, 74, 108),
                width=2,
            )
            draw.text((x + 22, y + 16), f"{board_name} Top 10", fill=(220, 235, 255), font=section_font)
            rows_for_board = sorted_boards[board_name]
            stat_key = key_by_board[board_name]
            row_y = y + 72
            row_step = 36
            if not rows_for_board:
                draw.text((x + 22, row_y), "No players yet.", fill=(150, 165, 190), font=row_font)
                continue
            for rank, entry in enumerate(rows_for_board, start=1):
                name = str(entry["display_name"]).strip() or f"User {int(entry['discord_id'])}"
                if len(name) > 22:
                    name = f"{name[:21]}..."
                left_text = f"{rank:>2}. {name}"
                sr_value = int(entry[stat_key])
                right_text = f"{sr_value}"
                draw.text((x + 22, row_y), left_text, fill=(230, 235, 245), font=row_font)
                draw.text(
                    (x + panel_w - 22 - _text_width(right_text, value_font), row_y),
                    right_text,
                    fill=(120, 205, 255),
                    font=value_font,
                )
                row_y += row_step

        output = BytesIO()
        image.save(output, format="PNG")
        output.seek(0)
        return output

    async def _resolve_leaderboard_channel(self) -> discord.TextChannel | None:
        channel_id = self.bot.settings.leaderboard_channel_id
        if not channel_id:
            return None
        channel = self.bot.get_channel(channel_id)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(channel_id)
            except discord.DiscordException:
                logger.warning("Unable to fetch leaderboard channel %s", channel_id)
                return None
        if isinstance(channel, discord.TextChannel):
            return channel
        logger.warning("Configured leaderboard channel is not a text channel: %s", channel_id)
        return None

    async def _find_existing_leaderboard_message(self, channel: discord.TextChannel) -> discord.Message | None:
        if self._leaderboard_message_id:
            try:
                message = await channel.fetch_message(self._leaderboard_message_id)
                if self.bot.user and message.author.id == self.bot.user.id:
                    return message
            except discord.DiscordException:
                self._leaderboard_message_id = None

        if self.bot.user is None:
            return None
        try:
            async for message in channel.history(limit=200):
                if message.author.id != self.bot.user.id:
                    continue
                if message.content.startswith("In-House Leaderboard"):
                    self._leaderboard_message_id = message.id
                    return message
        except discord.DiscordException:
            return None
        return None

    async def sync_leaderboard_image(self, *, force: bool = False) -> None:
        channel = await self._resolve_leaderboard_channel()
        if channel is None:
            return

        existing = await self._find_existing_leaderboard_message(channel)
        if existing is not None and not force:
            return

        payload = self._render_leaderboard_image(limit=10)
        if existing is not None:
            try:
                await existing.delete()
            except discord.DiscordException:
                pass

        try:
            message = await channel.send(
                content="In-House Leaderboard",
                file=discord.File(fp=payload, filename="leaderboard.png"),
            )
            self._leaderboard_message_id = message.id
        except discord.DiscordException as exc:
            logger.warning("Unable to post leaderboard image: %s", exc)

    def _seed_test_roles(self, roles: list[str]) -> int:
        created = 0
        for index, role in enumerate(roles, start=1):
            discord_id, display_name = self._next_test_player(role)
            mmr = 2200 + ((index * 53) % 700)
            self.bot.db.upsert_player(
                discord_id=discord_id,
                display_name=display_name,
                mmr=mmr,
                preferred_role=role if role in {"tank", "dps", "support", "fill"} else "fill",
            )
            self.bot.db.upsert_queue_entry(discord_id, role)
            created += 1
        return created

    def _scenario_standard_roles(self, config: QueueConfig) -> list[str]:
        roles: list[str] = []
        roles.extend(["tank"] * (config.tank_per_team * 2))
        roles.extend(["dps"] * (config.dps_per_team * 2))
        roles.extend(["support"] * (config.support_per_team * 2))
        if len(roles) < config.players_per_match:
            roles.extend(["fill"] * (config.players_per_match - len(roles)))
        return roles[: config.players_per_match]

    def _build_test_scenario(self, config: QueueConfig, scenario: str) -> tuple[str, list[str]]:
        standard_roles = self._scenario_standard_roles(config)

        if scenario == "role_standard":
            return "role", standard_roles

        if scenario == "role_with_fill":
            roles = list(standard_roles)
            for preferred in ("tank", "dps", "support"):
                if preferred in roles:
                    roles[roles.index(preferred)] = "fill"
                    break
            return "role", roles

        if scenario == "role_all_fill":
            return "role", ["fill"] * config.players_per_match

        if scenario == "role_fallback":
            dps_count = config.players_per_match // 2
            support_count = config.players_per_match - dps_count
            return "role", (["dps"] * dps_count) + (["support"] * support_count)

        if scenario == "role_partial":
            target = max(config.players_per_match - 1, 1)
            return "role", standard_roles[:target]

        if scenario == "open_full":
            return "open", ["open"] * config.players_per_match

        raise ValueError("Unknown test scenario.")

    def build_embed(self, config: QueueConfig) -> discord.Embed:
        queued = self.bot.db.list_queue()
        total = len(queued)
        queue_mode_label = "Role Queue" if config.queue_mode == "role" else "Open Queue"
        embed = discord.Embed(
            title="In-House Queue",
            description=f"`{queue_mode_label}` • `{total}/{config.players_per_match}` queued",
            color=discord.Color.blue(),
        )

        if config.queue_mode == "role":
            role_groups: dict[str, list[QueuedPlayer]] = {"tank": [], "dps": [], "support": [], "fill": []}
            for player in queued:
                role = player.role if player.role in role_groups else "fill"
                role_groups[role].append(player)

            caps = config.role_caps_total()

            embed.add_field(
                name=f"Tank ({len(role_groups['tank'])}/{caps.get('tank', 0)})",
                value=self._mention_list(role_groups["tank"]),
                inline=False,
            )
            embed.add_field(
                name=f"DPS ({len(role_groups['dps'])}/{caps.get('dps', 0)})",
                value=self._mention_list(role_groups["dps"]),
                inline=False,
            )
            embed.add_field(
                name=f"Support ({len(role_groups['support'])}/{caps.get('support', 0)})",
                value=self._mention_list(role_groups["support"]),
                inline=False,
            )
            embed.add_field(
                name=f"Fill ({len(role_groups['fill'])}/{caps.get('fill', config.players_per_match)})",
                value=self._mention_list(role_groups["fill"]),
                inline=False,
            )
        else:
            embed.add_field(name="Queued Players", value=self._mention_list(queued), inline=False)

        return embed

    async def _sync_active_match_message(self) -> None:
        active = self.bot.db.get_active_match()
        if active is None:
            return
        channel = await self._resolve_text_channel_by_id(active.channel_id)
        if channel is None:
            return
        embed = await self._build_active_match_embed(active.match_id)
        if embed is None:
            return
        reports_locked = self._reports_complete(active.match_id) or active.status not in {"live", "disputed"}
        captain_claim_enabled = active.status in {"live", "disputed"} and self.bot.db.get_match_captain(active.match_id) is None
        view = ActiveMatchView(
            self.bot,
            reports_locked=reports_locked,
            captain_claim_enabled=captain_claim_enabled,
        )

        try:
            message = await channel.fetch_message(active.message_id)
            await message.edit(embed=embed, view=view)
        except discord.NotFound:
            content = "Active match panel"
            message = await channel.send(content=content, embed=embed, view=view)
            self.bot.db.update_active_match(message_id=message.id)
        except discord.DiscordException as exc:
            logger.warning("Unable to sync active match message: %s", exc)

    async def resume_active_match(self) -> None:
        active = self.bot.db.get_active_match()
        if active is None:
            return
        if active.match_id not in self._vc_check_status:
            teams = self.bot.db.get_match_teams(active.match_id)
            if teams is not None:
                team_a_payload, team_b_payload = teams
                self._vc_check_status[active.match_id] = {
                    "state": ("pending" if active.status == "waiting_vc" else "unavailable"),
                    "team_a_total": len(self._team_ids_from_payload(team_a_payload)),
                    "team_b_total": len(self._team_ids_from_payload(team_b_payload)),
                    "team_a_missing": [],
                    "team_b_missing": [],
                    "team_a_disconnected": 0,
                    "team_b_disconnected": 0,
                }
        await self._sync_active_match_message()

    def _cancel_ready_task(self, match_id: int) -> None:
        task = self._ready_check_tasks.pop(match_id, None)
        if task and not task.done():
            task.cancel()

    def _schedule_ready_check(self, match_id: int, delay_seconds: float) -> None:
        self._cancel_ready_task(match_id)
        self._ready_check_tasks[match_id] = asyncio.create_task(
            self._run_ready_check_after_delay(match_id, max(delay_seconds, 0.0))
        )

    async def _run_ready_check_after_delay(self, match_id: int, delay_seconds: float) -> None:
        try:
            await asyncio.sleep(delay_seconds)
            async with self.lock:
                await self._run_ready_check(match_id, force_start=True)
        except asyncio.CancelledError:
            return

    async def _run_ready_check(
        self,
        match_id: int,
        *,
        assume_test_players_ready: bool = False,
        force_start: bool = False,
    ) -> bool:
        active = self.bot.db.get_active_match()
        if active is None or active.match_id != match_id or active.status != "waiting_vc":
            return False

        channel = await self._resolve_text_channel_by_id(active.channel_id)
        if channel is None:
            return False
        guild = channel.guild

        teams = self.bot.db.get_match_teams(match_id)
        if teams is None:
            return False
        team_a_payload, team_b_payload = teams
        team_a_ids = self._team_ids_from_payload(team_a_payload)
        team_b_ids = self._team_ids_from_payload(team_b_payload)
        all_ids = team_a_ids + team_b_ids
        ready_ids = set(self.bot.db.list_match_ready_ids(match_id))
        if assume_test_players_ready:
            for discord_id in all_ids:
                if self._is_synthetic_player(discord_id):
                    # Persist synthetic readiness so embed/state checks stay consistent.
                    self.bot.db.set_match_ready(match_id, discord_id)
                    ready_ids.add(discord_id)

        config = self.bot.db.get_queue_config()
        moved_a = await self._move_members_from_main_vc(
            guild,
            team_a_ids,
            config.main_voice_channel_id,
            active.team_a_voice_channel_id,
        )
        moved_b = await self._move_members_from_main_vc(
            guild,
            team_b_ids,
            config.main_voice_channel_id,
            active.team_b_voice_channel_id,
        )

        wrong_team_a: list[int] = []
        wrong_team_b: list[int] = []
        disconnected_team_a: list[int] = []
        disconnected_team_b: list[int] = []
        if active.team_a_voice_channel_id:
            for pid in team_a_ids:
                member = await self._safe_fetch_member(guild, pid)
                if member is None or member.voice is None or member.voice.channel is None:
                    if assume_test_players_ready and self._is_synthetic_player(pid):
                        continue
                    disconnected_team_a.append(pid)
                    wrong_team_a.append(pid)
                    continue
                if member.voice.channel.id != active.team_a_voice_channel_id:
                    if assume_test_players_ready and self._is_synthetic_player(pid):
                        continue
                    wrong_team_a.append(pid)
        if active.team_b_voice_channel_id:
            for pid in team_b_ids:
                member = await self._safe_fetch_member(guild, pid)
                if member is None or member.voice is None or member.voice.channel is None:
                    if assume_test_players_ready and self._is_synthetic_player(pid):
                        continue
                    disconnected_team_b.append(pid)
                    wrong_team_b.append(pid)
                    continue
                if member.voice.channel.id != active.team_b_voice_channel_id:
                    if assume_test_players_ready and self._is_synthetic_player(pid):
                        continue
                    wrong_team_b.append(pid)

        self._vc_check_status[match_id] = {
            "state": "checked",
            "team_a_total": len(team_a_ids),
            "team_b_total": len(team_b_ids),
            "team_a_missing": list(wrong_team_a),
            "team_b_missing": list(wrong_team_b),
            "team_a_disconnected": len(disconnected_team_a),
            "team_b_disconnected": len(disconnected_team_b),
            "moved_a": moved_a,
            "moved_b": moved_b,
        }

        missing_ready = [
            discord_id
            for discord_id in all_ids
            if discord_id not in ready_ids
            and not (assume_test_players_ready and self._is_synthetic_player(discord_id))
        ]

        started_at = _utc_now_iso()

        readiness_lines: list[str] = []
        if missing_ready:
            readiness_lines.append(
                f"Ready pending: `{len(missing_ready)}` ({self._summarize_missing_mentions(missing_ready, limit=6)})"
            )
        if moved_a or moved_b:
            readiness_lines.append(f"Auto-moved from main VC: Team A `{moved_a}`, Team B `{moved_b}`.")
        if wrong_team_a:
            readiness_lines.append(f"Team A not in VC: {' '.join(f'<@{pid}>' for pid in wrong_team_a)}")
        if wrong_team_b:
            readiness_lines.append(f"Team B not in VC: {' '.join(f'<@{pid}>' for pid in wrong_team_b)}")
        if assume_test_players_ready:
            readiness_lines.append("Synthetic test players were treated as VC-ready.")
        can_start = not missing_ready and not wrong_team_a and not wrong_team_b
        if force_start:
            can_start = True

        if can_start:
            self.bot.db.increment_player_reliability(
                no_show_ids=wrong_team_a + wrong_team_b,
                disconnect_ids=disconnected_team_a + disconnected_team_b,
            )
            self.bot.db.update_active_match(status="live", started_at=started_at, ready_deadline=None)
            auto_captain_id = await self._auto_assign_admin_captain(match_id, guild, all_ids)
            if not readiness_lines:
                readiness_lines.append("All players are in their assigned voice channels.")
            if auto_captain_id is not None:
                readiness_lines.append(f"Admin in lobby detected. Lobby captain: <@{auto_captain_id}>.")
            else:
                readiness_lines.append("Captain selection is open. Press `Claim Captain`.")
            self._active_match_updates[match_id] = "Match is now live.\n" + "\n".join(readiness_lines)
            await self._sync_active_match_message()
            return True

        if not readiness_lines:
            readiness_lines.append("Waiting for voice channel checks.")
        self._active_match_updates[match_id] = "\n".join(readiness_lines)
        await self._sync_active_match_message()
        return False

    def _mmr_change_block(self, changes: list[MatchMmrChange], team: str) -> str:
        team_changes = [change for change in changes if change.team == team]
        if not team_changes:
            return "None"
        lines = [
            (
                f"<@{change.discord_id}> `{change.mmr_before}` -> `{change.mmr_after}` "
                f"(`{_format_delta(change.delta)}`)"
            )
            for change in team_changes
        ]
        return "\n".join(lines)

    def _build_match_result_embed(
        self,
        *,
        match_id: int,
        winner_team: str,
        started_at: str | None,
        finished_at: str,
        changes: list[MatchMmrChange],
    ) -> discord.Embed:
        color = discord.Color.blurple() if winner_team == "Draw" else discord.Color.green()
        team_a_changes = [change for change in changes if change.team == "Team A"]
        team_b_changes = [change for change in changes if change.team == "Team B"]
        team_a_total_delta = sum(change.delta for change in team_a_changes)
        team_b_total_delta = sum(change.delta for change in team_b_changes)
        embed = discord.Embed(
            title=f"Match #{match_id} Completed",
            description=f"Result: `{winner_team}`",
            color=color,
        )
        embed.timestamp = datetime.now(timezone.utc)
        embed.add_field(
            name="Summary",
            value=(
                f"Team A total delta: `{_format_delta(team_a_total_delta)}`\n"
                f"Team B total delta: `{_format_delta(team_b_total_delta)}`"
            ),
            inline=False,
        )
        if started_at:
            embed.add_field(name="Started", value=_discord_ts(started_at), inline=True)
        embed.add_field(name="Finished", value=_discord_ts(finished_at), inline=True)
        embed.add_field(name="Team A MMR", value=self._mmr_change_block(changes, "Team A"), inline=False)
        embed.add_field(name="Team B MMR", value=self._mmr_change_block(changes, "Team B"), inline=False)
        return embed

    def _normalize_queue_role_for_mode(self, queue_mode: str, preferred_role: str | None) -> str:
        if queue_mode == "open":
            return "open"
        candidate = (preferred_role or "fill").strip().lower()
        if candidate in {"tank", "dps", "support", "fill"}:
            return candidate
        return "fill"

    def _build_match_cancelled_embed(
        self,
        *,
        match_id: int,
        started_at: str | None,
        requeued_count: int,
        reason: str,
    ) -> discord.Embed:
        embed = discord.Embed(
            title=f"Match #{match_id} Cancelled",
            description=reason,
            color=discord.Color.dark_red(),
        )
        embed.timestamp = datetime.now(timezone.utc)
        if started_at:
            embed.add_field(name="Started", value=_discord_ts(started_at), inline=True)
        embed.add_field(name="Players Requeued", value=f"`{requeued_count}`", inline=True)
        embed.add_field(name="Ended", value=_discord_ts(_utc_now_iso()), inline=True)
        return embed

    async def admin_cancel_active_match(
        self,
        *,
        requeue_players: bool,
        remake_immediately: bool,
    ) -> tuple[bool, str]:
        async with self.lock:
            active = self.bot.db.get_active_match()
            if active is None:
                return False, "No active match to cancel."

            teams = self.bot.db.get_match_teams(active.match_id)
            requeued = 0
            if requeue_players and teams is not None:
                queue_mode = self.bot.db.get_queue_config().queue_mode
                for payload in teams:
                    for entry in payload:
                        try:
                            discord_id = int(entry.get("discord_id", 0))
                        except (TypeError, ValueError):
                            continue
                        if discord_id <= 0:
                            continue
                        display_name = str(entry.get("display_name", f"User {discord_id}"))
                        preferred_role = str(entry.get("preferred_role", "fill"))
                        try:
                            mmr_value = int(entry.get("mmr", self.bot.settings.default_mmr))
                        except (TypeError, ValueError):
                            mmr_value = self.bot.settings.default_mmr
                        self.bot.db.upsert_player(
                            discord_id=discord_id,
                            display_name=display_name,
                            mmr=mmr_value,
                            preferred_role=preferred_role,
                        )
                        target_role = self._normalize_queue_role_for_mode(queue_mode, preferred_role)
                        changed, _ = self.bot.db.upsert_queue_entry(discord_id, target_role)
                        if changed:
                            requeued += 1

            self._cancel_ready_task(active.match_id)
            self.bot.db.clear_match_reports(active.match_id)
            self.bot.db.clear_match_ready(active.match_id)
            self.bot.db.clear_match_captain(active.match_id)
            self._active_match_updates.pop(active.match_id, None)
            self._vc_check_status.pop(active.match_id, None)
            self.bot.db.clear_active_match()

            channel = await self._resolve_text_channel_by_id(active.channel_id)
            if channel:
                reason = "Cancelled by admin."
                if remake_immediately:
                    reason = "Cancelled by admin. Players requeued for remake."
                elif requeue_players:
                    reason = "Cancelled by admin. Players returned to queue."
                cancelled_embed = self._build_match_cancelled_embed(
                    match_id=active.match_id,
                    started_at=active.started_at,
                    requeued_count=requeued,
                    reason=reason,
                )
                try:
                    message = await channel.fetch_message(active.message_id)
                    await message.edit(content=None, embed=cancelled_embed, view=None)
                except discord.NotFound:
                    await channel.send(embed=cancelled_embed)
                except discord.DiscordException:
                    await channel.send(embed=cancelled_embed)

            if remake_immediately:
                await self._start_match_if_ready()
            if self.bot.db.get_active_match() is None:
                await self.sync_panel(repost=True)
            else:
                await self.sync_panel()

            if remake_immediately:
                return True, f"Match #{active.match_id} cancelled and remake attempted."
            if requeue_players:
                return True, f"Match #{active.match_id} cancelled and `{requeued}` players requeued."
            return True, f"Match #{active.match_id} cancelled."

    async def _finalize_active_match(self, match_id: int, winner_team: str, *, save_result: bool = True) -> None:
        active = self.bot.db.get_active_match()
        if active is None or active.match_id != match_id:
            return
        self._cancel_ready_task(match_id)
        if save_result:
            changed, msg = self.bot.db.set_match_result(match_id, winner_team)
            if not changed and msg not in {"result already set to that value"}:
                logger.warning("Failed to save result for match %s: %s", match_id, msg)

        applied, mmr_changes, mmr_msg = self.bot.db.apply_match_mmr_changes(match_id, winner_team)
        if not applied and mmr_msg != "mmr already applied":
            logger.warning("Failed to apply MMR for match %s: %s", match_id, mmr_msg)

        if not mmr_changes:
            mmr_changes = self.bot.db.get_match_mmr_changes(match_id)

        finished_at = _utc_now_iso()
        result_embed = self._build_match_result_embed(
            match_id=match_id,
            winner_team=winner_team,
            started_at=active.started_at,
            finished_at=finished_at,
            changes=mmr_changes,
        )

        channel = await self._resolve_text_channel_by_id(active.channel_id)
        if channel:
            try:
                message = await channel.fetch_message(active.message_id)
                await message.edit(content=None, embed=result_embed, view=MatchResultView(self.bot))
            except discord.NotFound:
                await channel.send(embed=result_embed, view=MatchResultView(self.bot))
            except discord.DiscordException:
                await channel.send(embed=result_embed, view=MatchResultView(self.bot))

            # QoL move: best-effort return match players from team VCs to main VC after result.
            main_voice_channel_id = self.bot.db.get_queue_config().main_voice_channel_id
            if main_voice_channel_id:
                try:
                    teams = self.bot.db.get_match_teams(match_id)
                    if teams is not None:
                        team_a_payload, team_b_payload = teams
                        team_a_ids = self._team_ids_from_payload(team_a_payload)
                        team_b_ids = self._team_ids_from_payload(team_b_payload)
                        await self._move_members_from_main_vc(
                            channel.guild,
                            team_a_ids,
                            active.team_a_voice_channel_id,
                            main_voice_channel_id,
                        )
                        await self._move_members_from_main_vc(
                            channel.guild,
                            team_b_ids,
                            active.team_b_voice_channel_id,
                            main_voice_channel_id,
                        )
                except Exception:
                    pass

        self.bot.db.clear_match_reports(match_id)
        self.bot.db.clear_match_ready(match_id)
        self.bot.db.clear_match_captain(match_id)
        self._active_match_updates.pop(match_id, None)
        self._vc_check_status.pop(match_id, None)
        self.bot.db.clear_active_match()
        await self.sync_leaderboard_image(force=True)
        await self._start_match_if_ready()
        if self.bot.db.get_active_match() is None:
            await self.sync_panel(repost=True)
        else:
            await self.sync_panel()

    async def handle_match_report(self, interaction: discord.Interaction, report_type: str) -> None:
        await interaction.response.defer(ephemeral=True, thinking=False)
        async with self.lock:
            active = self.bot.db.get_active_match()
            if active is None:
                await interaction.followup.send("There is no active match.", ephemeral=True)
                return
            if active.status not in {"live", "disputed"}:
                await interaction.followup.send("Result reporting is available after match start.", ephemeral=True)
                return

            player_team = self.bot.db.get_player_team_for_match(active.match_id, interaction.user.id)
            if player_team is None:
                await interaction.followup.send("You are not part of the active match.", ephemeral=True)
                return

            if report_type == "win":
                reported_winner = player_team
            elif report_type == "loss":
                reported_winner = "Team B" if player_team == "Team A" else "Team A"
            else:
                await interaction.followup.send("Invalid report type.", ephemeral=True)
                return

            changed, msg = self.bot.db.upsert_match_report(
                match_id=active.match_id,
                team=player_team,
                reported_winner_team=reported_winner,
                reporter_id=interaction.user.id,
            )
            if not changed:
                await interaction.followup.send(msg, ephemeral=True)
                return

            await self._finalize_active_match(active.match_id, reported_winner)
            await interaction.followup.send(
                (
                    f"Result submitted. Match finalized as `{reported_winner}`. "
                    "If this is incorrect, press `Dispute Winner` on the result message."
                ),
                ephemeral=True,
            )

    async def handle_match_escalation(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True, thinking=False)
        async with self.lock:
            active = self.bot.db.get_active_match()
            if active is None:
                await interaction.followup.send("There is no active match.", ephemeral=True)
                return
            player_team = self.bot.db.get_player_team_for_match(active.match_id, interaction.user.id)
            if player_team is None and not _is_admin(interaction):
                await interaction.followup.send("Only match players or admins can escalate.", ephemeral=True)
                return

            self.bot.db.update_active_match(status="disputed", escalated=True)
            self._active_match_updates[active.match_id] = (
                f"Dispute escalated by <@{interaction.user.id}>. Waiting for admin result."
            )
            await self._sync_active_match_message()
            await interaction.followup.send("Dispute escalated to admins.", ephemeral=True)

    async def handle_result_dispute(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True, thinking=False)
        async with self.lock:
            match_id = self._match_id_from_result_message(interaction.message)
            if match_id is None:
                await interaction.followup.send("Could not determine match ID from this result message.", ephemeral=True)
                return

            player_team = self.bot.db.get_player_team_for_match(match_id, interaction.user.id)
            if player_team is None and not _is_admin(interaction):
                await interaction.followup.send("Only match players or admins can dispute this winner.", ephemeral=True)
                return

            message = interaction.message
            if message is None or not message.embeds:
                await interaction.followup.send("Result message is unavailable.", ephemeral=True)
                return

            updated_embed = discord.Embed.from_dict(message.embeds[0].to_dict())
            for field in updated_embed.fields:
                if field.name == "Dispute":
                    await interaction.followup.send(
                        f"Dispute is already open for match `{match_id}`. Admins can resolve with `/match_result`.",
                        ephemeral=True,
                    )
                    return

            updated_embed.add_field(
                name="Dispute",
                value=(
                    f"Opened by <@{interaction.user.id}> at {_discord_ts(_utc_now_iso())}.\n"
                    f"Admins can resolve with `/match_result` using match ID `{match_id}`."
                ),
                inline=False,
            )
            try:
                await message.edit(embed=updated_embed, view=MatchResultView(self.bot))
            except discord.DiscordException:
                pass

            await interaction.followup.send(
                f"Dispute opened for match `{match_id}`. Admins have been signaled on the result embed.",
                ephemeral=True,
            )

    async def sync_panel(self, *, repost: bool = False) -> None:
        config = self.bot.db.get_queue_config()
        channel = await self.resolve_queue_channel(config)
        if channel is None:
            return

        embed = self.build_embed(config)
        view = QueuePanelView(self.bot, config)

        message_id = config.queue_message_id or 0
        if repost:
            self._reposting = True
            if message_id > 0:
                try:
                    old_message = await channel.fetch_message(message_id)
                    await old_message.delete()
                except discord.DiscordException:
                    pass
            try:
                message = await channel.send(embed=embed, view=view)
                self.bot.db.set_queue_message(message.id)
            finally:
                self._reposting = False
            return

        if message_id <= 0:
            message = await channel.send(embed=embed, view=view)
            self.bot.db.set_queue_message(message.id)
            return

        try:
            message = await channel.fetch_message(message_id)
            await message.edit(content=None, embed=embed, view=view)
        except discord.NotFound:
            message = await channel.send(embed=embed, view=view)
            self.bot.db.set_queue_message(message.id)
        except discord.DiscordException as exc:
            logger.warning("Unable to sync queue panel: %s", exc)

    def _role_quota_for_match(self, config: QueueConfig) -> dict[str, int]:
        return {
            "tank": config.tank_per_team,
            "dps": config.dps_per_team,
            "support": config.support_per_team,
        }

    def _compose_match_message(self, match_id: int, config: QueueConfig, team_a: Team, team_b: Team, roles_enforced: bool) -> str:
        roles_note = "role constraints enabled" if roles_enforced else "role constraints disabled (fallback)"
        return (
            f"**Match #{match_id} created** (`{config.queue_mode}` - {roles_note})\n\n"
            f"**{team_a.name}**\n{_format_team(team_a)}\n\n"
            f"**{team_b.name}**\n{_format_team(team_b)}"
        )

    async def _start_match_if_ready(self) -> None:
        active = self.bot.db.get_active_match()
        if active is not None:
            return

        config = self.bot.db.get_queue_config()
        queued = self.bot.db.list_queue()
        if len(queued) < config.players_per_match:
            return

        selected = queued[: config.players_per_match]
        enforce_roles = config.queue_mode == "role"
        role_quota = self._role_quota_for_match(config) if enforce_roles else None

        try:
            result = make_match(
                selected,
                enforce_roles=enforce_roles,
                role_quota_per_team=role_quota,
            )
        except Exception as exc:  # pragma: no cover - runtime guard
            logger.exception("Failed to create a match: %s", exc)
            return

        player_ids = [player.discord_id for player in selected]
        self.bot.db.dequeue_many(player_ids)
        match_id = self.bot.db.record_match(
            mode=config.queue_mode,
            team_a=result.team_a,
            team_b=result.team_b,
            roles_enforced=result.roles_enforced,
        )

        channel = await self.resolve_queue_channel(config)
        if channel is None:
            return

        team_a_ids = [player.discord_id for player in result.team_a.players]
        team_b_ids = [player.discord_id for player in result.team_b.players]
        mentions = self._active_match_mentions(team_a_ids, team_b_ids)

        moved_a = await self._move_members_from_main_vc(
            channel.guild,
            team_a_ids,
            config.main_voice_channel_id,
            config.team_a_voice_channel_id,
        )
        moved_b = await self._move_members_from_main_vc(
            channel.guild,
            team_b_ids,
            config.main_voice_channel_id,
            config.team_b_voice_channel_id,
        )
        move_note = f"Auto-move attempted: Team A `{moved_a}`, Team B `{moved_b}`."

        if config.main_voice_channel_id:
            ready_prompt = (
                f"Match #{match_id} formed. Join <#{config.main_voice_channel_id}> or your team VC.\n{move_note}"
            )
        else:
            ready_prompt = f"Match #{match_id} formed. Join your team VC.\n{move_note}"

        message = await channel.send(f"{mentions}\n{ready_prompt}")
        self.bot.db.set_active_match(
            match_id=match_id,
            channel_id=channel.id,
            message_id=message.id,
            status="live",
            ready_deadline=None,
            started_at=_utc_now_iso(),
            team_a_voice_channel_id=config.team_a_voice_channel_id,
            team_b_voice_channel_id=config.team_b_voice_channel_id,
            escalated=False,
        )
        self._vc_check_status[match_id] = {
            "state": "unavailable",
            "team_a_total": len(team_a_ids),
            "team_b_total": len(team_b_ids),
            "team_a_missing": [],
            "team_b_missing": [],
            "team_a_disconnected": 0,
            "team_b_disconnected": 0,
            "moved_a": moved_a,
            "moved_b": moved_b,
        }
        self.bot.db.clear_match_ready(match_id)
        self.bot.db.clear_match_captain(match_id)
        auto_captain_id = await self._auto_assign_admin_captain(match_id, channel.guild, team_a_ids + team_b_ids)
        if auto_captain_id is not None:
            self._active_match_updates[match_id] = (
                "Match is now live.\n"
                f"{move_note}\n"
                f"Admin captain auto-selected: <@{auto_captain_id}>."
            )
        else:
            self._active_match_updates[match_id] = (
                "Match is now live.\n"
                f"{move_note}\n"
                "First player to press `Claim Captain` becomes captain."
            )
        self.bot.db.clear_match_reports(match_id)
        await self._sync_active_match_message()

    async def _handle_join_core(
        self,
        interaction: discord.Interaction,
        *,
        requested_role: str,
        battletag: str | None = None,
    ) -> None:
        await interaction.response.defer(ephemeral=True, thinking=False)
        async with self.lock:
            config = self.bot.db.get_queue_config()
            if not config.queue_channel_id:
                await interaction.followup.send("Queue channel is not configured.", ephemeral=True)
                return

            if interaction.channel_id != config.queue_channel_id:
                await interaction.followup.send(
                    f"Use the queue panel in <#{config.queue_channel_id}>.",
                    ephemeral=True,
                )
                return

            user_id = interaction.user.id
            normalized_battletag = None
            if battletag is not None:
                trimmed = battletag.strip()
                normalized_battletag = trimmed if trimmed else None
            self.bot.db.upsert_player(
                discord_id=user_id,
                display_name=interaction.user.display_name,
                battletag=normalized_battletag,
            )

            target_role = "open" if config.queue_mode == "open" else requested_role
            if config.queue_mode == "role":
                if target_role not in {"tank", "dps", "support", "fill"}:
                    await interaction.followup.send("Invalid role selection.", ephemeral=True)
                    return
            else:
                target_role = "open"

            existing = self.bot.db.get_queue_entry(user_id)
            if existing is None and self.bot.db.queue_count() >= config.players_per_match:
                await interaction.followup.send("Queue is currently full.", ephemeral=True)
                return

            if config.queue_mode == "role":
                if existing is None or existing["role"] != target_role:
                    role_cap = config.role_caps_total().get(target_role, 0)
                    role_count = self.bot.db.count_role(target_role)
                    if role_count >= role_cap:
                        await interaction.followup.send(
                            f"{target_role.title()} is currently full.",
                            ephemeral=True,
                        )
                        return

            changed, status = self.bot.db.upsert_queue_entry(user_id, target_role)
            if not changed:
                await interaction.followup.send("You are already queued in that role.", ephemeral=True)
                return

            if status == "role updated":
                response_text = f"Queue role updated to `{target_role}`."
            else:
                response_text = f"You joined the queue as `{target_role}`."

            await self._start_match_if_ready()
            await self.sync_panel()
            await interaction.followup.send(response_text, ephemeral=True)

    async def handle_join_after_battletag(
        self,
        interaction: discord.Interaction,
        *,
        requested_role: str,
        battletag: str,
    ) -> None:
        self._cancel_battletag_reminder(interaction.user.id)
        await self._handle_join_core(
            interaction,
            requested_role=requested_role,
            battletag=battletag,
        )

    async def handle_join(self, interaction: discord.Interaction, requested_role: str) -> None:
        player = self.bot.db.get_player(interaction.user.id)
        if player is None or not player.battletag:
            self._cancel_battletag_reminder(interaction.user.id)
            await interaction.response.send_modal(BattleTagModal(self.bot, requested_role))
            self._battletag_reminder_tasks[interaction.user.id] = asyncio.create_task(
                self._send_battletag_reminder(interaction, interaction.user.id)
            )
            return
        await self._handle_join_core(interaction, requested_role=requested_role)

    async def handle_claim_captain(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True, thinking=False)
        async with self.lock:
            active = self.bot.db.get_active_match()
            if active is None:
                await interaction.followup.send("There is no active match.", ephemeral=True)
                return
            if active.status not in {"live", "disputed"}:
                await interaction.followup.send("Captain selection opens after match start.", ephemeral=True)
                return

            captain = self.bot.db.get_match_captain(active.match_id)
            if captain is not None:
                await interaction.followup.send(f"Lobby captain is already <@{captain.captain_id}>.", ephemeral=True)
                return

            player_ids = self._match_player_ids_from_db(active.match_id)
            if interaction.user.id not in player_ids:
                await interaction.followup.send("Only match players can claim captain.", ephemeral=True)
                return

            channel = await self._resolve_text_channel_by_id(active.channel_id)
            if channel is None:
                await interaction.followup.send("Match channel is unavailable.", ephemeral=True)
                return

            auto_admin_captain = await self._auto_assign_admin_captain(active.match_id, channel.guild, player_ids)
            if auto_admin_captain is not None:
                self._active_match_updates[active.match_id] = f"Admin captain auto-selected: <@{auto_admin_captain}>."
                await self._sync_active_match_message()
                await interaction.followup.send(
                    f"Admin in lobby takes priority. Lobby captain: <@{auto_admin_captain}>.",
                    ephemeral=True,
                )
                return

            changed, message = self.bot.db.set_match_captain(
                match_id=active.match_id,
                captain_id=interaction.user.id,
                selected_by=interaction.user.id,
                selection_method="first_claim",
            )
            if not changed:
                existing = self.bot.db.get_match_captain(active.match_id)
                if existing is not None:
                    await interaction.followup.send(
                        f"Captain already selected: <@{existing.captain_id}>.",
                        ephemeral=True,
                    )
                else:
                    await interaction.followup.send(f"Unable to set captain: {message}.", ephemeral=True)
                return

            self._active_match_updates[active.match_id] = f"Lobby captain selected: <@{interaction.user.id}>."
            await self._sync_active_match_message()
            await interaction.followup.send("You are now the lobby captain.", ephemeral=True)

    async def handle_leave(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True, thinking=False)
        async with self.lock:
            left = self.bot.db.remove_queue_entry(interaction.user.id)
            if not left:
                await interaction.followup.send("You are not currently in queue.", ephemeral=True)
                return
            await self.sync_panel()
            await interaction.followup.send("You have been removed from queue.", ephemeral=True)

    async def admin_remove_player(self, player_id: int) -> bool:
        async with self.lock:
            removed = self.bot.db.remove_queue_entry(player_id)
            await self.sync_panel()
            return removed

    async def admin_force_result(self, match_id: int, winner_team: str) -> tuple[bool, str]:
        async with self.lock:
            previous_winner = self.bot.db.get_match_result(match_id)
            changed, msg = self.bot.db.set_match_result(match_id, winner_team)
            if not changed and msg != "result already set to that value":
                return False, f"Unable to set result: {msg}."

            active = self.bot.db.get_active_match()
            if active and active.match_id == match_id:
                await self._finalize_active_match(match_id, winner_team, save_result=False)
                return True, "Result saved and match finalized."

            applied, _, mmr_msg = self.bot.db.apply_match_mmr_changes(match_id, winner_team)
            mmr_note = ""
            if applied:
                await self.sync_leaderboard_image(force=True)
                mmr_note = "MMR updated."
            elif mmr_msg == "mmr already applied":
                if previous_winner is not None and previous_winner != winner_team:
                    corrected, _, correction_msg = self.bot.db.recompute_match_mmr_changes(match_id, winner_team)
                    if not corrected:
                        return True, f"Result saved, but MMR correction failed: {correction_msg}."
                    if correction_msg == "mmr corrected for updated result":
                        await self.sync_leaderboard_image(force=True)
                        mmr_note = "MMR corrected for updated result."
                    else:
                        mmr_note = "MMR already matched result."
                else:
                    mmr_note = "MMR already applied."
            else:
                return True, f"Result saved, but MMR update failed: {mmr_msg}."
            if active and active.match_id != match_id:
                if mmr_note:
                    return True, f"Result saved for archived match. {mmr_note} Active match is `#{active.match_id}`."
                return True, f"Result saved for archived match. Active match is `#{active.match_id}`."
            if mmr_note:
                return True, f"Result saved. {mmr_note}"
            return True, "Result saved."

    async def admin_force_vc_check(self, *, assume_test_players_ready: bool = True) -> tuple[bool, str]:
        async with self.lock:
            active = self.bot.db.get_active_match()
            if active is None:
                return False, "No active match."
            if active.status != "waiting_vc":
                return False, f"Active match is `{active.status}` (VC check already completed)."

            self._cancel_ready_task(active.match_id)
            started = await self._run_ready_check(
                active.match_id,
                assume_test_players_ready=assume_test_players_ready,
                force_start=True,
            )
            refreshed = self.bot.db.get_active_match()
            if started or (refreshed and refreshed.match_id == active.match_id and refreshed.status == "live"):
                return True, "VC check forced and match moved to live."
            return False, "Unable to complete VC check. Verify active match channel and team VC setup."

    async def admin_set_channel(self, channel_id: int) -> None:
        async with self.lock:
            self.bot.db.update_queue_config(queue_channel_id=channel_id, queue_message_id=0)
            await self.sync_panel(repost=True)

    async def admin_set_voice_channels(
        self,
        *,
        main_voice_channel_id: int | None = None,
        team_a_voice_channel_id: int | None = None,
        team_b_voice_channel_id: int | None = None,
    ) -> None:
        async with self.lock:
            self.bot.db.update_queue_config(
                main_voice_channel_id=main_voice_channel_id,
                team_a_voice_channel_id=team_a_voice_channel_id,
                team_b_voice_channel_id=team_b_voice_channel_id,
            )
            active = self.bot.db.get_active_match()
            if active and active.status == "waiting_vc":
                self.bot.db.update_active_match(
                    team_a_voice_channel_id=team_a_voice_channel_id,
                    team_b_voice_channel_id=team_b_voice_channel_id,
                )
                await self._sync_active_match_message()

    async def admin_set_team_vc_privacy(self, guild: discord.Guild, *, enabled: bool) -> tuple[bool, str]:
        async with self.lock:
            config = self.bot.db.get_queue_config()
            vc_ids = [config.team_a_voice_channel_id, config.team_b_voice_channel_id]
            resolved: list[discord.VoiceChannel] = []
            for channel_id in vc_ids:
                if not channel_id:
                    continue
                channel = guild.get_channel(channel_id)
                if channel is None:
                    try:
                        channel = await guild.fetch_channel(channel_id)
                    except discord.DiscordException:
                        continue
                if isinstance(channel, discord.VoiceChannel):
                    resolved.append(channel)

            if not resolved:
                return False, "No Team A/Team B voice channels are configured."

            updated = 0
            default_role = guild.default_role
            for voice_channel in resolved:
                overwrite = voice_channel.overwrites_for(default_role)
                overwrite.connect = False if enabled else None
                try:
                    await voice_channel.set_permissions(
                        default_role,
                        overwrite=overwrite,
                        reason=("Queue bot: team VC private mode" if enabled else "Queue bot: team VC public mode"),
                    )
                except discord.DiscordException:
                    continue
                updated += 1

            if updated <= 0:
                return False, "Unable to update team VC permissions."
            mode_label = "private" if enabled else "public"
            return True, f"Updated `{updated}` team VC(s) to `{mode_label}` mode."

    async def admin_set_mode(self, queue_mode: str) -> None:
        async with self.lock:
            self.bot.db.update_queue_config(queue_mode=queue_mode)
            if queue_mode == "open":
                self.bot.db.set_all_queue_roles("open")
            else:
                self.bot.db.normalize_queue_roles_for_role_mode()
            await self._start_match_if_ready()
            await self.sync_panel()

    async def admin_set_rules(
        self,
        players_per_match: int,
        tank_per_team: int,
        dps_per_team: int,
        support_per_team: int,
    ) -> None:
        async with self.lock:
            self.bot.db.update_queue_config(
                players_per_match=players_per_match,
                tank_per_team=tank_per_team,
                dps_per_team=dps_per_team,
                support_per_team=support_per_team,
            )
            await self._start_match_if_ready()
            await self.sync_panel()

    async def admin_clear_queue(self) -> int:
        async with self.lock:
            removed = self.bot.db.clear_queue()
            await self.sync_panel()
            return removed

    async def admin_seed_test_scenario(self, scenario: str) -> tuple[str, int, int]:
        async with self.lock:
            config = self.bot.db.get_queue_config()
            queue_mode, roles = self._build_test_scenario(config, scenario)
            self.bot.db.update_queue_config(queue_mode=queue_mode)
            self.bot.db.clear_queue()
            added = self._seed_test_roles(roles)
            await self._start_match_if_ready()
            remaining = self.bot.db.queue_count()
            await self.sync_panel()
            return queue_mode, added, remaining

    async def admin_add_test_players(self, role: str, count: int) -> tuple[str, int, int]:
        async with self.lock:
            config = self.bot.db.get_queue_config()
            queue_mode = config.queue_mode
            if queue_mode == "open":
                if role != "open":
                    raise ValueError("Current mode is open queue. Use role `open`.")
                target_role = "open"
            else:
                if role == "open":
                    raise ValueError("`open` test role can only be used in open queue mode.")
                target_role = role

            added = self._seed_test_roles([target_role] * count)
            await self._start_match_if_ready()
            remaining = self.bot.db.queue_count()
            await self.sync_panel()
            return target_role, added, remaining

    async def admin_apply_test_results(self, mode: str, count: int) -> tuple[int, int, int]:
        async with self.lock:
            if mode == "clear":
                cleared = self.bot.db.clear_match_results()
                return cleared, 0, 0

            matches = self.bot.db.list_recent_matches(limit=count)
            updated = 0
            mmr_applied = 0
            for index, match in enumerate(matches):
                if mode == "team_a":
                    winner = "Team A"
                elif mode == "team_b":
                    winner = "Team B"
                elif mode == "draw":
                    winner = "Draw"
                elif mode == "alternating":
                    winner = "Team A" if index % 2 == 0 else "Team B"
                else:
                    raise ValueError("Unknown test result mode.")

                match_id = int(match["id"])
                changed, msg = self.bot.db.set_match_result(match_id, winner)
                if changed:
                    updated += 1
                if changed or msg == "result already set to that value":
                    applied, _, _ = self.bot.db.apply_match_mmr_changes(match_id, winner)
                    if applied:
                        mmr_applied += 1
            if mmr_applied > 0:
                await self.sync_leaderboard_image(force=True)
            return updated, len(matches), mmr_applied

    async def handle_queue_channel_message(self, _message: discord.Message) -> None:
        if self._reposting:
            return
        async with self.lock:
            await self.sync_panel(repost=True)


class OverwatchBot(commands.Bot):
    def __init__(self, settings: Settings) -> None:
        intents = discord.Intents.default()
        super().__init__(command_prefix="!", intents=intents)

        self.settings = settings
        self.db = Database(
            path=settings.database_path,
            default_mmr=settings.default_mmr,
            default_role=settings.default_role,
            default_players_per_match=settings.players_per_match,
            default_tank_per_team=settings.tank_per_team,
            default_dps_per_team=settings.dps_per_team,
            default_support_per_team=settings.support_per_team,
        )
        self.queue_service = QueueService(self)
        self.modmail_service = ModmailService(self)
        self._ready_once = False

        config = self.db.get_queue_config()
        queue_channel_id = config.queue_channel_id or settings.queue_channel_id
        main_vc_id = config.main_voice_channel_id or settings.main_voice_channel_id
        team_a_vc_id = config.team_a_voice_channel_id or settings.team_a_voice_channel_id
        team_b_vc_id = config.team_b_voice_channel_id or settings.team_b_voice_channel_id
        self.db.update_queue_config(
            queue_channel_id=queue_channel_id,
            queue_message_id=(config.queue_message_id if queue_channel_id == config.queue_channel_id else 0),
            main_voice_channel_id=main_vc_id,
            team_a_voice_channel_id=team_a_vc_id,
            team_b_voice_channel_id=team_b_vc_id,
        )
        modmail_config = self.db.get_modmail_config()
        modmail_channel_id = modmail_config.panel_channel_id or settings.modmail_channel_id
        modmail_logs_channel_id = modmail_config.logs_channel_id or settings.modmail_logs_channel_id
        self.db.update_modmail_config(
            panel_channel_id=modmail_channel_id,
            panel_message_id=(
                modmail_config.panel_message_id if modmail_channel_id == modmail_config.panel_channel_id else 0
            ),
            logs_channel_id=modmail_logs_channel_id,
        )

    async def setup_hook(self) -> None:
        register_commands(self)
        self.add_view(ModmailPanelView(self))
        self.add_view(TicketThreadView(self))
        self.add_view(MatchResultView(self))
        if self.settings.command_guild_id:
            guild = discord.Object(id=self.settings.command_guild_id)
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
            logger.info("Synced commands to guild %s", self.settings.command_guild_id)
        else:
            await self.tree.sync()
            logger.info("Synced global commands")

    async def on_ready(self) -> None:
        if self.user:
            logger.info("Connected as %s (%s)", self.user.name, self.user.id)
        if not self._ready_once:
            await self.queue_service.sync_panel()
            await self.modmail_service.sync_panel()
            await self.queue_service.resume_active_match()
            await self.queue_service.sync_leaderboard_image()
            self._ready_once = True

    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot:
            return
        config = self.db.get_queue_config()
        if config.queue_channel_id and message.channel.id == config.queue_channel_id:
            await self.queue_service.handle_queue_channel_message(message)
            return
        await self.process_commands(message)

    async def close(self) -> None:
        self.db.close()
        await super().close()


def register_commands(bot: OverwatchBot) -> None:
    @bot.tree.command(name="queue_channel", description="Set the queue channel and post the queue panel.")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(channel="Text channel for the queue panel")
    async def queue_admin_channel(interaction: discord.Interaction, channel: discord.TextChannel) -> None:
        if not _is_admin(interaction):
            await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=False)
        await bot.queue_service.admin_set_channel(channel.id)
        await interaction.followup.send(f"Queue channel set to {channel.mention}.", ephemeral=True)

    @bot.tree.command(name="modmail_channel", description="Set the modmail panel channel and post the embed.")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(channel="Text channel where modmail embed should live")
    async def modmail_channel(interaction: discord.Interaction, channel: discord.TextChannel) -> None:
        if not _is_admin(interaction):
            await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=False)
        await bot.modmail_service.admin_set_channel(channel.id)
        await interaction.followup.send(f"Modmail channel set to {channel.mention}.", ephemeral=True)

    @bot.tree.command(name="modmail_logs_channel", description="Set where closed ticket logs are posted.")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(channel="Text channel for ticket logs")
    async def modmail_logs_channel(interaction: discord.Interaction, channel: discord.TextChannel) -> None:
        if not _is_admin(interaction):
            await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=False)
        await bot.modmail_service.admin_set_logs_channel(channel.id)
        await interaction.followup.send(f"Modmail logs channel set to {channel.mention}.", ephemeral=True)

    @bot.tree.command(name="modmail_logs_channel_id", description="Set ticket logs channel by raw channel ID.")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(channel_id="Discord text channel ID for ticket logs")
    async def modmail_logs_channel_id(interaction: discord.Interaction, channel_id: str) -> None:
        if not _is_admin(interaction):
            await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
            return

        try:
            parsed_id = int(channel_id.strip())
        except ValueError:
            await interaction.response.send_message("`channel_id` must be a valid integer.", ephemeral=True)
            return

        channel = bot.get_channel(parsed_id)
        if channel is None:
            try:
                channel = await bot.fetch_channel(parsed_id)
            except discord.DiscordException:
                await interaction.response.send_message("I could not fetch that channel ID.", ephemeral=True)
                return
        if not isinstance(channel, discord.TextChannel):
            await interaction.response.send_message("That ID is not a text channel.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=False)
        await bot.modmail_service.admin_set_logs_channel(channel.id)
        await interaction.followup.send(f"Modmail logs channel set to {channel.mention}.", ephemeral=True)

    @bot.tree.command(name="modmail_refresh", description="Repost the modmail panel message.")
    @app_commands.default_permissions(manage_guild=True)
    async def modmail_refresh(interaction: discord.Interaction) -> None:
        if not _is_admin(interaction):
            await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=False)
        await bot.modmail_service.sync_panel(repost=True)
        await interaction.followup.send("Modmail panel refreshed.", ephemeral=True)

    @bot.tree.command(name="queue_vc", description="Set main and team voice channels used for match start checks.")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(
        main_vc="Main voice channel where queued players wait",
        team_a_vc="Team A voice channel",
        team_b_vc="Team B voice channel",
    )
    async def queue_vc(
        interaction: discord.Interaction,
        main_vc: discord.VoiceChannel | None = None,
        team_a_vc: discord.VoiceChannel | None = None,
        team_b_vc: discord.VoiceChannel | None = None,
    ) -> None:
        if not _is_admin(interaction):
            await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
            return

        if main_vc is None and team_a_vc is None and team_b_vc is None:
            config = bot.db.get_queue_config()
            await interaction.response.send_message(
                (
                    "Current voice config:\n"
                    f"Main VC: {_channel_ref(config.main_voice_channel_id)}\n"
                    f"Team A VC: {_channel_ref(config.team_a_voice_channel_id)}\n"
                    f"Team B VC: {_channel_ref(config.team_b_voice_channel_id)}"
                ),
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=False)
        await bot.queue_service.admin_set_voice_channels(
            main_voice_channel_id=(main_vc.id if main_vc else None),
            team_a_voice_channel_id=(team_a_vc.id if team_a_vc else None),
            team_b_voice_channel_id=(team_b_vc.id if team_b_vc else None),
        )
        config = bot.db.get_queue_config()
        await interaction.followup.send(
            (
                "Voice channels updated:\n"
                f"Main VC: {_channel_ref(config.main_voice_channel_id)}\n"
                f"Team A VC: {_channel_ref(config.team_a_voice_channel_id)}\n"
                f"Team B VC: {_channel_ref(config.team_b_voice_channel_id)}"
            ),
            ephemeral=True,
        )

    @bot.tree.command(name="vc_finish", description="Force-finish the current VC check and start the match now.")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(assume_test_ready="Treat synthetic test players as already VC-ready")
    async def vc_finish(interaction: discord.Interaction, assume_test_ready: bool = True) -> None:
        if not _is_admin(interaction):
            await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=False)
        ok, msg = await bot.queue_service.admin_force_vc_check(assume_test_players_ready=assume_test_ready)
        if not ok:
            await interaction.followup.send(msg, ephemeral=True)
            return
        await interaction.followup.send(msg, ephemeral=True)

    @bot.tree.command(name="vc_private", description="Toggle private mode for Team A / Team B voice channels.")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(enabled="If true, users cannot manually join team VC channels")
    async def vc_private(interaction: discord.Interaction, enabled: bool = True) -> None:
        if not _is_admin(interaction):
            await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
            return
        if interaction.guild is None:
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=False)
        ok, msg = await bot.queue_service.admin_set_team_vc_privacy(interaction.guild, enabled=enabled)
        if not ok:
            await interaction.followup.send(msg, ephemeral=True)
            return
        await interaction.followup.send(msg, ephemeral=True)

    @bot.tree.command(name="queue_mode", description="Set queue mode (role queue or open queue).")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(mode="Queue mode")
    @app_commands.choices(mode=QUEUE_MODE_CHOICES)
    async def queue_admin_mode(
        interaction: discord.Interaction,
        mode: app_commands.Choice[str],
    ) -> None:
        if not _is_admin(interaction):
            await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=False)
        await bot.queue_service.admin_set_mode(mode.value)
        await interaction.followup.send(f"Queue mode updated to `{mode.value}`.", ephemeral=True)

    @bot.tree.command(name="queue_rules", description="Update players per match and role slots per team.")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(
        players_per_match="Total players needed to start a match",
        tank_per_team="Tank slots per team",
        dps_per_team="DPS slots per team",
        support_per_team="Support slots per team",
    )
    async def queue_admin_rules(
        interaction: discord.Interaction,
        players_per_match: int,
        tank_per_team: int,
        dps_per_team: int,
        support_per_team: int,
    ) -> None:
        if not _is_admin(interaction):
            await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
            return

        if players_per_match < 2 or players_per_match % 2 != 0:
            await interaction.response.send_message("players_per_match must be an even number greater than 1.", ephemeral=True)
            return
        if tank_per_team < 0 or dps_per_team < 0 or support_per_team < 0:
            await interaction.response.send_message("Role slots cannot be negative.", ephemeral=True)
            return

        team_size = players_per_match // 2
        role_slots_per_team = tank_per_team + dps_per_team + support_per_team
        if role_slots_per_team > team_size:
            await interaction.response.send_message(
                "Per-team role slots exceed team size. Reduce role slots or increase players_per_match.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=False)
        await bot.queue_service.admin_set_rules(
            players_per_match=players_per_match,
            tank_per_team=tank_per_team,
            dps_per_team=dps_per_team,
            support_per_team=support_per_team,
        )
        await interaction.followup.send(
            (
                f"Queue rules updated: players_per_match `{players_per_match}`, "
                f"tank `{tank_per_team}`, dps `{dps_per_team}`, support `{support_per_team}`, "
                "fill is always available until the queue is full."
            ),
            ephemeral=True,
        )

    @bot.tree.command(name="queue_remove", description="Remove a player from queue.")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(player="Player to remove")
    async def queue_admin_remove(interaction: discord.Interaction, player: discord.Member) -> None:
        if not _is_admin(interaction):
            await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=False)
        removed = await bot.queue_service.admin_remove_player(player.id)
        if removed:
            await interaction.followup.send(f"Removed {player.mention} from queue.", ephemeral=True)
        else:
            await interaction.followup.send(f"{player.mention} is not in queue.", ephemeral=True)

    @bot.tree.command(name="player_stats", description="Show stored DB stats for a player.")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(
        player="Discord user to inspect",
        player_id="Raw Discord ID (useful for synthetic test users)",
    )
    async def player_stats(
        interaction: discord.Interaction,
        player: discord.User | None = None,
        player_id: str | None = None,
    ) -> None:
        if not _is_admin(interaction):
            await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
            return

        target_id: int
        target_label: str
        if player is not None:
            target_id = player.id
            target_label = player.mention
        elif player_id:
            try:
                target_id = int(player_id.strip())
            except ValueError:
                await interaction.response.send_message("player_id must be a valid integer Discord ID.", ephemeral=True)
                return
            target_label = f"`{target_id}`"
        else:
            await interaction.response.send_message("Provide `player` or `player_id`.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=False)
        stats = bot.db.get_player_stats(target_id)
        if stats is None:
            await interaction.followup.send(f"No player record found for {target_label}.", ephemeral=True)
            return

        history = bot.db.list_player_match_entries(target_id, limit=5)
        history_lines = [
            (
                f"#{entry.match_id} {entry.created_at} | {entry.mode} | {entry.team} | "
                f"{entry.assigned_role} | mmr {entry.mmr} | result {entry.result}"
            )
            for entry in history
        ]
        queue_status = (
            f"queued as `{stats.queue_role}` since `{stats.queue_joined_at}`"
            if stats.queue_role and stats.queue_joined_at
            else "not queued"
        )
        history_block = "\n".join(history_lines) if history_lines else "None"
        history_value = history_block if len(history_block) <= 1000 else f"{history_block[:997]}..."
        decided_matches = stats.wins + stats.losses
        win_rate = (stats.wins / decided_matches * 100.0) if decided_matches > 0 else 0.0

        stats_embed = discord.Embed(
            title="Player Stats",
            description=f"Target: {target_label}",
            color=discord.Color.blurple(),
            timestamp=datetime.now(timezone.utc),
        )
        stats_embed.add_field(
            name="Profile",
            value=(
                f"DB ID: `{stats.discord_id}`\n"
                f"Display Name: `{stats.display_name}`\n"
                f"BattleTag: `{stats.battletag or 'not set'}`\n"
                f"MMR: `{stats.mmr}`\n"
                f"Preferred Role: `{stats.preferred_role}`"
            ),
            inline=False,
        )
        stats_embed.add_field(
            name="Queue",
            value=f"{queue_status}\nLast profile update: `{stats.updated_at}`",
            inline=False,
        )
        stats_embed.add_field(
            name="Results",
            value=(
                f"Matches: `{stats.matches_played}`\n"
                f"W/L/D: `{stats.wins}/{stats.losses}/{stats.draws}`\n"
                f"Win rate (W/L): `{win_rate:.1f}%`\n"
                f"Results reported: `{stats.results_reported}`"
            ),
            inline=True,
        )
        stats_embed.add_field(
            name="Reliability",
            value=(
                f"No-shows: `{stats.no_show_count}`\n"
                f"Disconnects: `{stats.disconnect_count}`\n"
                f"Last match: `{stats.last_match_at or 'none'}`"
            ),
            inline=True,
        )
        stats_embed.add_field(
            name="Role Usage",
            value=f"`{_format_role_distribution(stats.assigned_role_counts)}`",
            inline=False,
        )
        stats_embed.add_field(name="Recent Matches (max 5)", value=history_value, inline=False)
        await interaction.followup.send(embed=stats_embed, ephemeral=True)

    @bot.tree.command(name="recent_matches", description="Show recent matches and recorded results.")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(limit="How many recent matches to list (1-25)")
    async def recent_matches(interaction: discord.Interaction, limit: int = 10) -> None:
        if not _is_admin(interaction):
            await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
            return
        if limit < 1 or limit > 25:
            await interaction.response.send_message("limit must be between 1 and 25.", ephemeral=True)
            return

        rows = bot.db.list_recent_matches(limit=limit)
        if not rows:
            await interaction.response.send_message("No matches found.", ephemeral=True)
            return

        lines: list[str] = []
        for row in rows:
            winner = row["winner_team"] or "unreported"
            reported_at = row["reported_at"] or "n/a"
            roles_flag = "on" if int(row["roles_enforced"]) else "off"
            lines.append(
                (
                    f"#{row['id']} | {row['created_at']} | mode `{row['mode']}` | "
                    f"roles `{roles_flag}` | winner `{winner}` | reported `{reported_at}`"
                )
            )
        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    @bot.tree.command(name="match_result", description="Record a match result.")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(match_id="Match ID", winner="Winning side or draw")
    @app_commands.choices(winner=RESULT_TEAM_CHOICES)
    async def match_result(
        interaction: discord.Interaction,
        match_id: int,
        winner: app_commands.Choice[str],
    ) -> None:
        if not _is_admin(interaction):
            await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
            return
        if match_id < 1:
            await interaction.response.send_message("match_id must be positive.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=False)
        ok, message = await bot.queue_service.admin_force_result(match_id, winner.value)
        if not ok:
            await interaction.followup.send(message, ephemeral=True)
            return
        await interaction.followup.send(f"Match `{match_id}` -> `{winner.value}`. {message}", ephemeral=True)

    @bot.tree.command(name="match_cancel", description="Cancel the active match.")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(requeue_players="Return active match players to queue")
    async def match_cancel(interaction: discord.Interaction, requeue_players: bool = True) -> None:
        if not _is_admin(interaction):
            await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=False)
        ok, message = await bot.queue_service.admin_cancel_active_match(
            requeue_players=requeue_players,
            remake_immediately=False,
        )
        if not ok:
            await interaction.followup.send(message, ephemeral=True)
            return
        await interaction.followup.send(message, ephemeral=True)

    @bot.tree.command(name="match_remake", description="Cancel and immediately remake the active match.")
    @app_commands.default_permissions(manage_guild=True)
    async def match_remake(interaction: discord.Interaction) -> None:
        if not _is_admin(interaction):
            await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=False)
        ok, message = await bot.queue_service.admin_cancel_active_match(
            requeue_players=True,
            remake_immediately=True,
        )
        if not ok:
            await interaction.followup.send(message, ephemeral=True)
            return
        await interaction.followup.send(message, ephemeral=True)

    @bot.tree.command(name="queue_clear", description="Clear all queued players.")
    @app_commands.default_permissions(manage_guild=True)
    async def queue_admin_clear(interaction: discord.Interaction) -> None:
        if not _is_admin(interaction):
            await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=False)
        removed = await bot.queue_service.admin_clear_queue()
        await interaction.followup.send(f"Queue cleared. Removed `{removed}` players.", ephemeral=True)

    @bot.tree.command(name="queue_refresh", description="Repost the queue panel message.")
    @app_commands.default_permissions(manage_guild=True)
    async def queue_admin_refresh(interaction: discord.Interaction) -> None:
        if not _is_admin(interaction):
            await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=False)
        await bot.queue_service.sync_panel(repost=True)
        await interaction.followup.send("Queue panel refreshed.", ephemeral=True)

    @bot.tree.command(name="ticket_close", description="Close the current modmail ticket thread.")
    async def ticket_close(interaction: discord.Interaction) -> None:
        await bot.modmail_service.handle_close_ticket(interaction)

    @bot.tree.command(name="queue_admin_test_scenario", description="Load a test scenario with synthetic players.")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(scenario="Scenario to load")
    @app_commands.choices(scenario=TEST_SCENARIO_CHOICES)
    async def queue_admin_test_scenario(
        interaction: discord.Interaction,
        scenario: app_commands.Choice[str],
    ) -> None:
        if not _is_admin(interaction):
            await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=False)
        queue_mode, added, remaining = await bot.queue_service.admin_seed_test_scenario(scenario.value)
        consumed = max(added - remaining, 0)
        await interaction.followup.send(
            (
                f"Loaded test scenario `{scenario.value}` in `{queue_mode}` mode. "
                f"Added `{added}` synthetic players. Queue now has `{remaining}` players. "
                f"Auto-match consumed `{consumed}` players."
            ),
            ephemeral=True,
        )

    @bot.tree.command(name="queue_admin_test_add", description="Add synthetic players to the current queue.")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(role="Role for synthetic players", count="Number of synthetic players to add (1-50)")
    @app_commands.choices(role=TEST_ROLE_CHOICES)
    async def queue_admin_test_add(
        interaction: discord.Interaction,
        role: app_commands.Choice[str],
        count: int,
    ) -> None:
        if not _is_admin(interaction):
            await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
            return
        if count < 1 or count > 50:
            await interaction.response.send_message("count must be between 1 and 50.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=False)
        try:
            target_role, added, remaining = await bot.queue_service.admin_add_test_players(role.value, count)
        except ValueError as exc:
            await interaction.followup.send(str(exc), ephemeral=True)
            return

        await interaction.followup.send(
            (
                f"Added `{added}` synthetic players as `{target_role}`. "
                f"Queue now has `{remaining}` players."
            ),
            ephemeral=True,
        )

    @bot.tree.command(name="queue_admin_test_results", description="Apply synthetic win/loss results to recent matches.")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(mode="Result pattern", count="How many recent matches to update (1-50)")
    @app_commands.choices(mode=TEST_RESULT_MODE_CHOICES)
    async def queue_admin_test_results(
        interaction: discord.Interaction,
        mode: app_commands.Choice[str],
        count: int = 10,
    ) -> None:
        if not _is_admin(interaction):
            await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
            return
        if mode.value != "clear" and (count < 1 or count > 50):
            await interaction.response.send_message("count must be between 1 and 50.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=False)
        updated, scanned, mmr_applied = await bot.queue_service.admin_apply_test_results(mode.value, count)
        if mode.value == "clear":
            await interaction.followup.send(f"Cleared `{updated}` recorded match results.", ephemeral=True)
            return

        await interaction.followup.send(
            (
                f"Applied `{mode.value}` results to `{updated}` matches "
                f"(scanned `{scanned}` recent matches, MMR applied to `{mmr_applied}`)."
            ),
            ephemeral=True,
        )


def main() -> None:
    settings = load_settings()
    bot = OverwatchBot(settings)
    bot.run(settings.discord_token)


if __name__ == "__main__":
    main()
