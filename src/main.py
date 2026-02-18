from __future__ import annotations

import asyncio
import logging

import discord
from discord import app_commands
from discord.ext import commands

from .config import Settings, load_settings
from .matchmaking import make_match
from .models import AssignedPlayer, QueueConfig, QueuedPlayer, Team
from .storage import Database


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("ow-bot")

QUEUE_MODE_CHOICES = [
    app_commands.Choice(name="Role Queue", value="role"),
    app_commands.Choice(name="Open Queue", value="open"),
]


def _format_player(player: AssignedPlayer) -> str:
    role = player.assigned_role.upper()
    return f"<@{player.discord_id}> | `{role}` | MMR `{player.mmr}`"


def _format_team(team: Team) -> str:
    lines = [_format_player(player) for player in team.players]
    lines.append(f"Average MMR: `{team.average_mmr}`")
    return "\n".join(lines)


def _is_admin(interaction: discord.Interaction) -> bool:
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    return bool(member and member.guild_permissions.manage_guild)


class QueuePanelView(discord.ui.View):
    def __init__(self, bot: OverwatchBot, config: QueueConfig) -> None:
        super().__init__(timeout=None)
        self.bot = bot
        is_open_queue = config.queue_mode == "open"

        self.join_open.disabled = not is_open_queue
        self.join_tank.disabled = is_open_queue
        self.join_dps.disabled = is_open_queue
        self.join_support.disabled = is_open_queue
        self.join_fill.disabled = is_open_queue

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


class QueueService:
    def __init__(self, bot: OverwatchBot) -> None:
        self.bot = bot
        self.lock = asyncio.Lock()
        self._reposting = False

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

    def build_embed(self, config: QueueConfig) -> discord.Embed:
        queued = self.bot.db.list_queue()
        total = len(queued)
        queue_mode_label = "Role Queue" if config.queue_mode == "role" else "Open Queue"
        embed = discord.Embed(
            title="Overwatch Queue",
            description=f"Players queued: {total}/{config.players_per_match}\nMode: {queue_mode_label}",
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

        embed.set_footer(text="Use the buttons below to join or leave.")
        return embed

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
        while True:
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
            if channel:
                match_message = self._compose_match_message(
                    match_id,
                    config,
                    result.team_a,
                    result.team_b,
                    result.roles_enforced,
                )
                await channel.send(match_message)

    async def handle_join(self, interaction: discord.Interaction, requested_role: str) -> None:
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
            self.bot.db.upsert_player(discord_id=user_id, display_name=interaction.user.display_name)

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

    async def admin_set_channel(self, channel_id: int) -> None:
        async with self.lock:
            self.bot.db.update_queue_config(queue_channel_id=channel_id, queue_message_id=0)
            await self.sync_panel(repost=True)

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

    async def handle_queue_channel_message(self, message: discord.Message) -> None:
        if self._reposting:
            return
        async with self.lock:
            try:
                await message.delete()
            except discord.DiscordException:
                pass
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
        self._ready_once = False

        config = self.db.get_queue_config()
        if not config.queue_channel_id and settings.queue_channel_id:
            self.db.update_queue_config(queue_channel_id=settings.queue_channel_id, queue_message_id=0)

    async def setup_hook(self) -> None:
        register_commands(self)
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
    @bot.tree.command(name="queue_admin_channel", description="Set the queue channel and post the queue panel.")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(channel="Text channel for the queue panel")
    async def queue_admin_channel(interaction: discord.Interaction, channel: discord.TextChannel) -> None:
        if not _is_admin(interaction):
            await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=False)
        await bot.queue_service.admin_set_channel(channel.id)
        await interaction.followup.send(f"Queue channel set to {channel.mention}.", ephemeral=True)

    @bot.tree.command(name="queue_admin_mode", description="Set queue mode (role queue or open queue).")
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

    @bot.tree.command(name="queue_admin_rules", description="Update players per match and role slots per team.")
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

    @bot.tree.command(name="queue_admin_remove", description="Remove a player from queue.")
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

    @bot.tree.command(name="queue_admin_clear", description="Clear all queued players.")
    @app_commands.default_permissions(manage_guild=True)
    async def queue_admin_clear(interaction: discord.Interaction) -> None:
        if not _is_admin(interaction):
            await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=False)
        removed = await bot.queue_service.admin_clear_queue()
        await interaction.followup.send(f"Queue cleared. Removed `{removed}` players.", ephemeral=True)

    @bot.tree.command(name="queue_admin_refresh", description="Repost the queue panel message.")
    @app_commands.default_permissions(manage_guild=True)
    async def queue_admin_refresh(interaction: discord.Interaction) -> None:
        if not _is_admin(interaction):
            await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=False)
        await bot.queue_service.sync_panel(repost=True)
        await interaction.followup.send("Queue panel refreshed.", ephemeral=True)


def main() -> None:
    settings = load_settings()
    bot = OverwatchBot(settings)
    bot.run(settings.discord_token)


if __name__ == "__main__":
    main()
