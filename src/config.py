from dataclasses import dataclass
import os

from dotenv import load_dotenv


VALID_ROLES = {"tank", "dps", "support", "flex"}


@dataclass(frozen=True, slots=True)
class Settings:
    discord_token: str
    database_path: str
    command_guild_id: int | None
    queue_channel_id: int | None
    players_per_match: int
    tank_per_team: int
    dps_per_team: int
    support_per_team: int
    default_mmr: int
    default_role: str


def load_settings() -> Settings:
    load_dotenv()

    token = os.getenv("DISCORD_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Missing DISCORD_TOKEN in environment.")

    database_path = os.getenv("SQLITE_PATH", "bot.db").strip() or "bot.db"

    guild_raw = os.getenv("COMMAND_GUILD_ID", "").strip()
    command_guild_id = int(guild_raw) if guild_raw else None

    queue_channel_raw = os.getenv("QUEUE_CHANNEL_ID", "").strip()
    queue_channel_id = int(queue_channel_raw) if queue_channel_raw else None

    players_per_match = int(os.getenv("PLAYERS_PER_MATCH", os.getenv("QUEUE_SIZE", "10")))
    if players_per_match < 2 or players_per_match % 2 != 0:
        raise RuntimeError("PLAYERS_PER_MATCH must be an even number >= 2.")

    tank_per_team = int(os.getenv("TANK_PER_TEAM", "1"))
    dps_per_team = int(os.getenv("DPS_PER_TEAM", "2"))
    support_per_team = int(os.getenv("SUPPORT_PER_TEAM", "2"))
    if tank_per_team < 0 or dps_per_team < 0 or support_per_team < 0:
        raise RuntimeError("Role slots per team cannot be negative.")

    team_size = players_per_match // 2
    role_slots_per_team = tank_per_team + dps_per_team + support_per_team
    if role_slots_per_team > team_size:
        raise RuntimeError(
            "Role slots per team exceed team size. "
            "Increase PLAYERS_PER_MATCH or reduce per-role values."
        )

    default_mmr = int(os.getenv("DEFAULT_MMR", "2500"))
    if default_mmr < 1:
        raise RuntimeError("DEFAULT_MMR must be positive.")

    default_role = os.getenv("DEFAULT_ROLE", "flex").strip().lower() or "flex"
    if default_role not in VALID_ROLES:
        default_role = "flex"

    return Settings(
        discord_token=token,
        database_path=database_path,
        command_guild_id=command_guild_id,
        queue_channel_id=queue_channel_id,
        players_per_match=players_per_match,
        tank_per_team=tank_per_team,
        dps_per_team=dps_per_team,
        support_per_team=support_per_team,
        default_mmr=default_mmr,
        default_role=default_role,
    )
