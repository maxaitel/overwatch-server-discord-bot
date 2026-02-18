from dataclasses import dataclass


@dataclass(slots=True)
class Player:
    discord_id: int
    display_name: str
    battletag: str | None
    mmr: int
    preferred_role: str


@dataclass(slots=True)
class QueuedPlayer:
    discord_id: int
    display_name: str
    mmr: int
    role: str
    queued_at: str


@dataclass(slots=True)
class AssignedPlayer:
    discord_id: int
    display_name: str
    mmr: int
    preferred_role: str
    assigned_role: str


@dataclass(slots=True)
class Team:
    name: str
    players: list[AssignedPlayer]

    @property
    def total_mmr(self) -> int:
        return sum(player.mmr for player in self.players)

    @property
    def average_mmr(self) -> int:
        if not self.players:
            return 0
        return round(self.total_mmr / len(self.players))


@dataclass(slots=True)
class MatchmakingResult:
    team_a: Team
    team_b: Team
    roles_enforced: bool


@dataclass(slots=True)
class QueueConfig:
    queue_channel_id: int | None
    queue_message_id: int | None
    queue_mode: str  # "role" | "open"
    players_per_match: int
    tank_per_team: int
    dps_per_team: int
    support_per_team: int

    @property
    def team_size(self) -> int:
        return self.players_per_match // 2

    @property
    def role_slots_per_team(self) -> int:
        return self.tank_per_team + self.dps_per_team + self.support_per_team

    @property
    def fill_entries_cap_total(self) -> int:
        # Fill acts as a wildcard role preference and can be chosen by anyone
        # until the overall queue is full.
        return self.players_per_match

    def role_caps_total(self) -> dict[str, int]:
        caps = {
            "tank": self.tank_per_team * 2,
            "dps": self.dps_per_team * 2,
            "support": self.support_per_team * 2,
            "fill": self.fill_entries_cap_total,
        }
        return caps
