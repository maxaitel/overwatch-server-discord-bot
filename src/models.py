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
    queue_mode: str  # "queue"
    players_per_match: int
    tank_per_team: int
    dps_per_team: int
    support_per_team: int
    main_voice_channel_id: int | None
    team_a_voice_channel_id: int | None
    team_b_voice_channel_id: int | None

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


@dataclass(slots=True)
class PlayerMatchEntry:
    match_id: int
    created_at: str
    mode: str
    team: str
    assigned_role: str
    mmr: int
    result: str  # "win" | "loss" | "draw" | "unknown"


@dataclass(slots=True)
class PlayerStats:
    discord_id: int
    display_name: str
    battletag: str | None
    mmr: int
    preferred_role: str
    updated_at: str
    queue_role: str | None
    queue_joined_at: str | None
    matches_played: int
    last_match_at: str | None
    wins: int
    losses: int
    draws: int
    results_reported: int
    no_show_count: int
    disconnect_count: int
    assigned_role_counts: dict[str, int]


@dataclass(slots=True)
class ActiveMatch:
    match_id: int
    channel_id: int
    message_id: int
    status: str  # waiting_vc | live | disputed
    map_name: str | None
    ready_deadline: str | None
    started_at: str | None
    team_a_voice_channel_id: int | None
    team_b_voice_channel_id: int | None
    escalated: bool


@dataclass(slots=True)
class MatchMmrChange:
    match_id: int
    discord_id: int
    display_name: str
    team: str
    mmr_before: int
    delta: int
    mmr_after: int


@dataclass(slots=True)
class MatchCaptain:
    match_id: int
    captain_id: int
    selected_by: int
    selected_at: str
    selection_method: str  # admin_auto | first_claim


@dataclass(slots=True)
class ModmailConfig:
    panel_channel_id: int | None
    panel_message_id: int | None
    logs_channel_id: int | None


@dataclass(slots=True)
class ModmailTicket:
    ticket_id: int
    guild_id: int
    user_id: int
    thread_id: int
    status: str  # open | closed
    created_at: str
    closed_at: str | None
    closed_by: int | None
