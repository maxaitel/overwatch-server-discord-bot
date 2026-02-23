from __future__ import annotations

from itertools import combinations

from .models import AssignedPlayer, MatchmakingResult, QueuedPlayer, Team


def normalize_role(_: str | None) -> str:
    return "queue"


def _sum_mmr(players: list[QueuedPlayer]) -> int:
    return sum(player.mmr for player in players)


def _best_split(players: list[QueuedPlayer]) -> tuple[list[QueuedPlayer], list[QueuedPlayer]] | None:
    team_size = len(players) // 2
    if team_size == 0:
        return None

    indexes = list(range(len(players)))
    best: tuple[int, int, list[QueuedPlayer], list[QueuedPlayer]] | None = None

    # Keep index 0 in team A to avoid mirrored duplicates.
    for combo in combinations(indexes[1:], team_size - 1):
        team_a_idx = {0, *combo}
        team_a = [players[i] for i in indexes if i in team_a_idx]
        team_b = [players[i] for i in indexes if i not in team_a_idx]

        team_a_mmr = _sum_mmr(team_a)
        team_b_mmr = _sum_mmr(team_b)
        diff = abs(team_a_mmr - team_b_mmr)
        stronger_side = max(team_a_mmr, team_b_mmr)
        score = (diff, stronger_side)

        if best is None or score < (best[0], best[1]):
            best = (diff, stronger_side, team_a, team_b)

    if best is None:
        return None
    return best[2], best[3]


def _build_team(team_name: str, players: list[QueuedPlayer]) -> Team:
    assigned = [
        AssignedPlayer(
            discord_id=player.discord_id,
            display_name=player.display_name,
            mmr=player.mmr,
            preferred_role="queue",
            assigned_role="queue",
        )
        for player in sorted(players, key=lambda p: p.mmr, reverse=True)
    ]
    return Team(name=team_name, players=assigned)


def make_match(
    players: list[QueuedPlayer],
    *,
    enforce_roles: bool = True,
    role_quota_per_team: dict[str, int] | None = None,
) -> MatchmakingResult:
    _ = enforce_roles
    _ = role_quota_per_team
    if len(players) < 2 or len(players) % 2 != 0:
        raise ValueError("Matchmaking requires an even number of players.")

    split = _best_split(players)
    if split is None:
        raise RuntimeError("Unable to create a match from the current queue.")

    team_a_raw, team_b_raw = split
    team_a = _build_team("Team A", team_a_raw)
    team_b = _build_team("Team B", team_b_raw)
    return MatchmakingResult(team_a=team_a, team_b=team_b, roles_enforced=False)
