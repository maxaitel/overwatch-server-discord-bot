from __future__ import annotations

from itertools import combinations

from .models import AssignedPlayer, MatchmakingResult, QueuedPlayer, Team

CORE_ROLES = ("tank", "dps", "support")
VALID_ROLES = set(CORE_ROLES) | {"fill", "flex", "open"}


def normalize_role(role: str | None) -> str:
    if role is None:
        return "fill"
    normalized = role.strip().lower()
    if normalized not in VALID_ROLES:
        return "fill"
    if normalized == "flex":
        return "fill"
    if normalized == "open":
        return "fill"
    return normalized


def _is_role_feasible(
    players: list[QueuedPlayer],
    team_size: int,
    role_quota_per_team: dict[str, int],
) -> bool:
    if len(players) != team_size:
        return False

    fixed = {role: 0 for role in role_quota_per_team}
    flex_count = 0

    for player in players:
        role = normalize_role(player.role)
        if role in role_quota_per_team:
            fixed[role] += 1
            if fixed[role] > role_quota_per_team[role]:
                return False
        else:
            flex_count += 1

    missing = sum(max(role_quota_per_team[role] - fixed[role], 0) for role in role_quota_per_team)
    return missing <= flex_count


def _assign_roles(
    players: list[QueuedPlayer],
    team_size: int,
    role_quota_per_team: dict[str, int],
) -> dict[int, str] | None:
    if not _is_role_feasible(players, team_size, role_quota_per_team):
        return None

    remaining = dict(role_quota_per_team)
    assigned: dict[int, str] = {}
    flex_players: list[QueuedPlayer] = []

    for player in players:
        role = normalize_role(player.role)
        if role in role_quota_per_team:
            assigned[player.discord_id] = role
            remaining[role] -= 1
        else:
            flex_players.append(player)

    ordered_roles = [role for role in CORE_ROLES if role in remaining] + [
        role for role in remaining if role not in CORE_ROLES
    ]
    next_flex_index = 0

    for role in ordered_roles:
        while remaining[role] > 0:
            if next_flex_index >= len(flex_players):
                return None
            assigned[flex_players[next_flex_index].discord_id] = role
            next_flex_index += 1
            remaining[role] -= 1

    while next_flex_index < len(flex_players):
        assigned[flex_players[next_flex_index].discord_id] = "fill"
        next_flex_index += 1

    return assigned


def _sum_mmr(players: list[QueuedPlayer]) -> int:
    return sum(player.mmr for player in players)


def _best_split(
    players: list[QueuedPlayer],
    require_roles: bool,
    role_quota_per_team: dict[str, int],
) -> tuple[list[QueuedPlayer], list[QueuedPlayer]] | None:
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

        if require_roles:
            if not _is_role_feasible(team_a, team_size, role_quota_per_team):
                continue
            if not _is_role_feasible(team_b, team_size, role_quota_per_team):
                continue

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


def _build_team(team_name: str, players: list[QueuedPlayer], role_map: dict[int, str]) -> Team:
    assigned = [
        AssignedPlayer(
            discord_id=player.discord_id,
            display_name=player.display_name,
            mmr=player.mmr,
            preferred_role=normalize_role(player.role),
            assigned_role=role_map.get(player.discord_id, normalize_role(player.role)),
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
    if len(players) < 2 or len(players) % 2 != 0:
        raise ValueError("Matchmaking requires an even number of players.")

    role_quota = role_quota_per_team or {}
    team_size = len(players) // 2
    if enforce_roles and sum(role_quota.values()) > team_size:
        raise ValueError("Role quota exceeds team size.")

    require_roles = enforce_roles and bool(role_quota)
    split = _best_split(players, require_roles=require_roles, role_quota_per_team=role_quota)
    roles_enforced = require_roles

    if split is None and require_roles:
        split = _best_split(players, require_roles=False, role_quota_per_team=role_quota)
        roles_enforced = False

    if split is None:
        raise RuntimeError("Unable to create a match from the current queue.")

    team_a_raw, team_b_raw = split
    role_map_a = _assign_roles(team_a_raw, team_size, role_quota) if roles_enforced else {}
    role_map_b = _assign_roles(team_b_raw, team_size, role_quota) if roles_enforced else {}

    if roles_enforced and (role_map_a is None or role_map_b is None):
        raise RuntimeError("Role assignment failed unexpectedly.")

    team_a = _build_team("Team A", team_a_raw, role_map_a or {})
    team_b = _build_team("Team B", team_b_raw, role_map_b or {})
    return MatchmakingResult(team_a=team_a, team_b=team_b, roles_enforced=roles_enforced)
