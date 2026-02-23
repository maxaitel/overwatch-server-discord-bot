from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3

from .models import (
    ActiveMatch,
    MatchCaptain,
    MatchMmrChange,
    ModmailConfig,
    ModmailTicket,
    Player,
    PlayerMatchEntry,
    PlayerStats,
    QueueConfig,
    QueuedPlayer,
    Team,
)

DEFAULT_QUEUE_MODE = "queue"
DEFAULT_QUEUE_STATE_KEY = "default"
DEFAULT_QUEUE_ENTRY_ROLE = "queue"
VALID_WINNER_TEAMS = {"Team A", "Team B", "Draw"}
VALID_ACTIVE_MATCH_STATUSES = {"waiting_vc", "live", "disputed"}
MIN_SR = 0
MAX_SR = 5000
DEFAULT_ELO_K_FACTOR = 24
DEFAULT_CALIBRATION_MATCHES = 5
DEFAULT_CALIBRATION_MULTIPLIER = 2.0
_UNSET = object()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class Database:
    def __init__(
        self,
        path: str,
        default_mmr: int,
        default_role: str,
        default_players_per_match: int,
        default_tank_per_team: int = 1,
        default_dps_per_team: int = 2,
        default_support_per_team: int = 2,
    ) -> None:
        self.default_mmr = max(MIN_SR, min(MAX_SR, default_mmr))
        self.default_role = default_role
        self.default_players_per_match = default_players_per_match
        self.default_tank_per_team = default_tank_per_team
        self.default_dps_per_team = default_dps_per_team
        self.default_support_per_team = default_support_per_team
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self._create_schema()
        self._ensure_player_columns()
        self._ensure_queue_config_columns()
        self._ensure_active_match_columns()
        self._ensure_modmail_config_columns()
        self._normalize_queue_state()
        self._ensure_queue_config_row()
        self._normalize_queue_config_mode()
        self._normalize_queue_entry_roles()
        self._ensure_modmail_config_row()
        self._ensure_player_role_mmr_rows()

    def _clamp_sr(self, value: int) -> int:
        if value < MIN_SR:
            return MIN_SR
        if value > MAX_SR:
            return MAX_SR
        return value

    def _create_schema(self) -> None:
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS players (
                    discord_id INTEGER PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    battletag TEXT,
                    mmr INTEGER NOT NULL,
                    preferred_role TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS player_role_mmr (
                    discord_id INTEGER PRIMARY KEY,
                    tank_mmr INTEGER NOT NULL,
                    dps_mmr INTEGER NOT NULL,
                    support_mmr INTEGER NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (discord_id) REFERENCES players(discord_id) ON DELETE CASCADE
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    discord_id INTEGER NOT NULL UNIQUE,
                    mode TEXT NOT NULL,
                    role TEXT NOT NULL,
                    queued_at TEXT NOT NULL,
                    FOREIGN KEY (discord_id) REFERENCES players(discord_id) ON DELETE CASCADE
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS matches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    mode TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    roles_enforced INTEGER NOT NULL,
                    team_a_json TEXT NOT NULL,
                    team_b_json TEXT NOT NULL
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS match_results (
                    match_id INTEGER PRIMARY KEY,
                    winner_team TEXT NOT NULL,
                    reported_at TEXT NOT NULL,
                    FOREIGN KEY (match_id) REFERENCES matches(id) ON DELETE CASCADE
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS match_mmr_changes (
                    match_id INTEGER NOT NULL,
                    discord_id INTEGER NOT NULL,
                    display_name TEXT NOT NULL,
                    team TEXT NOT NULL,
                    mmr_before INTEGER NOT NULL,
                    delta INTEGER NOT NULL,
                    mmr_after INTEGER NOT NULL,
                    PRIMARY KEY (match_id, discord_id),
                    FOREIGN KEY (match_id) REFERENCES matches(id) ON DELETE CASCADE,
                    FOREIGN KEY (discord_id) REFERENCES players(discord_id) ON DELETE CASCADE
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS queue_config (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    queue_channel_id INTEGER,
                    queue_message_id INTEGER,
                    queue_mode TEXT NOT NULL,
                    players_per_match INTEGER NOT NULL,
                    tank_per_team INTEGER NOT NULL,
                    dps_per_team INTEGER NOT NULL,
                    support_per_team INTEGER NOT NULL,
                    main_voice_channel_id INTEGER,
                    team_a_voice_channel_id INTEGER,
                    team_b_voice_channel_id INTEGER
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS active_match (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    match_id INTEGER NOT NULL,
                    channel_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    ready_deadline TEXT,
                    started_at TEXT,
                    team_a_voice_channel_id INTEGER,
                    team_b_voice_channel_id INTEGER,
                    escalated INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY (match_id) REFERENCES matches(id) ON DELETE CASCADE
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS match_reports (
                    match_id INTEGER NOT NULL,
                    team TEXT NOT NULL,
                    reported_winner_team TEXT NOT NULL,
                    reporter_id INTEGER NOT NULL,
                    reported_at TEXT NOT NULL,
                    PRIMARY KEY (match_id, team),
                    FOREIGN KEY (match_id) REFERENCES matches(id) ON DELETE CASCADE
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS match_ready (
                    match_id INTEGER NOT NULL,
                    discord_id INTEGER NOT NULL,
                    ready_at TEXT NOT NULL,
                    PRIMARY KEY (match_id, discord_id),
                    FOREIGN KEY (match_id) REFERENCES matches(id) ON DELETE CASCADE,
                    FOREIGN KEY (discord_id) REFERENCES players(discord_id) ON DELETE CASCADE
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS match_captain (
                    match_id INTEGER PRIMARY KEY,
                    captain_id INTEGER NOT NULL,
                    selected_by INTEGER NOT NULL,
                    selected_at TEXT NOT NULL,
                    selection_method TEXT NOT NULL,
                    FOREIGN KEY (match_id) REFERENCES matches(id) ON DELETE CASCADE,
                    FOREIGN KEY (captain_id) REFERENCES players(discord_id) ON DELETE CASCADE
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS modmail_config (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    panel_channel_id INTEGER,
                    panel_message_id INTEGER,
                    logs_channel_id INTEGER
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS modmail_tickets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    thread_id INTEGER NOT NULL UNIQUE,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    closed_at TEXT,
                    closed_by INTEGER
                )
                """
            )
            self.conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_modmail_tickets_guild_user_status
                ON modmail_tickets(guild_id, user_id, status)
                """
            )

    def _ensure_player_columns(self) -> None:
        columns = {
            row["name"]
            for row in self.conn.execute("PRAGMA table_info(players)").fetchall()
        }
        with self.conn:
            if "no_show_count" not in columns:
                self.conn.execute(
                    """
                    ALTER TABLE players
                    ADD COLUMN no_show_count INTEGER NOT NULL DEFAULT 0
                    """
                )
            if "disconnect_count" not in columns:
                self.conn.execute(
                    """
                    ALTER TABLE players
                    ADD COLUMN disconnect_count INTEGER NOT NULL DEFAULT 0
                    """
                )

    def _ensure_queue_config_columns(self) -> None:
        columns = {
            row["name"]
            for row in self.conn.execute("PRAGMA table_info(queue_config)").fetchall()
        }
        with self.conn:
            if "team_a_voice_channel_id" not in columns:
                self.conn.execute(
                    """
                    ALTER TABLE queue_config
                    ADD COLUMN team_a_voice_channel_id INTEGER
                    """
                )
            if "team_b_voice_channel_id" not in columns:
                self.conn.execute(
                    """
                    ALTER TABLE queue_config
                    ADD COLUMN team_b_voice_channel_id INTEGER
                    """
                )
            if "main_voice_channel_id" not in columns:
                self.conn.execute(
                    """
                    ALTER TABLE queue_config
                    ADD COLUMN main_voice_channel_id INTEGER
                    """
                )

    def _ensure_modmail_config_columns(self) -> None:
        columns = {
            row["name"]
            for row in self.conn.execute("PRAGMA table_info(modmail_config)").fetchall()
        }
        with self.conn:
            if "logs_channel_id" not in columns:
                self.conn.execute(
                    """
                    ALTER TABLE modmail_config
                    ADD COLUMN logs_channel_id INTEGER
                    """
                )

    def _ensure_active_match_columns(self) -> None:
        columns = {
            row["name"]
            for row in self.conn.execute("PRAGMA table_info(active_match)").fetchall()
        }
        with self.conn:
            if "map_name" not in columns:
                self.conn.execute(
                    """
                    ALTER TABLE active_match
                    ADD COLUMN map_name TEXT
                    """
                )

    def _ensure_queue_config_row(self) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO queue_config (
                    id,
                    queue_channel_id,
                    queue_message_id,
                    queue_mode,
                    players_per_match,
                    tank_per_team,
                    dps_per_team,
                    support_per_team,
                    main_voice_channel_id,
                    team_a_voice_channel_id,
                    team_b_voice_channel_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO NOTHING
                """,
                (
                    1,
                    None,
                    None,
                    DEFAULT_QUEUE_MODE,
                    self.default_players_per_match,
                    self.default_tank_per_team,
                    self.default_dps_per_team,
                    self.default_support_per_team,
                    None,
                    None,
                    None,
                ),
            )

    def _ensure_modmail_config_row(self) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO modmail_config (id, panel_channel_id, panel_message_id, logs_channel_id)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(id) DO NOTHING
                """,
                (1, None, None, None),
            )

    def _ensure_player_role_mmr_rows(self) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO player_role_mmr (discord_id, tank_mmr, dps_mmr, support_mmr, updated_at)
                SELECT p.discord_id, p.mmr, p.mmr, p.mmr, p.updated_at
                FROM players p
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM player_role_mmr prm
                    WHERE prm.discord_id = p.discord_id
                )
                """
            )

    def _normalize_queue_config_mode(self) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE queue_config
                SET queue_mode = ?,
                    tank_per_team = 0,
                    dps_per_team = 0,
                    support_per_team = 0
                WHERE id = 1
                """,
                (DEFAULT_QUEUE_MODE,),
            )

    def _normalize_queue_entry_roles(self) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE queue
                SET role = ?
                WHERE mode = ?
                """,
                (DEFAULT_QUEUE_ENTRY_ROLE, DEFAULT_QUEUE_STATE_KEY),
            )

    def _normalize_queue_state(self) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE queue
                SET mode = ?
                """,
                (DEFAULT_QUEUE_STATE_KEY,),
            )

    def close(self) -> None:
        self.conn.close()

    def upsert_player(
        self,
        discord_id: int,
        display_name: str,
        battletag: str | None = None,
        mmr: int | None = None,
        preferred_role: str | None = None,
    ) -> None:
        now = utc_now_iso()
        current = self.get_player(discord_id)
        merged_battletag = battletag if battletag is not None else (current.battletag if current else None)
        merged_mmr_raw = mmr if mmr is not None else (current.mmr if current else self.default_mmr)
        merged_mmr = self._clamp_sr(int(merged_mmr_raw))
        merged_role = preferred_role if preferred_role is not None else (current.preferred_role if current else self.default_role)

        with self.conn:
            self.conn.execute(
                """
                INSERT INTO players (discord_id, display_name, battletag, mmr, preferred_role, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(discord_id) DO UPDATE SET
                    display_name = excluded.display_name,
                    battletag = excluded.battletag,
                    mmr = excluded.mmr,
                    preferred_role = excluded.preferred_role,
                    updated_at = excluded.updated_at
                """,
                (discord_id, display_name, merged_battletag, merged_mmr, merged_role, now),
            )
            self.conn.execute(
                """
                INSERT INTO player_role_mmr (discord_id, tank_mmr, dps_mmr, support_mmr, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(discord_id) DO NOTHING
                """,
                (discord_id, merged_mmr, merged_mmr, merged_mmr, now),
            )

    def get_player(self, discord_id: int) -> Player | None:
        row = self.conn.execute(
            """
            SELECT discord_id, display_name, battletag, mmr, preferred_role
            FROM players
            WHERE discord_id = ?
            """,
            (discord_id,),
        ).fetchone()
        if row is None:
            return None
        return Player(
            discord_id=row["discord_id"],
            display_name=row["display_name"],
            battletag=row["battletag"],
            mmr=row["mmr"],
            preferred_role=row["preferred_role"],
        )

    def increment_player_reliability(
        self,
        *,
        no_show_ids: list[int],
        disconnect_ids: list[int],
    ) -> None:
        unique_no_show = sorted({discord_id for discord_id in no_show_ids if discord_id > 0})
        unique_disconnect = sorted({discord_id for discord_id in disconnect_ids if discord_id > 0})
        with self.conn:
            if unique_no_show:
                self.conn.executemany(
                    """
                    UPDATE players
                    SET no_show_count = no_show_count + 1
                    WHERE discord_id = ?
                    """,
                    [(discord_id,) for discord_id in unique_no_show],
                )
            if unique_disconnect:
                self.conn.executemany(
                    """
                    UPDATE players
                    SET disconnect_count = disconnect_count + 1
                    WHERE discord_id = ?
                    """,
                    [(discord_id,) for discord_id in unique_disconnect],
                )

    def get_player_battletags(self, discord_ids: list[int]) -> dict[int, str | None]:
        unique_ids = sorted({discord_id for discord_id in discord_ids if discord_id > 0})
        if not unique_ids:
            return {}
        placeholders = ", ".join("?" for _ in unique_ids)
        rows = self.conn.execute(
            f"""
            SELECT discord_id, battletag
            FROM players
            WHERE discord_id IN ({placeholders})
            """,
            tuple(unique_ids),
        ).fetchall()
        return {int(row["discord_id"]): row["battletag"] for row in rows}

    def _result_for_team(self, team: str, winner_team: str | None) -> str:
        if winner_team is None:
            return "unknown"
        if winner_team == "Draw":
            return "draw"
        if winner_team == team:
            return "win"
        return "loss"

    def _match_results_map(self) -> dict[int, str]:
        rows = self.conn.execute(
            """
            SELECT match_id, winner_team
            FROM match_results
            """
        ).fetchall()
        return {int(row["match_id"]): row["winner_team"] for row in rows}

    def _read_match_entries(self, discord_id: int) -> list[PlayerMatchEntry]:
        rows = self.conn.execute(
            """
            SELECT id, mode, created_at, team_a_json, team_b_json
            FROM matches
            ORDER BY created_at DESC, id DESC
            """
        ).fetchall()

        result_by_match_id = self._match_results_map()
        entries: list[PlayerMatchEntry] = []
        for row in rows:
            match_id = int(row["id"])
            winner_team = result_by_match_id.get(match_id)
            for team_name, json_key in (("Team A", "team_a_json"), ("Team B", "team_b_json")):
                try:
                    players = json.loads(row[json_key])
                except (TypeError, json.JSONDecodeError):
                    continue
                if not isinstance(players, list):
                    continue

                for player in players:
                    if not isinstance(player, dict):
                        continue
                    try:
                        player_id = int(player.get("discord_id", -1))
                    except (TypeError, ValueError):
                        continue
                    if player_id != discord_id:
                        continue

                    assigned_role = str(player.get("assigned_role", "unknown"))
                    try:
                        mmr_value = int(player.get("mmr", 0))
                    except (TypeError, ValueError):
                        mmr_value = 0

                    entries.append(
                        PlayerMatchEntry(
                            match_id=match_id,
                            created_at=row["created_at"],
                            mode=row["mode"],
                            team=team_name,
                            assigned_role=assigned_role,
                            mmr=mmr_value,
                            result=self._result_for_team(team_name, winner_team),
                        )
                    )
        return entries

    def list_player_match_entries(self, discord_id: int, limit: int = 5) -> list[PlayerMatchEntry]:
        all_entries = self._read_match_entries(discord_id)
        return all_entries[: max(limit, 0)]

    def get_player_stats(self, discord_id: int) -> PlayerStats | None:
        player_row = self.conn.execute(
            """
            SELECT discord_id, display_name, battletag, mmr, preferred_role, updated_at,
                   no_show_count, disconnect_count
            FROM players
            WHERE discord_id = ?
            """,
            (discord_id,),
        ).fetchone()
        if player_row is None:
            return None

        queue_row = self.conn.execute(
            """
            SELECT role, queued_at
            FROM queue
            WHERE discord_id = ?
              AND mode = ?
            """,
            (discord_id, DEFAULT_QUEUE_STATE_KEY),
        ).fetchone()

        matches = self._read_match_entries(discord_id)
        role_counts: dict[str, int] = {}
        for entry in matches:
            role_counts[entry.assigned_role] = role_counts.get(entry.assigned_role, 0) + 1

        wins = sum(1 for entry in matches if entry.result == "win")
        losses = sum(1 for entry in matches if entry.result == "loss")
        draws = sum(1 for entry in matches if entry.result == "draw")
        results_reported = wins + losses + draws
        last_match_at = matches[0].created_at if matches else None

        return PlayerStats(
            discord_id=int(player_row["discord_id"]),
            display_name=player_row["display_name"],
            battletag=player_row["battletag"],
            mmr=int(player_row["mmr"]),
            preferred_role=player_row["preferred_role"],
            updated_at=player_row["updated_at"],
            queue_role=(queue_row["role"] if queue_row else None),
            queue_joined_at=(queue_row["queued_at"] if queue_row else None),
            matches_played=len(matches),
            last_match_at=last_match_at,
            wins=wins,
            losses=losses,
            draws=draws,
            results_reported=results_reported,
            no_show_count=int(player_row["no_show_count"]),
            disconnect_count=int(player_row["disconnect_count"]),
            assigned_role_counts=role_counts,
        )

    def list_role_rating_rows(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT p.discord_id, p.display_name, prm.tank_mmr, prm.dps_mmr, prm.support_mmr,
                   stats.games_played
            FROM players p
            JOIN player_role_mmr prm ON prm.discord_id = p.discord_id
            JOIN (
                SELECT discord_id, COUNT(*) AS games_played
                FROM match_mmr_changes
                GROUP BY discord_id
            ) stats ON stats.discord_id = p.discord_id
            """
        ).fetchall()

    def list_player_rating_rows(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT p.discord_id, p.display_name, p.mmr,
                   COALESCE(stats.games_played, 0) AS games_played
            FROM players p
            LEFT JOIN (
                SELECT discord_id, COUNT(*) AS games_played
                FROM match_mmr_changes
                GROUP BY discord_id
            ) stats ON stats.discord_id = p.discord_id
            """
        ).fetchall()

    def list_recent_matches(self, limit: int = 10) -> list[sqlite3.Row]:
        rows = self.conn.execute(
            """
            SELECT m.id, m.mode, m.created_at, m.roles_enforced,
                   r.winner_team, r.reported_at
            FROM matches m
            LEFT JOIN match_results r ON r.match_id = m.id
            ORDER BY m.id DESC
            LIMIT ?
            """,
            (max(limit, 0),),
        ).fetchall()
        return rows

    def set_match_result(self, match_id: int, winner_team: str) -> tuple[bool, str]:
        if winner_team not in VALID_WINNER_TEAMS:
            return False, "invalid winner team"

        exists = self.conn.execute(
            """
            SELECT 1
            FROM matches
            WHERE id = ?
            """,
            (match_id,),
        ).fetchone()
        if exists is None:
            return False, "match not found"

        existing = self.conn.execute(
            """
            SELECT winner_team
            FROM match_results
            WHERE match_id = ?
            """,
            (match_id,),
        ).fetchone()
        if existing is not None and existing["winner_team"] == winner_team:
            return False, "result already set to that value"

        now = utc_now_iso()
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO match_results (match_id, winner_team, reported_at)
                VALUES (?, ?, ?)
                ON CONFLICT(match_id) DO UPDATE SET
                    winner_team = excluded.winner_team,
                    reported_at = excluded.reported_at
                """,
                (match_id, winner_team, now),
            )
        return True, "result saved"

    def get_match_result(self, match_id: int) -> str | None:
        row = self.conn.execute(
            """
            SELECT winner_team
            FROM match_results
            WHERE match_id = ?
            """,
            (match_id,),
        ).fetchone()
        if row is None:
            return None
        return str(row["winner_team"])

    def recompute_match_mmr_changes(
        self,
        match_id: int,
        winner_team: str,
        *,
        k_factor: int = DEFAULT_ELO_K_FACTOR,
        calibration_matches: int = DEFAULT_CALIBRATION_MATCHES,
        calibration_multiplier: float = DEFAULT_CALIBRATION_MULTIPLIER,
    ) -> tuple[bool, list[MatchMmrChange], str]:
        if winner_team not in VALID_WINNER_TEAMS:
            return False, [], "invalid winner team"

        existing_changes = self.get_match_mmr_changes(match_id)
        if not existing_changes:
            return False, [], "mmr not applied"

        teams = self.get_match_teams(match_id)
        if teams is None:
            return False, [], "match not found"
        team_a_payload, team_b_payload = teams
        if not team_a_payload or not team_b_payload:
            return False, [], "invalid match teams"

        def _avg_mmr(payload: list[dict[str, object]]) -> int:
            values: list[int] = []
            for entry in payload:
                try:
                    values.append(self._clamp_sr(int(entry.get("mmr", self.default_mmr))))
                except (TypeError, ValueError):
                    continue
            if not values:
                return self.default_mmr
            return round(sum(values) / len(values))

        team_a_avg = _avg_mmr(team_a_payload)
        team_b_avg = _avg_mmr(team_b_payload)

        team_by_player_id: dict[int, str] = {}
        for team_name, payload in (("Team A", team_a_payload), ("Team B", team_b_payload)):
            for entry in payload:
                try:
                    discord_id = int(entry.get("discord_id", 0))
                except (TypeError, ValueError):
                    continue
                if discord_id <= 0:
                    continue
                team_by_player_id[discord_id] = team_name
        now = utc_now_iso()
        adjusted_rows = 0

        with self.conn:
            for existing in existing_changes:
                discord_id = existing.discord_id
                team_name = team_by_player_id.get(discord_id, existing.team)
                opponent_avg = team_b_avg if team_name == "Team A" else team_a_avg
                if winner_team == "Draw":
                    score = 0.5
                else:
                    score = 1.0 if winner_team == team_name else 0.0

                expected = self._expected_score(existing.mmr_before, opponent_avg)
                prior_games_row = self.conn.execute(
                    """
                    SELECT COUNT(*) AS games_played
                    FROM match_mmr_changes
                    WHERE discord_id = ?
                      AND match_id < ?
                    """,
                    (discord_id, match_id),
                ).fetchone()
                prior_games = int(prior_games_row["games_played"]) if prior_games_row is not None else 0
                multiplier = 1.0
                if calibration_matches > 0 and calibration_multiplier > 1.0 and prior_games < calibration_matches:
                    multiplier = calibration_multiplier
                desired_delta = int(round(k_factor * (score - expected) * multiplier))
                existing_effective_delta = int(existing.mmr_after) - int(existing.mmr_before)
                desired_mmr_after = self._clamp_sr(int(existing.mmr_before) + desired_delta)
                desired_effective_delta = desired_mmr_after - int(existing.mmr_before)
                correction = desired_effective_delta - existing_effective_delta
                if correction == 0:
                    continue

                player_row = self.conn.execute(
                    """
                    SELECT mmr
                    FROM players
                    WHERE discord_id = ?
                    """,
                    (discord_id,),
                ).fetchone()
                player_after = desired_mmr_after
                if player_row is not None:
                    player_after = self._clamp_sr(int(player_row["mmr"]) + correction)
                    self.conn.execute(
                        """
                        UPDATE players
                        SET mmr = ?, updated_at = ?
                        WHERE discord_id = ?
                        """,
                        (player_after, now, discord_id),
                    )

                self.conn.execute(
                    """
                    INSERT INTO player_role_mmr (discord_id, tank_mmr, dps_mmr, support_mmr, updated_at)
                    SELECT discord_id, mmr, mmr, mmr, ?
                    FROM players
                    WHERE discord_id = ?
                    ON CONFLICT(discord_id) DO NOTHING
                    """,
                    (now, discord_id),
                )

                self.conn.execute(
                    """
                    UPDATE player_role_mmr
                    SET tank_mmr = ?, dps_mmr = ?, support_mmr = ?, updated_at = ?
                    WHERE discord_id = ?
                    """,
                    (player_after, player_after, player_after, now, discord_id),
                )

                self.conn.execute(
                    """
                    UPDATE match_mmr_changes
                    SET delta = ?, mmr_after = ?
                    WHERE match_id = ?
                      AND discord_id = ?
                    """,
                    (
                        desired_delta,
                        desired_mmr_after,
                        match_id,
                        discord_id,
                    ),
                )
                adjusted_rows += 1

        updated_changes = self.get_match_mmr_changes(match_id)
        if adjusted_rows == 0:
            return True, updated_changes, "mmr already matched result"
        return True, updated_changes, "mmr corrected for updated result"

    def clear_match_results(self) -> int:
        with self.conn:
            result = self.conn.execute(
                """
                DELETE FROM match_results
                """
            )
        return result.rowcount

    def get_match_mmr_changes(self, match_id: int) -> list[MatchMmrChange]:
        rows = self.conn.execute(
            """
            SELECT match_id, discord_id, display_name, team, mmr_before, delta, mmr_after
            FROM match_mmr_changes
            WHERE match_id = ?
            ORDER BY team ASC, mmr_before DESC, discord_id ASC
            """,
            (match_id,),
        ).fetchall()
        return [
            MatchMmrChange(
                match_id=int(row["match_id"]),
                discord_id=int(row["discord_id"]),
                display_name=row["display_name"],
                team=row["team"],
                mmr_before=int(row["mmr_before"]),
                delta=int(row["delta"]),
                mmr_after=int(row["mmr_after"]),
            )
            for row in rows
        ]

    def _expected_score(self, rating_a: int, rating_b: int) -> float:
        return 1.0 / (1.0 + pow(10.0, (rating_b - rating_a) / 400.0))

    def apply_match_mmr_changes(
        self,
        match_id: int,
        winner_team: str,
        *,
        k_factor: int = DEFAULT_ELO_K_FACTOR,
        calibration_matches: int = DEFAULT_CALIBRATION_MATCHES,
        calibration_multiplier: float = DEFAULT_CALIBRATION_MULTIPLIER,
    ) -> tuple[bool, list[MatchMmrChange], str]:
        if winner_team not in VALID_WINNER_TEAMS:
            return False, [], "invalid winner team"

        existing = self.get_match_mmr_changes(match_id)
        if existing:
            return False, existing, "mmr already applied"

        teams = self.get_match_teams(match_id)
        if teams is None:
            return False, [], "match not found"
        team_a_payload, team_b_payload = teams
        if not team_a_payload or not team_b_payload:
            return False, [], "invalid match teams"

        def _avg_mmr(payload: list[dict[str, object]]) -> int:
            values: list[int] = []
            for entry in payload:
                try:
                    values.append(self._clamp_sr(int(entry.get("mmr", self.default_mmr))))
                except (TypeError, ValueError):
                    continue
            if not values:
                return self.default_mmr
            return round(sum(values) / len(values))

        team_a_avg = _avg_mmr(team_a_payload)
        team_b_avg = _avg_mmr(team_b_payload)

        if winner_team == "Draw":
            score_a = 0.5
            score_b = 0.5
        else:
            score_a = 1.0 if winner_team == "Team A" else 0.0
            score_b = 1.0 if winner_team == "Team B" else 0.0

        now = utc_now_iso()
        changes: list[MatchMmrChange] = []
        prior_games: dict[int, int] = {}
        for payload in (team_a_payload, team_b_payload):
            for entry in payload:
                try:
                    discord_id = int(entry.get("discord_id", 0))
                except (TypeError, ValueError):
                    continue
                if discord_id <= 0 or discord_id in prior_games:
                    continue
                games_row = self.conn.execute(
                    """
                    SELECT COUNT(*) AS games_played
                    FROM match_mmr_changes
                    WHERE discord_id = ?
                    """,
                    (discord_id,),
                ).fetchone()
                prior_games[discord_id] = int(games_row["games_played"]) if games_row is not None else 0

        def _apply_team(team: str, payload: list[dict[str, object]], score: float, opponent_avg: int) -> None:
            for entry in payload:
                try:
                    discord_id = int(entry.get("discord_id", 0))
                except (TypeError, ValueError):
                    continue
                if discord_id <= 0:
                    continue

                display_name = str(entry.get("display_name", f"User {discord_id}"))
                preferred_role = str(entry.get("preferred_role", self.default_role))
                try:
                    seeded_mmr = self._clamp_sr(int(entry.get("mmr", self.default_mmr)))
                except (TypeError, ValueError):
                    seeded_mmr = self._clamp_sr(self.default_mmr)

                existing_player = self.conn.execute(
                    """
                    SELECT mmr
                    FROM players
                    WHERE discord_id = ?
                    """,
                    (discord_id,),
                ).fetchone()

                if existing_player is None:
                    mmr_before = seeded_mmr
                    self.conn.execute(
                        """
                        INSERT INTO players (discord_id, display_name, battletag, mmr, preferred_role, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (discord_id, display_name, None, mmr_before, preferred_role, now),
                    )
                    self.conn.execute(
                        """
                        INSERT INTO player_role_mmr (discord_id, tank_mmr, dps_mmr, support_mmr, updated_at)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(discord_id) DO NOTHING
                        """,
                        (discord_id, mmr_before, mmr_before, mmr_before, now),
                    )
                else:
                    mmr_before = self._clamp_sr(int(existing_player["mmr"]))
                    self.conn.execute(
                        """
                        INSERT INTO player_role_mmr (discord_id, tank_mmr, dps_mmr, support_mmr, updated_at)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(discord_id) DO NOTHING
                        """,
                        (discord_id, mmr_before, mmr_before, mmr_before, now),
                    )

                expected = self._expected_score(mmr_before, opponent_avg)
                multiplier = 1.0
                if calibration_matches > 0 and calibration_multiplier > 1.0:
                    if prior_games.get(discord_id, 0) < calibration_matches:
                        multiplier = calibration_multiplier
                delta = int(round(k_factor * (score - expected) * multiplier))
                mmr_after = self._clamp_sr(mmr_before + delta)
                self.conn.execute(
                    """
                    UPDATE players
                    SET display_name = ?, mmr = ?, updated_at = ?
                    WHERE discord_id = ?
                    """,
                    (display_name, mmr_after, now, discord_id),
                )
                self.conn.execute(
                    """
                    UPDATE player_role_mmr
                    SET tank_mmr = ?, dps_mmr = ?, support_mmr = ?, updated_at = ?
                    WHERE discord_id = ?
                    """,
                    (mmr_after, mmr_after, mmr_after, now, discord_id),
                )
                self.conn.execute(
                    """
                    INSERT INTO match_mmr_changes (match_id, discord_id, display_name, team, mmr_before, delta, mmr_after)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (match_id, discord_id, display_name, team, mmr_before, delta, mmr_after),
                )
                changes.append(
                    MatchMmrChange(
                        match_id=match_id,
                        discord_id=discord_id,
                        display_name=display_name,
                        team=team,
                        mmr_before=mmr_before,
                        delta=delta,
                        mmr_after=mmr_after,
                    )
                )

        with self.conn:
            _apply_team("Team A", team_a_payload, score_a, team_b_avg)
            _apply_team("Team B", team_b_payload, score_b, team_a_avg)

        changes.sort(key=lambda c: (c.team, -c.mmr_before, c.discord_id))
        return True, changes, "mmr applied"

    def get_match_row(self, match_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            """
            SELECT id, mode, created_at, roles_enforced, team_a_json, team_b_json
            FROM matches
            WHERE id = ?
            """,
            (match_id,),
        ).fetchone()

    def _decode_team_json(self, payload: str) -> list[dict[str, object]]:
        try:
            raw = json.loads(payload)
        except (TypeError, json.JSONDecodeError):
            return []
        if not isinstance(raw, list):
            return []
        decoded: list[dict[str, object]] = []
        for entry in raw:
            if isinstance(entry, dict):
                decoded.append(entry)
        return decoded

    def get_match_teams(self, match_id: int) -> tuple[list[dict[str, object]], list[dict[str, object]]] | None:
        row = self.get_match_row(match_id)
        if row is None:
            return None
        return self._decode_team_json(row["team_a_json"]), self._decode_team_json(row["team_b_json"])

    def get_player_team_for_match(self, match_id: int, discord_id: int) -> str | None:
        teams = self.get_match_teams(match_id)
        if teams is None:
            return None
        team_a, team_b = teams
        for entry in team_a:
            try:
                if int(entry.get("discord_id", -1)) == discord_id:
                    return "Team A"
            except (TypeError, ValueError):
                continue
        for entry in team_b:
            try:
                if int(entry.get("discord_id", -1)) == discord_id:
                    return "Team B"
            except (TypeError, ValueError):
                continue
        return None

    def clear_match_reports(self, match_id: int) -> int:
        with self.conn:
            result = self.conn.execute(
                """
                DELETE FROM match_reports
                WHERE match_id = ?
                """,
                (match_id,),
            )
        return result.rowcount

    def upsert_match_report(
        self,
        match_id: int,
        team: str,
        reported_winner_team: str,
        reporter_id: int,
    ) -> tuple[bool, str]:
        if team not in {"Team A", "Team B"}:
            return False, "invalid reporting team"
        if reported_winner_team not in VALID_WINNER_TEAMS:
            return False, "invalid reported winner"

        now = utc_now_iso()
        existing = self.conn.execute(
            """
            SELECT reported_winner_team, reporter_id
            FROM match_reports
            WHERE match_id = ?
              AND team = ?
            """,
            (match_id, team),
        ).fetchone()
        if (
            existing is not None
            and existing["reported_winner_team"] == reported_winner_team
            and int(existing["reporter_id"]) == reporter_id
        ):
            return False, "report already submitted"
        if existing is not None and int(existing["reporter_id"]) != reporter_id:
            return False, "your team already has a report from another teammate"
        result_message = "report updated" if existing is not None else "report saved"

        with self.conn:
            self.conn.execute(
                """
                INSERT INTO match_reports (match_id, team, reported_winner_team, reporter_id, reported_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(match_id, team) DO UPDATE SET
                    reported_winner_team = excluded.reported_winner_team,
                    reporter_id = excluded.reporter_id,
                    reported_at = excluded.reported_at
                """,
                (match_id, team, reported_winner_team, reporter_id, now),
            )
        return True, result_message

    def get_match_reports(self, match_id: int) -> dict[str, sqlite3.Row]:
        rows = self.conn.execute(
            """
            SELECT team, reported_winner_team, reporter_id, reported_at
            FROM match_reports
            WHERE match_id = ?
            """,
            (match_id,),
        ).fetchall()
        return {row["team"]: row for row in rows}

    def set_match_ready(self, match_id: int, discord_id: int) -> tuple[bool, str]:
        existing = self.conn.execute(
            """
            SELECT 1
            FROM match_ready
            WHERE match_id = ?
              AND discord_id = ?
            """,
            (match_id, discord_id),
        ).fetchone()
        if existing is not None:
            return False, "already ready"
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO match_ready (match_id, discord_id, ready_at)
                VALUES (?, ?, ?)
                """,
                (match_id, discord_id, utc_now_iso()),
            )
        return True, "ready recorded"

    def clear_match_ready(self, match_id: int) -> int:
        with self.conn:
            result = self.conn.execute(
                """
                DELETE FROM match_ready
                WHERE match_id = ?
                """,
                (match_id,),
            )
        return result.rowcount

    def list_match_ready_ids(self, match_id: int) -> list[int]:
        rows = self.conn.execute(
            """
            SELECT discord_id
            FROM match_ready
            WHERE match_id = ?
            ORDER BY ready_at ASC
            """,
            (match_id,),
        ).fetchall()
        return [int(row["discord_id"]) for row in rows]

    def get_match_captain(self, match_id: int) -> MatchCaptain | None:
        row = self.conn.execute(
            """
            SELECT match_id, captain_id, selected_by, selected_at, selection_method
            FROM match_captain
            WHERE match_id = ?
            """,
            (match_id,),
        ).fetchone()
        if row is None:
            return None
        return MatchCaptain(
            match_id=int(row["match_id"]),
            captain_id=int(row["captain_id"]),
            selected_by=int(row["selected_by"]),
            selected_at=row["selected_at"],
            selection_method=row["selection_method"],
        )

    def set_match_captain(
        self,
        *,
        match_id: int,
        captain_id: int,
        selected_by: int,
        selection_method: str,
    ) -> tuple[bool, str]:
        if selection_method not in {"admin_auto", "first_claim"}:
            return False, "invalid selection method"
        existing = self.get_match_captain(match_id)
        if existing is not None:
            return False, "captain already set"
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO match_captain (match_id, captain_id, selected_by, selected_at, selection_method)
                VALUES (?, ?, ?, ?, ?)
                """,
                (match_id, captain_id, selected_by, utc_now_iso(), selection_method),
            )
        return True, "captain set"

    def clear_match_captain(self, match_id: int) -> int:
        with self.conn:
            result = self.conn.execute(
                """
                DELETE FROM match_captain
                WHERE match_id = ?
                """,
                (match_id,),
            )
        return result.rowcount

    def get_active_match(self) -> ActiveMatch | None:
        row = self.conn.execute(
            """
            SELECT match_id, channel_id, message_id, status, map_name, ready_deadline, started_at,
                   team_a_voice_channel_id, team_b_voice_channel_id, escalated
            FROM active_match
            WHERE id = 1
            """
        ).fetchone()
        if row is None:
            return None
        return ActiveMatch(
            match_id=int(row["match_id"]),
            channel_id=int(row["channel_id"]),
            message_id=int(row["message_id"]),
            status=row["status"],
            map_name=row["map_name"],
            ready_deadline=row["ready_deadline"],
            started_at=row["started_at"],
            team_a_voice_channel_id=row["team_a_voice_channel_id"],
            team_b_voice_channel_id=row["team_b_voice_channel_id"],
            escalated=bool(row["escalated"]),
        )

    def set_active_match(
        self,
        *,
        match_id: int,
        channel_id: int,
        message_id: int,
        status: str,
        map_name: str | None = None,
        ready_deadline: str | None = None,
        started_at: str | None = None,
        team_a_voice_channel_id: int | None = None,
        team_b_voice_channel_id: int | None = None,
        escalated: bool = False,
    ) -> ActiveMatch:
        if status not in VALID_ACTIVE_MATCH_STATUSES:
            raise ValueError("Invalid active match status.")
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO active_match (
                    id, match_id, channel_id, message_id, status, map_name, ready_deadline, started_at,
                    team_a_voice_channel_id, team_b_voice_channel_id, escalated
                )
                VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    match_id = excluded.match_id,
                    channel_id = excluded.channel_id,
                    message_id = excluded.message_id,
                    status = excluded.status,
                    map_name = excluded.map_name,
                    ready_deadline = excluded.ready_deadline,
                    started_at = excluded.started_at,
                    team_a_voice_channel_id = excluded.team_a_voice_channel_id,
                    team_b_voice_channel_id = excluded.team_b_voice_channel_id,
                    escalated = excluded.escalated
                """,
                (
                    match_id,
                    channel_id,
                    message_id,
                    status,
                    map_name,
                    ready_deadline,
                    started_at,
                    team_a_voice_channel_id,
                    team_b_voice_channel_id,
                    int(escalated),
                ),
            )
        active = self.get_active_match()
        if active is None:
            raise RuntimeError("Failed to set active match.")
        return active

    def update_active_match(
        self,
        *,
        status: str | None = None,
        message_id: int | None | object = _UNSET,
        map_name: str | None | object = _UNSET,
        ready_deadline: str | None | object = _UNSET,
        started_at: str | None | object = _UNSET,
        team_a_voice_channel_id: int | None | object = _UNSET,
        team_b_voice_channel_id: int | None | object = _UNSET,
        escalated: bool | None | object = _UNSET,
    ) -> ActiveMatch | None:
        current = self.get_active_match()
        if current is None:
            return None
        next_status = status if status is not None else current.status
        if next_status not in VALID_ACTIVE_MATCH_STATUSES:
            raise ValueError("Invalid active match status.")
        with self.conn:
            self.conn.execute(
                """
                UPDATE active_match
                SET status = ?,
                    message_id = ?,
                    map_name = ?,
                    ready_deadline = ?,
                    started_at = ?,
                    team_a_voice_channel_id = ?,
                    team_b_voice_channel_id = ?,
                    escalated = ?
                WHERE id = 1
                """,
                (
                    next_status,
                    current.message_id if message_id is _UNSET else message_id,
                    current.map_name if map_name is _UNSET else map_name,
                    current.ready_deadline if ready_deadline is _UNSET else ready_deadline,
                    current.started_at if started_at is _UNSET else started_at,
                    team_a_voice_channel_id
                    if team_a_voice_channel_id is not _UNSET
                    else current.team_a_voice_channel_id,
                    team_b_voice_channel_id
                    if team_b_voice_channel_id is not _UNSET
                    else current.team_b_voice_channel_id,
                    int(current.escalated) if escalated is _UNSET else int(bool(escalated)),
                ),
            )
        return self.get_active_match()

    def clear_active_match(self) -> int:
        with self.conn:
            result = self.conn.execute(
                """
                DELETE FROM active_match
                WHERE id = 1
                """
            )
        return result.rowcount

    def get_queue_config(self) -> QueueConfig:
        row = self.conn.execute(
            """
            SELECT queue_channel_id, queue_message_id, queue_mode,
                   players_per_match, tank_per_team, dps_per_team, support_per_team,
                   main_voice_channel_id, team_a_voice_channel_id, team_b_voice_channel_id
            FROM queue_config
            WHERE id = 1
            """
        ).fetchone()
        if row is None:
            raise RuntimeError("Queue config row is missing.")
        return QueueConfig(
            queue_channel_id=row["queue_channel_id"],
            queue_message_id=row["queue_message_id"],
            queue_mode=DEFAULT_QUEUE_MODE,
            players_per_match=row["players_per_match"],
            tank_per_team=row["tank_per_team"],
            dps_per_team=row["dps_per_team"],
            support_per_team=row["support_per_team"],
            main_voice_channel_id=row["main_voice_channel_id"],
            team_a_voice_channel_id=row["team_a_voice_channel_id"],
            team_b_voice_channel_id=row["team_b_voice_channel_id"],
        )

    def update_queue_config(
        self,
        *,
        queue_mode: str | None = None,
        players_per_match: int | None = None,
        tank_per_team: int | None = None,
        dps_per_team: int | None = None,
        support_per_team: int | None = None,
        main_voice_channel_id: int | None = None,
        team_a_voice_channel_id: int | None = None,
        team_b_voice_channel_id: int | None = None,
        queue_channel_id: int | None = None,
        queue_message_id: int | None = None,
    ) -> QueueConfig:
        current = self.get_queue_config()
        with self.conn:
            self.conn.execute(
                """
                UPDATE queue_config
                SET queue_mode = ?,
                    players_per_match = ?,
                    tank_per_team = ?,
                    dps_per_team = ?,
                    support_per_team = ?,
                    main_voice_channel_id = ?,
                    team_a_voice_channel_id = ?,
                    team_b_voice_channel_id = ?,
                    queue_channel_id = ?,
                    queue_message_id = ?
                WHERE id = 1
                """,
                (
                    DEFAULT_QUEUE_MODE,
                    players_per_match if players_per_match is not None else current.players_per_match,
                    tank_per_team if tank_per_team is not None else current.tank_per_team,
                    dps_per_team if dps_per_team is not None else current.dps_per_team,
                    support_per_team if support_per_team is not None else current.support_per_team,
                    main_voice_channel_id if main_voice_channel_id is not None else current.main_voice_channel_id,
                    team_a_voice_channel_id
                    if team_a_voice_channel_id is not None
                    else current.team_a_voice_channel_id,
                    team_b_voice_channel_id
                    if team_b_voice_channel_id is not None
                    else current.team_b_voice_channel_id,
                    queue_channel_id if queue_channel_id is not None else current.queue_channel_id,
                    queue_message_id if queue_message_id is not None else current.queue_message_id,
                ),
            )
        return self.get_queue_config()

    def set_queue_channel(self, channel_id: int) -> QueueConfig:
        return self.update_queue_config(queue_channel_id=channel_id, queue_message_id=0)

    def set_queue_message(self, message_id: int) -> QueueConfig:
        return self.update_queue_config(queue_message_id=message_id)

    def clear_queue_message(self) -> QueueConfig:
        with self.conn:
            self.conn.execute(
                """
                UPDATE queue_config
                SET queue_message_id = 0
                WHERE id = 1
                """
            )
        return self.get_queue_config()

    def get_modmail_config(self) -> ModmailConfig:
        row = self.conn.execute(
            """
            SELECT panel_channel_id, panel_message_id, logs_channel_id
            FROM modmail_config
            WHERE id = 1
            """
        ).fetchone()
        if row is None:
            raise RuntimeError("Modmail config row is missing.")
        return ModmailConfig(
            panel_channel_id=row["panel_channel_id"],
            panel_message_id=row["panel_message_id"],
            logs_channel_id=row["logs_channel_id"],
        )

    def update_modmail_config(
        self,
        *,
        panel_channel_id: int | None | object = _UNSET,
        panel_message_id: int | None | object = _UNSET,
        logs_channel_id: int | None | object = _UNSET,
    ) -> ModmailConfig:
        current = self.get_modmail_config()
        with self.conn:
            self.conn.execute(
                """
                UPDATE modmail_config
                SET panel_channel_id = ?,
                    panel_message_id = ?,
                    logs_channel_id = ?
                WHERE id = 1
                """,
                (
                    current.panel_channel_id if panel_channel_id is _UNSET else panel_channel_id,
                    current.panel_message_id if panel_message_id is _UNSET else panel_message_id,
                    current.logs_channel_id if logs_channel_id is _UNSET else logs_channel_id,
                ),
            )
        return self.get_modmail_config()

    def set_modmail_channel(self, channel_id: int) -> ModmailConfig:
        return self.update_modmail_config(panel_channel_id=channel_id, panel_message_id=0)

    def set_modmail_message(self, message_id: int) -> ModmailConfig:
        return self.update_modmail_config(panel_message_id=message_id)

    def clear_modmail_message(self) -> ModmailConfig:
        return self.update_modmail_config(panel_message_id=0)

    def set_modmail_logs_channel(self, channel_id: int) -> ModmailConfig:
        return self.update_modmail_config(logs_channel_id=channel_id)

    def _row_to_modmail_ticket(self, row: sqlite3.Row | None) -> ModmailTicket | None:
        if row is None:
            return None
        return ModmailTicket(
            ticket_id=int(row["id"]),
            guild_id=int(row["guild_id"]),
            user_id=int(row["user_id"]),
            thread_id=int(row["thread_id"]),
            status=str(row["status"]),
            created_at=str(row["created_at"]),
            closed_at=row["closed_at"],
            closed_by=(int(row["closed_by"]) if row["closed_by"] is not None else None),
        )

    def get_open_modmail_ticket(self, guild_id: int, user_id: int) -> ModmailTicket | None:
        row = self.conn.execute(
            """
            SELECT id, guild_id, user_id, thread_id, status, created_at, closed_at, closed_by
            FROM modmail_tickets
            WHERE guild_id = ?
              AND user_id = ?
              AND status = 'open'
            ORDER BY id DESC
            LIMIT 1
            """,
            (guild_id, user_id),
        ).fetchone()
        return self._row_to_modmail_ticket(row)

    def get_modmail_ticket_by_thread(self, thread_id: int) -> ModmailTicket | None:
        row = self.conn.execute(
            """
            SELECT id, guild_id, user_id, thread_id, status, created_at, closed_at, closed_by
            FROM modmail_tickets
            WHERE thread_id = ?
            LIMIT 1
            """,
            (thread_id,),
        ).fetchone()
        return self._row_to_modmail_ticket(row)

    def create_modmail_ticket(self, *, guild_id: int, user_id: int, thread_id: int) -> ModmailTicket:
        now = utc_now_iso()
        with self.conn:
            cursor = self.conn.execute(
                """
                INSERT INTO modmail_tickets (guild_id, user_id, thread_id, status, created_at, closed_at, closed_by)
                VALUES (?, ?, ?, 'open', ?, NULL, NULL)
                """,
                (guild_id, user_id, thread_id, now),
            )
        ticket = self.get_modmail_ticket_by_thread(thread_id)
        if ticket is None:
            raise RuntimeError(f"Failed to create modmail ticket: {cursor.lastrowid}")
        return ticket

    def close_open_modmail_tickets_for_user(
        self,
        *,
        guild_id: int,
        user_id: int,
        closed_by: int | None,
    ) -> int:
        now = utc_now_iso()
        with self.conn:
            result = self.conn.execute(
                """
                UPDATE modmail_tickets
                SET status = 'closed',
                    closed_at = ?,
                    closed_by = ?
                WHERE guild_id = ?
                  AND user_id = ?
                  AND status = 'open'
                """,
                (now, closed_by, guild_id, user_id),
            )
        return result.rowcount

    def close_modmail_ticket_by_thread(self, *, thread_id: int, closed_by: int | None) -> bool:
        now = utc_now_iso()
        with self.conn:
            result = self.conn.execute(
                """
                UPDATE modmail_tickets
                SET status = 'closed',
                    closed_at = ?,
                    closed_by = ?
                WHERE thread_id = ?
                  AND status = 'open'
                """,
                (now, closed_by, thread_id),
            )
        return result.rowcount > 0

    def get_queue_entry(self, discord_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            """
            SELECT discord_id, mode, role, queued_at
            FROM queue
            WHERE discord_id = ?
              AND mode = ?
            """,
            (discord_id, DEFAULT_QUEUE_STATE_KEY),
        ).fetchone()

    def upsert_queue_entry(self, discord_id: int, role: str) -> tuple[bool, str]:
        _ = role
        target_role = DEFAULT_QUEUE_ENTRY_ROLE
        existing = self.get_queue_entry(discord_id)
        if existing is None:
            with self.conn:
                self.conn.execute(
                    """
                    INSERT INTO queue (discord_id, mode, role, queued_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (discord_id, DEFAULT_QUEUE_STATE_KEY, target_role, utc_now_iso()),
                )
            return True, "joined"

        if existing["role"] == target_role:
            return False, "already queued"

        with self.conn:
            self.conn.execute(
                """
                UPDATE queue
                SET role = ?
                WHERE discord_id = ?
                  AND mode = ?
                """,
                (target_role, discord_id, DEFAULT_QUEUE_STATE_KEY),
            )
        return True, "queue updated"

    def remove_queue_entry(self, discord_id: int) -> bool:
        with self.conn:
            result = self.conn.execute(
                """
                DELETE FROM queue
                WHERE discord_id = ?
                  AND mode = ?
                """,
                (discord_id, DEFAULT_QUEUE_STATE_KEY),
            )
        return result.rowcount > 0

    def clear_queue(self) -> int:
        with self.conn:
            result = self.conn.execute(
                """
                DELETE FROM queue
                WHERE mode = ?
                """,
                (DEFAULT_QUEUE_STATE_KEY,),
            )
        return result.rowcount

    def queue_count(self) -> int:
        row = self.conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM queue
            WHERE mode = ?
            """,
            (DEFAULT_QUEUE_STATE_KEY,),
        ).fetchone()
        return int(row["count"]) if row else 0

    def count_role(self, role: str) -> int:
        row = self.conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM queue
            WHERE mode = ?
              AND role = ?
            """,
            (DEFAULT_QUEUE_STATE_KEY, role),
        ).fetchone()
        return int(row["count"]) if row else 0

    def list_queue(self) -> list[QueuedPlayer]:
        rows = self.conn.execute(
            """
            SELECT q.discord_id, p.display_name, p.mmr, q.role, q.queued_at
            FROM queue q
            JOIN players p ON q.discord_id = p.discord_id
            WHERE q.mode = ?
            ORDER BY q.queued_at ASC, q.id ASC
            """,
            (DEFAULT_QUEUE_STATE_KEY,),
        ).fetchall()
        return [
            QueuedPlayer(
                discord_id=row["discord_id"],
                display_name=row["display_name"],
                mmr=row["mmr"],
                role=row["role"],
                queued_at=row["queued_at"],
            )
            for row in rows
        ]

    def dequeue_many(self, discord_ids: list[int]) -> None:
        if not discord_ids:
            return
        with self.conn:
            self.conn.executemany(
                """
                DELETE FROM queue
                WHERE discord_id = ?
                  AND mode = ?
                """,
                [(discord_id, DEFAULT_QUEUE_STATE_KEY) for discord_id in discord_ids],
            )

    def set_all_queue_roles(self, role: str) -> None:
        _ = role
        with self.conn:
            self.conn.execute(
                """
                UPDATE queue
                SET role = ?
                WHERE mode = ?
                """,
                (DEFAULT_QUEUE_ENTRY_ROLE, DEFAULT_QUEUE_STATE_KEY),
            )

    def normalize_queue_roles_for_role_mode(self) -> None:
        self._normalize_queue_entry_roles()

    def record_match(self, mode: str, team_a: Team, team_b: Team, roles_enforced: bool) -> int:
        now = utc_now_iso()
        team_a_json = json.dumps(
            [
                {
                    "discord_id": p.discord_id,
                    "display_name": p.display_name,
                    "mmr": p.mmr,
                    "preferred_role": p.preferred_role,
                    "assigned_role": p.assigned_role,
                }
                for p in team_a.players
            ]
        )
        team_b_json = json.dumps(
            [
                {
                    "discord_id": p.discord_id,
                    "display_name": p.display_name,
                    "mmr": p.mmr,
                    "preferred_role": p.preferred_role,
                    "assigned_role": p.assigned_role,
                }
                for p in team_b.players
            ]
        )
        with self.conn:
            cursor = self.conn.execute(
                """
                INSERT INTO matches (mode, created_at, roles_enforced, team_a_json, team_b_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (mode, now, int(roles_enforced), team_a_json, team_b_json),
            )
        return int(cursor.lastrowid)
