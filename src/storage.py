from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3

from .models import Player, QueueConfig, QueuedPlayer, Team

DEFAULT_QUEUE_MODE = "role"
DEFAULT_QUEUE_STATE_KEY = "default"


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
        self.default_mmr = default_mmr
        self.default_role = default_role
        self.default_players_per_match = default_players_per_match
        self.default_tank_per_team = default_tank_per_team
        self.default_dps_per_team = default_dps_per_team
        self.default_support_per_team = default_support_per_team
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self._create_schema()
        self._normalize_queue_state()
        self._ensure_queue_config_row()

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
                CREATE TABLE IF NOT EXISTS queue_config (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    queue_channel_id INTEGER,
                    queue_message_id INTEGER,
                    queue_mode TEXT NOT NULL,
                    players_per_match INTEGER NOT NULL,
                    tank_per_team INTEGER NOT NULL,
                    dps_per_team INTEGER NOT NULL,
                    support_per_team INTEGER NOT NULL
                )
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
                    support_per_team
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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
                ),
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
        merged_mmr = mmr if mmr is not None else (current.mmr if current else self.default_mmr)
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

    def get_queue_config(self) -> QueueConfig:
        row = self.conn.execute(
            """
            SELECT queue_channel_id, queue_message_id, queue_mode,
                   players_per_match, tank_per_team, dps_per_team, support_per_team
            FROM queue_config
            WHERE id = 1
            """
        ).fetchone()
        if row is None:
            raise RuntimeError("Queue config row is missing.")
        return QueueConfig(
            queue_channel_id=row["queue_channel_id"],
            queue_message_id=row["queue_message_id"],
            queue_mode=row["queue_mode"],
            players_per_match=row["players_per_match"],
            tank_per_team=row["tank_per_team"],
            dps_per_team=row["dps_per_team"],
            support_per_team=row["support_per_team"],
        )

    def update_queue_config(
        self,
        *,
        queue_mode: str | None = None,
        players_per_match: int | None = None,
        tank_per_team: int | None = None,
        dps_per_team: int | None = None,
        support_per_team: int | None = None,
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
                    queue_channel_id = ?,
                    queue_message_id = ?
                WHERE id = 1
                """,
                (
                    queue_mode if queue_mode is not None else current.queue_mode,
                    players_per_match if players_per_match is not None else current.players_per_match,
                    tank_per_team if tank_per_team is not None else current.tank_per_team,
                    dps_per_team if dps_per_team is not None else current.dps_per_team,
                    support_per_team if support_per_team is not None else current.support_per_team,
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
        existing = self.get_queue_entry(discord_id)
        if existing is None:
            with self.conn:
                self.conn.execute(
                    """
                    INSERT INTO queue (discord_id, mode, role, queued_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (discord_id, DEFAULT_QUEUE_STATE_KEY, role, utc_now_iso()),
                )
            return True, "joined"

        if existing["role"] == role:
            return False, "already in that queue role"

        with self.conn:
            self.conn.execute(
                """
                UPDATE queue
                SET role = ?
                WHERE discord_id = ?
                  AND mode = ?
                """,
                (role, discord_id, DEFAULT_QUEUE_STATE_KEY),
            )
        return True, "role updated"

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
        with self.conn:
            self.conn.execute(
                """
                UPDATE queue
                SET role = ?
                WHERE mode = ?
                """,
                (role, DEFAULT_QUEUE_STATE_KEY),
            )

    def normalize_queue_roles_for_role_mode(self) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE queue
                SET role = 'fill'
                WHERE mode = ?
                  AND role NOT IN ('tank', 'dps', 'support', 'fill')
                """,
                (DEFAULT_QUEUE_STATE_KEY,),
            )

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
