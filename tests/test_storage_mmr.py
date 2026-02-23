from __future__ import annotations

import os
import tempfile
import unittest

from src.models import AssignedPlayer, Team
from src.storage import Database


class StorageMmrTests(unittest.TestCase):
    def setUp(self) -> None:
        fd, path = tempfile.mkstemp(prefix="ow-bot-test-", suffix=".db")
        os.close(fd)
        self.db_path = path
        self.db = Database(
            path=path,
            default_mmr=2500,
            default_role="queue",
            default_players_per_match=2,
            default_tank_per_team=1,
            default_dps_per_team=0,
            default_support_per_team=0,
        )

    def tearDown(self) -> None:
        self.db.close()
        try:
            os.remove(self.db_path)
        except FileNotFoundError:
            pass

    def _record_one_vs_one_match(self, *, player_a_mmr: int = 2500, player_b_mmr: int = 2500, role: str = "queue") -> int:
        self.db.upsert_player(discord_id=101, display_name="Alpha", mmr=player_a_mmr, preferred_role=role)
        self.db.upsert_player(discord_id=202, display_name="Bravo", mmr=player_b_mmr, preferred_role=role)

        team_a = Team(
            name="Team A",
            players=[
                AssignedPlayer(
                    discord_id=101,
                    display_name="Alpha",
                    mmr=player_a_mmr,
                    preferred_role=role,
                    assigned_role=role,
                )
            ],
        )
        team_b = Team(
            name="Team B",
            players=[
                AssignedPlayer(
                    discord_id=202,
                    display_name="Bravo",
                    mmr=player_b_mmr,
                    preferred_role=role,
                    assigned_role=role,
                )
            ],
        )
        return self.db.record_match(mode="queue", team_a=team_a, team_b=team_b, roles_enforced=False)

    @staticmethod
    def _delta_for(changes: list[object], discord_id: int) -> int:
        for change in changes:
            if int(change.discord_id) == discord_id:
                return int(change.delta)
        raise AssertionError(f"No change row for player {discord_id}")

    def _seed_completed_matches(self, count: int) -> None:
        for _ in range(count):
            match_id = self._record_one_vs_one_match(player_a_mmr=2500, player_b_mmr=2500, role="queue")
            applied, _, msg = self.db.apply_match_mmr_changes(match_id, "Team A", calibration_multiplier=1.0)
            self.assertTrue(applied, msg)

    def test_expected_score_equal_and_opposite(self) -> None:
        self.assertAlmostEqual(self.db._expected_score(2500, 2500), 0.5, places=7)

        high_vs_low = self.db._expected_score(2600, 2400)
        low_vs_high = self.db._expected_score(2400, 2600)
        self.assertGreater(high_vs_low, 0.5)
        self.assertLess(low_vs_high, 0.5)
        self.assertAlmostEqual(high_vs_low + low_vs_high, 1.0, places=7)

    def test_base_elo_delta_without_calibration_bonus(self) -> None:
        match_id = self._record_one_vs_one_match(player_a_mmr=2500, player_b_mmr=2500)
        applied, changes, message = self.db.apply_match_mmr_changes(match_id, "Team A", calibration_multiplier=1.0)
        self.assertTrue(applied, message)
        self.assertEqual(self._delta_for(changes, 101), 12)
        self.assertEqual(self._delta_for(changes, 202), -12)

    def test_calibration_bonus_applies_to_new_players(self) -> None:
        match_id = self._record_one_vs_one_match(player_a_mmr=2500, player_b_mmr=2500)
        applied, changes, message = self.db.apply_match_mmr_changes(match_id, "Team A")
        self.assertTrue(applied, message)
        self.assertEqual(self._delta_for(changes, 101), 24)
        self.assertEqual(self._delta_for(changes, 202), -24)

    def test_calibration_bonus_still_applies_on_fifth_game(self) -> None:
        self._seed_completed_matches(4)
        match_id = self._record_one_vs_one_match(player_a_mmr=2500, player_b_mmr=2500)
        applied, changes, message = self.db.apply_match_mmr_changes(match_id, "Team A")
        self.assertTrue(applied, message)
        self.assertEqual(self._delta_for(changes, 101), 24)
        self.assertEqual(self._delta_for(changes, 202), -24)

    def test_calibration_bonus_stops_after_five_completed_games(self) -> None:
        self._seed_completed_matches(5)
        match_id = self._record_one_vs_one_match(player_a_mmr=2500, player_b_mmr=2500)
        applied, changes, message = self.db.apply_match_mmr_changes(match_id, "Team A")
        self.assertTrue(applied, message)
        self.assertEqual(self._delta_for(changes, 101), 12)
        self.assertEqual(self._delta_for(changes, 202), -12)

    def test_draw_at_equal_rating_produces_zero_delta(self) -> None:
        match_id = self._record_one_vs_one_match(player_a_mmr=2500, player_b_mmr=2500)
        applied, changes, message = self.db.apply_match_mmr_changes(match_id, "Draw")
        self.assertTrue(applied, message)
        self.assertEqual(self._delta_for(changes, 101), 0)
        self.assertEqual(self._delta_for(changes, 202), 0)

    def test_apply_match_mmr_changes_is_idempotent(self) -> None:
        match_id = self._record_one_vs_one_match()
        first_applied, first_changes, first_msg = self.db.apply_match_mmr_changes(match_id, "Team A")
        self.assertTrue(first_applied, first_msg)

        second_applied, second_changes, second_msg = self.db.apply_match_mmr_changes(match_id, "Team A")
        self.assertFalse(second_applied)
        self.assertEqual(second_msg, "mmr already applied")
        self.assertEqual(len(second_changes), len(first_changes))

    def test_role_buckets_track_global_mmr(self) -> None:
        match_id = self._record_one_vs_one_match(player_a_mmr=2500, player_b_mmr=2500, role="queue")
        applied, changes, message = self.db.apply_match_mmr_changes(match_id, "Team A", calibration_multiplier=1.0)
        self.assertTrue(applied, message)
        self.assertEqual(self._delta_for(changes, 101), 12)

        role_row = self.db.conn.execute(
            """
            SELECT tank_mmr, dps_mmr, support_mmr
            FROM player_role_mmr
            WHERE discord_id = ?
            """,
            (101,),
        ).fetchone()
        self.assertIsNotNone(role_row)
        self.assertEqual(int(role_row["tank_mmr"]), 2512)
        self.assertEqual(int(role_row["dps_mmr"]), 2512)
        self.assertEqual(int(role_row["support_mmr"]), 2512)

    def test_recompute_match_mmr_changes_corrects_ratings_when_winner_changes(self) -> None:
        match_id = self._record_one_vs_one_match(player_a_mmr=2500, player_b_mmr=2500, role="dps")
        applied, changes, message = self.db.apply_match_mmr_changes(match_id, "Team A")
        self.assertTrue(applied, message)
        self.assertEqual(self._delta_for(changes, 101), 24)
        self.assertEqual(self._delta_for(changes, 202), -24)

        corrected, corrected_changes, correction_message = self.db.recompute_match_mmr_changes(match_id, "Team B")
        self.assertTrue(corrected, correction_message)
        self.assertEqual(correction_message, "mmr corrected for updated result")
        self.assertEqual(self._delta_for(corrected_changes, 101), -24)
        self.assertEqual(self._delta_for(corrected_changes, 202), 24)

        player_a = self.db.get_player(101)
        player_b = self.db.get_player(202)
        self.assertIsNotNone(player_a)
        self.assertIsNotNone(player_b)
        self.assertEqual(int(player_a.mmr), 2476)
        self.assertEqual(int(player_b.mmr), 2524)

        role_row = self.db.conn.execute(
            """
            SELECT tank_mmr, dps_mmr, support_mmr
            FROM player_role_mmr
            WHERE discord_id = ?
            """,
            (101,),
        ).fetchone()
        self.assertIsNotNone(role_row)
        self.assertEqual(int(role_row["tank_mmr"]), 2500)
        self.assertEqual(int(role_row["dps_mmr"]), 2476)
        self.assertEqual(int(role_row["support_mmr"]), 2500)

    def test_recompute_match_mmr_changes_noop_when_result_already_matches(self) -> None:
        match_id = self._record_one_vs_one_match(player_a_mmr=2500, player_b_mmr=2500, role="dps")
        applied, changes, message = self.db.apply_match_mmr_changes(match_id, "Team A")
        self.assertTrue(applied, message)

        corrected, corrected_changes, correction_message = self.db.recompute_match_mmr_changes(match_id, "Team A")
        self.assertTrue(corrected, correction_message)
        self.assertEqual(correction_message, "mmr already matched result")
        self.assertEqual(len(corrected_changes), len(changes))
        self.assertEqual(self._delta_for(corrected_changes, 101), self._delta_for(changes, 101))
        self.assertEqual(self._delta_for(corrected_changes, 202), self._delta_for(changes, 202))

    def test_recompute_match_mmr_changes_uses_effective_delta_at_rating_cap(self) -> None:
        match_id = self._record_one_vs_one_match(player_a_mmr=5000, player_b_mmr=5000, role="dps")
        applied, initial_changes, message = self.db.apply_match_mmr_changes(match_id, "Team A", calibration_multiplier=1.0)
        self.assertTrue(applied, message)
        self.assertEqual(self._delta_for(initial_changes, 101), 12)
        self.assertEqual(self._delta_for(initial_changes, 202), -12)

        player_a_after_first = self.db.get_player(101)
        player_b_after_first = self.db.get_player(202)
        self.assertIsNotNone(player_a_after_first)
        self.assertIsNotNone(player_b_after_first)
        self.assertEqual(int(player_a_after_first.mmr), 5000)
        self.assertEqual(int(player_b_after_first.mmr), 4988)

        corrected, corrected_changes, correction_message = self.db.recompute_match_mmr_changes(match_id, "Team B", calibration_multiplier=1.0)
        self.assertTrue(corrected, correction_message)
        self.assertEqual(correction_message, "mmr corrected for updated result")
        self.assertEqual(self._delta_for(corrected_changes, 101), -12)
        self.assertEqual(self._delta_for(corrected_changes, 202), 12)

        player_a_after_correction = self.db.get_player(101)
        player_b_after_correction = self.db.get_player(202)
        self.assertIsNotNone(player_a_after_correction)
        self.assertIsNotNone(player_b_after_correction)
        self.assertEqual(int(player_a_after_correction.mmr), 4988)
        self.assertEqual(int(player_b_after_correction.mmr), 5000)


if __name__ == "__main__":
    unittest.main()
