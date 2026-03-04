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

    def _record_custom_match(
        self,
        *,
        team_a_players: list[tuple[int, str, int]],
        team_b_players: list[tuple[int, str, int]],
        role: str = "queue",
    ) -> int:
        for discord_id, display_name, mmr in team_a_players + team_b_players:
            self.db.upsert_player(discord_id=discord_id, display_name=display_name, mmr=mmr, preferred_role=role)

        team_a = Team(
            name="Team A",
            players=[
                AssignedPlayer(
                    discord_id=discord_id,
                    display_name=display_name,
                    mmr=mmr,
                    preferred_role=role,
                    assigned_role=role,
                )
                for discord_id, display_name, mmr in team_a_players
            ],
        )
        team_b = Team(
            name="Team B",
            players=[
                AssignedPlayer(
                    discord_id=discord_id,
                    display_name=display_name,
                    mmr=mmr,
                    preferred_role=role,
                    assigned_role=role,
                )
                for discord_id, display_name, mmr in team_b_players
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

    def test_draw_at_unequal_rating_produces_zero_delta(self) -> None:
        match_id = self._record_one_vs_one_match(player_a_mmr=3200, player_b_mmr=1800)
        applied, changes, message = self.db.apply_match_mmr_changes(match_id, "Draw")
        self.assertTrue(applied, message)
        self.assertEqual(self._delta_for(changes, 101), 0)
        self.assertEqual(self._delta_for(changes, 202), 0)

    def test_recompute_match_to_draw_zeroes_existing_changes(self) -> None:
        match_id = self._record_one_vs_one_match(player_a_mmr=2600, player_b_mmr=2400)
        applied, initial_changes, message = self.db.apply_match_mmr_changes(match_id, "Team A", calibration_multiplier=1.0)
        self.assertTrue(applied, message)
        self.assertNotEqual(self._delta_for(initial_changes, 101), 0)
        self.assertNotEqual(self._delta_for(initial_changes, 202), 0)

        corrected, corrected_changes, correction_message = self.db.recompute_match_mmr_changes(
            match_id,
            "Draw",
            calibration_multiplier=1.0,
        )
        self.assertTrue(corrected, correction_message)
        self.assertEqual(correction_message, "mmr corrected for updated result")
        self.assertEqual(self._delta_for(corrected_changes, 101), 0)
        self.assertEqual(self._delta_for(corrected_changes, 202), 0)

        player_a = self.db.get_player(101)
        player_b = self.db.get_player(202)
        self.assertIsNotNone(player_a)
        self.assertIsNotNone(player_b)
        self.assertEqual(int(player_a.mmr), 2600)
        self.assertEqual(int(player_b.mmr), 2400)

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
        self.assertEqual(int(role_row["tank_mmr"]), 2476)
        self.assertEqual(int(role_row["dps_mmr"]), 2476)
        self.assertEqual(int(role_row["support_mmr"]), 2476)

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

    def test_set_player_mmr_updates_global_and_role_buckets(self) -> None:
        applied_mmr, created = self.db.set_player_mmr(discord_id=777, mmr=2899, display_name="Gamma")
        self.assertTrue(created)
        self.assertEqual(applied_mmr, 2899)

        player = self.db.get_player(777)
        self.assertIsNotNone(player)
        self.assertEqual(int(player.mmr), 2899)

        role_row = self.db.conn.execute(
            """
            SELECT tank_mmr, dps_mmr, support_mmr
            FROM player_role_mmr
            WHERE discord_id = ?
            """,
            (777,),
        ).fetchone()
        self.assertIsNotNone(role_row)
        self.assertEqual(int(role_row["tank_mmr"]), 2899)
        self.assertEqual(int(role_row["dps_mmr"]), 2899)
        self.assertEqual(int(role_row["support_mmr"]), 2899)

        updated_mmr, created_again = self.db.set_player_mmr(discord_id=777, mmr=3101)
        self.assertFalse(created_again)
        self.assertEqual(updated_mmr, 3101)
        player_after = self.db.get_player(777)
        self.assertIsNotNone(player_after)
        self.assertEqual(int(player_after.mmr), 3101)

    def test_match_report_votes_require_threshold_and_majority(self) -> None:
        match_id = self._record_custom_match(
            team_a_players=[
                (1001, "A1", 2500),
                (1002, "A2", 2500),
                (1003, "A3", 2500),
                (1004, "A4", 2500),
            ],
            team_b_players=[
                (2001, "B1", 2500),
                (2002, "B2", 2500),
                (2003, "B3", 2500),
                (2004, "B4", 2500),
            ],
        )

        for reporter_id in (1001, 1002, 1003, 1004, 2001, 2002):
            changed, msg = self.db.upsert_match_report(
                match_id=match_id,
                team=("Team A" if reporter_id < 2000 else "Team B"),
                reported_winner_team="Team A",
                reporter_id=reporter_id,
            )
            self.assertTrue(changed, msg)

        # Two dissenting votes should still keep Team A as the winner by majority.
        for reporter_id in (2003, 2004):
            changed, msg = self.db.upsert_match_report(
                match_id=match_id,
                team="Team B",
                reported_winner_team="Team B",
                reporter_id=reporter_id,
            )
            self.assertTrue(changed, msg)

        totals = self.db.get_match_report_vote_totals(match_id)
        self.assertEqual(totals["Team A"], 6)
        self.assertEqual(totals["Team B"], 2)
        self.assertEqual(totals["Draw"], 0)

        winner, total_votes, is_tie = self.db.resolve_match_report_winner(match_id, required_votes=6)
        self.assertEqual(total_votes, 8)
        self.assertFalse(is_tie)
        self.assertEqual(winner, "Team A")

    def test_match_report_votes_tie_at_threshold_requires_more_votes(self) -> None:
        match_id = self._record_custom_match(
            team_a_players=[
                (3001, "A1", 2500),
                (3002, "A2", 2500),
                (3003, "A3", 2500),
            ],
            team_b_players=[
                (4001, "B1", 2500),
                (4002, "B2", 2500),
                (4003, "B3", 2500),
            ],
        )
        for reporter_id in (3001, 3002, 3003):
            changed, msg = self.db.upsert_match_report(
                match_id=match_id,
                team="Team A",
                reported_winner_team="Team A",
                reporter_id=reporter_id,
            )
            self.assertTrue(changed, msg)
        for reporter_id in (4001, 4002, 4003):
            changed, msg = self.db.upsert_match_report(
                match_id=match_id,
                team="Team B",
                reported_winner_team="Team B",
                reporter_id=reporter_id,
            )
            self.assertTrue(changed, msg)

        winner, total_votes, is_tie = self.db.resolve_match_report_winner(match_id, required_votes=6)
        self.assertEqual(total_votes, 6)
        self.assertTrue(is_tie)
        self.assertIsNone(winner)

    def test_match_report_votes_plurality_without_majority_does_not_finalize(self) -> None:
        match_id = self._record_custom_match(
            team_a_players=[
                (5001, "A1", 2500),
                (5002, "A2", 2500),
                (5003, "A3", 2500),
            ],
            team_b_players=[
                (6001, "B1", 2500),
                (6002, "B2", 2500),
                (6003, "B3", 2500),
            ],
        )
        for reporter_id, team, reported_winner in (
            (5001, "Team A", "Team A"),
            (5002, "Team A", "Team A"),
            (5003, "Team A", "Team A"),
            (6001, "Team B", "Team B"),
            (6002, "Team B", "Team B"),
            (6003, "Team B", "Draw"),
        ):
            changed, msg = self.db.upsert_match_report(
                match_id=match_id,
                team=team,
                reported_winner_team=reported_winner,
                reporter_id=reporter_id,
            )
            self.assertTrue(changed, msg)

        winner, total_votes, is_tie = self.db.resolve_match_report_winner(match_id, required_votes=6)
        self.assertEqual(total_votes, 6)
        self.assertTrue(is_tie)
        self.assertIsNone(winner)


if __name__ == "__main__":
    unittest.main()
