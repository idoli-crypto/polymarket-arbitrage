from __future__ import annotations

from pathlib import Path
import os
import tempfile
import unittest

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text

from apps.api.config.settings import get_settings


class V2ContractMigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temp_dir.name) / "migration-test.db"
        self.database_url = f"sqlite:///{self.database_path}"
        self.previous_database_url = os.environ.get("DATABASE_URL")
        os.environ["DATABASE_URL"] = self.database_url
        get_settings.cache_clear()

    def tearDown(self) -> None:
        if self.previous_database_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = self.previous_database_url
        get_settings.cache_clear()
        self.temp_dir.cleanup()

    def test_upgrade_head_on_empty_database_creates_v2_contracts(self) -> None:
        command.upgrade(self._alembic_config(), "head")

        inspector = inspect(create_engine(self.database_url))
        detected_columns = {column["name"] for column in inspector.get_columns("detected_opportunities")}
        market_columns = {column["name"] for column in inspector.get_columns("markets")}
        snapshot_columns = {column["name"] for column in inspector.get_columns("market_snapshots")}
        simulation_result_columns = {column["name"] for column in inspector.get_columns("simulation_results")}
        opportunity_kpi_columns = {column["name"] for column in inspector.get_columns("opportunity_kpi_snapshots")}
        run_summary_columns = {column["name"] for column in inspector.get_columns("kpi_run_summary")}
        self.assertIn("family", detected_columns)
        self.assertIn("confidence_tier", detected_columns)
        self.assertIn("recommendation_eligibility", detected_columns)
        self.assertIn("simulation_version", detected_columns)
        self.assertIn("validation_version", detected_columns)
        self.assertIn("raw_market_json", market_columns)
        self.assertIn("order_book_json", snapshot_columns)
        self.assertIn("validation_results", inspector.get_table_names())
        self.assertIn("simulation_results", inspector.get_table_names())
        self.assertIn("recommendation_scores", inspector.get_table_names())
        self.assertIn("recommendation_scoring_runs", inspector.get_table_names())
        self.assertIn("opportunity_kpi_snapshots", inspector.get_table_names())
        self.assertIn("kpi_run_summary", inspector.get_table_names())
        self.assertIn("fill_completion_ratio", simulation_result_columns)
        self.assertIn("execution_feasible", simulation_result_columns)
        self.assertIn("execution_risk_flag", simulation_result_columns)
        self.assertIn("validation_stage_reached", opportunity_kpi_columns)
        self.assertIn("persistence_duration_seconds", opportunity_kpi_columns)
        self.assertIn("decay_status", opportunity_kpi_columns)
        self.assertIn("valid_after_simulation", run_summary_columns)
        self.assertIn("family_distribution", run_summary_columns)

    def test_upgrade_existing_database_backfills_required_fields(self) -> None:
        command.upgrade(self._alembic_config(), "0006")

        engine = create_engine(self.database_url)
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO detected_opportunities (
                        detection_window_start,
                        event_group_key,
                        involved_market_ids,
                        opportunity_type,
                        outcome_count,
                        gross_price_sum,
                        gross_gap,
                        detector_version,
                        status
                    ) VALUES (
                        '2026-04-22 10:00:00',
                        'event-1',
                        '[1, 2]',
                        'neg_risk_long_yes_bundle',
                        2,
                        0.6300,
                        0.3700,
                        'neg_risk_v1',
                        'detected'
                    )
                    """
                )
            )

        command.upgrade(self._alembic_config(), "head")

        with engine.connect() as connection:
            row = connection.execute(
                text(
                    """
                    SELECT family, involved_market_ids_json, recommendation_eligibility
                    FROM detected_opportunities
                    WHERE event_group_key = 'event-1'
                    """
                )
            ).mappings().one()

        self.assertEqual(row["family"], "neg_risk_conversion")
        self.assertEqual(row["recommendation_eligibility"], 0)
        self.assertEqual(row["involved_market_ids_json"], "[1, 2]")

    def _alembic_config(self) -> Config:
        config = Config(str(Path(__file__).resolve().parents[1] / "alembic.ini"))
        config.set_main_option("script_location", str(Path(__file__).resolve().parents[1] / "migrations"))
        return config


if __name__ == "__main__":
    unittest.main()
