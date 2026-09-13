import pytest

from src.data_analysis import analytical_queries
from src.data_analysis.query_library import QUERY_NAMES, SQL_DIRECTORY, ANALYTICAL_QUERIES
import src.pipeline as pipeline


def test_query_listing_does_not_start_spark(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["analytical_queries", "--list"])
    def unexpected_call(*args):
        pytest.fail("Listing query names should not need Spark or a config")
    monkeypatch.setattr(analytical_queries, "create_spark", unexpected_call)
    monkeypatch.setattr(analytical_queries, "load_config", unexpected_call)
    analytical_queries.main()
    assert capsys.readouterr().out.splitlines() == list(QUERY_NAMES)


def test_pipeline_rejects_analysis_command(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["pipeline", "queries"])
    with pytest.raises(SystemExit) as error:
        pipeline.main()
    assert error.value.code == 2
    assert "invalid choice" in capsys.readouterr().err


def test_sql_registry_has_all_six_files():
    assert len(QUERY_NAMES) == 6
    assert set(QUERY_NAMES) == {path.stem for path in SQL_DIRECTORY.glob("*.sql")}
    assert all(sql.strip() for sql in ANALYTICAL_QUERIES.values())
