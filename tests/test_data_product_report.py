from pathlib import Path

from src.data_analysis.data_product_report import render_report


def report_payload() -> dict:
    return {
        "title": "Urban Mobility Data Products",
        "subtitle": "NYC Yellow Taxi",
        "period": {"start": "2024-01-01", "end": "2024-01-02"},
        "generated_at": "2024-04-01T00:00:00+00:00",
        "kpis": {
            "total_trips": 30,
            "average_daily_trips": 15.0,
            "total_revenue": 420.0,
            "busiest_date": "2024-01-02",
            "busiest_day_trips": 20,
            "busiest_day_peak_hour": 18,
        },
        "daily": [
            {"date": "2024-01-01", "trips": 10, "revenue": 140.0, "peak_hour": 17},
            {"date": "2024-01-02", "trips": 20, "revenue": 280.0, "peak_hour": 18},
        ],
        "zones": [
            {"zone": "Midtown Center", "borough": "Manhattan", "trips": 20}
        ],
        "weather": [
            {"code": 1, "condition": "Clear", "trips": 18},
            {"code": 7, "condition": "Light Rain", "trips": 12},
        ],
        "air_quality": [
            {"category": "good", "trips": 30, "avg_distance": 3.25}
        ],
    }


def test_render_report_is_standalone_and_uses_weather_names(tmp_path: Path) -> None:
    html = render_report(report_payload())
    output = tmp_path / "report.html"
    output.write_text(html, encoding="utf-8")

    assert html.startswith("<!doctype html>")
    assert "<script src=" not in html
    assert '"condition":"Clear"' in html
    assert '"condition":"Light Rain"' in html
    assert "Meteostat condition names" in html
    assert output.read_text(encoding="utf-8") == html
