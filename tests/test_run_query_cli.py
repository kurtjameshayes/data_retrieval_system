import json

import pytest

from run_query import build_parameters, extract_records, render_table


def test_build_parameters_merges_sources(tmp_path):
    params_file = tmp_path / "params.json"
    params_file.write_text(
        json.dumps(
            {
                "dataset": "2020/dec/pl",
                "filters": {"state": "01"},
                "limit": 3,
            }
        ),
        encoding="utf-8",
    )

    merged = build_parameters(
        params_file=str(params_file),
        params_json=json.dumps(
            {
                "filters": {"level": "state", "state": "02"},
                "columns": ["NAME", "P1_001N"],
            }
        ),
        inline_params=["filters.state=06", "limit=5"],
    )

    assert merged["dataset"] == "2020/dec/pl"
    assert merged["limit"] == 5
    assert merged["filters"]["level"] == "state"
    assert merged["filters"]["state"] == "06"
    assert merged["columns"] == ["NAME", "P1_001N"]


def test_render_table_includes_all_columns():
    records = [
        {"a": 1, "b": 2},
        {"a": 3, "c": 4},
    ]

    table = render_table(records, max_rows=1)

    # Header contains all discovered columns
    assert "a" in table.splitlines()[0]
    assert "b" in table.splitlines()[0]
    assert "c" in table.splitlines()[0]
    # Truncation notice since max_rows=1 but we had 2 records
    assert "... 1 more row(s)" in table


@pytest.mark.parametrize(
    "payload, expected_length",
    [
        ({"data": [{"x": 1}, {"x": 2}]}, 2),
        ([{"x": 1}, {"x": 2}, {"x": 3}], 3),
        ({"data": {"x": 1}}, 1),
        (None, 0),
    ],
)
def test_extract_records_handles_various_payloads(payload, expected_length):
    records = extract_records(payload)
    assert len(records) == expected_length
