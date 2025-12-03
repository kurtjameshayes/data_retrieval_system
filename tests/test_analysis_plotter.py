import pandas as pd

from core.analysis_plotter import AnalysisPlotter


def test_prepare_sort_key_column_sorts_numeric_strings():
    dataframe = pd.DataFrame({"period": ["10", "2", "1", "3"]})
    plotter = AnalysisPlotter()

    sort_column = plotter._prepare_sort_key_column(dataframe, "period", "axis")
    sorted_periods = dataframe.sort_values(sort_column)["period"].tolist()

    assert sorted_periods == ["1", "2", "3", "10"]


def test_prepare_sort_key_column_sorts_datetime_strings():
    dataframe = pd.DataFrame(
        {"date": ["2023-01-10", "2022-12-31", "2023-01-01", "2023-02-01"]}
    )
    plotter = AnalysisPlotter()

    sort_column = plotter._prepare_sort_key_column(dataframe, "date", "axis")
    sorted_dates = dataframe.sort_values(sort_column)["date"].tolist()

    assert sorted_dates == [
        "2022-12-31",
        "2023-01-01",
        "2023-01-10",
        "2023-02-01",
    ]


def test_prepare_sort_key_column_handles_case_insensitive_strings():
    dataframe = pd.DataFrame({"label": ["beta", "Alpha", "gamma"]})
    plotter = AnalysisPlotter()

    sort_column = plotter._prepare_sort_key_column(dataframe, "label", "axis")
    sorted_labels = dataframe.sort_values(sort_column)["label"].tolist()

    assert sorted_labels == ["Alpha", "beta", "gamma"]
