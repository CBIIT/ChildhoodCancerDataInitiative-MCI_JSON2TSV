import pytest
from datetime import datetime
from src.MCI_JSON2TSV import refresh_date

def test_refresh_date(mocker):
    mock_now = mocker.patch("src.MCI_JSON2TSV.datetime")
    mock_now.today.return_value = datetime(2022, 2, 6, 3, 15, 0)

    assert refresh_date() == "20220206_031500"
