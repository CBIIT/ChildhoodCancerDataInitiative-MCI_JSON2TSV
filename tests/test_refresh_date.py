import pytest
from datetime import datetime
import sys, os
sys.path.insert(0, 'src/') 
print(os.path)
from MCI_JSON2TSV import refresh_date

def test_refresh_date(mocker):
    mock_dt = mocker.patch("MCI_JSON2TSV.datetime")
    mock_dt.today.return_value = datetime(2022, 2, 6, 3, 15, 0)

    assert refresh_date() == "20220206_031500"
