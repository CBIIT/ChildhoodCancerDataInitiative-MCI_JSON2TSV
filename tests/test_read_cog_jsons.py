import pytest
import pandas as pd
import os
import json
from unittest import mock
from src.MCI_JSON2TSV import read_cog_jsons  # Replace with your actual module name

@pytest.fixture
def mock_filesystem(mocker):
    # Mock os.listdir to simulate files in a directory
    mocker.patch("os.listdir", return_value=["file1.json", "file2.json", "file3.txt"])

    # Mock open() for reading JSON content from files
    mock_open = mocker.patch("builtins.open", mock.mock_open(read_data='{"data": [{"key1": "value1"}]}'))

    return mock_open

def test_read_cog_jsons_success(mocker, mock_filesystem):
    # Mock json.loads to simulate parsing
    mock_json_loads = mocker.patch("json.loads", return_value={"data": [{"key1": "value1"}]})

    # Mock pd.json_normalize to simulate DataFrame creation
    mock_json_normalize = mocker.patch("pandas.json_normalize", return_value=pd.DataFrame({"key1": ["value1"]}))

    # Call the function
    dir_path = "/mocked/dir"
    result_df, success_count, error_count = read_cog_jsons(dir_path)

    # Verify that the JSON loading and DataFrame creation functions were called
    mock_json_loads.assert_called()
    mock_json_normalize.assert_called()

    # Check the result
    assert isinstance(result_df, pd.DataFrame)
    assert not result_df.empty
    assert success_count == 2
    assert error_count == 0




