import pytest
from collections import defaultdict
import json
from src.MCI_JSON2TSV import custom_json_parser

# Test cases
def test_single_key_value():
    input_data = json.dumps({"a": 1})
    expected_output = {"a": 1}
    assert json.loads(input_data, object_pairs_hook=custom_json_parser) == expected_output

def test_empty_dict():
    input_data = {}
    expected_output = {}
    assert custom_json_parser(input_data) == expected_output

def test_duplicate_keys():
    input_data = '{"a": 1, "a": 2, "b": 3}'
    expected_output = {"a": [1, 2], "b": 3}
    assert json.loads(input_data, object_pairs_hook=custom_json_parser) == expected_output

def test_nested_dict():
    input_data = '{"a": {"b": 2},"c": 3}'
    expected_output = {
        "a": {"b": 2},
        "c": 3
    }
    assert json.loads(input_data, object_pairs_hook=custom_json_parser) == expected_output

def test_nested_dict_with_duplicates():
    input_data = '{"a": {"b": 1, "b": 2}, "c": 3}'
    expected_output = {"a": {"b": [1, 2]}, "c": 3}
    assert json.loads(input_data, object_pairs_hook=custom_json_parser) == expected_output


def test_mixed_dict_and_list():
    input_data = '{"a": 1,"b": {"c": 2, "c": 3},"d": {"e": 4}}'
    expected_output = {"a": 1,"b": {"c": [2, 3]},"d": {"e": 4}}
    assert json.loads(input_data, object_pairs_hook=custom_json_parser) == expected_output

def test_multiple_duplicates_in_different_keys():
    input_data = '{"a": 1, "a": 2, "b": 3, "b": 4, "c": {"d": 5, "d": 6}}'
    expected_output = {
        "a": [1, 2],
        "b": [3, 4],
        "c": {"d": [5, 6]}}
    assert json.loads(input_data, object_pairs_hook=custom_json_parser) == expected_output
