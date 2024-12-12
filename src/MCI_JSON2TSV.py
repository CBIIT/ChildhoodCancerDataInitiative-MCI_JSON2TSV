#!/usr/bin/env python3

"""MCI_JSON2TSV.py: Script to transform clinical JSON files to TSV format.
"""

##############
#
# Env. Setup
#
##############

import sys
import os
import json
import itertools
import argparse
import logging
from collections import defaultdict
from datetime import datetime

import pandas as pd
import numpy as np


##############
#
# Logging
#
##############

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename="JSON2TSV.log", encoding="utf-8", filemode="w", level=logging.INFO
)


##############
#
# Functions
#
##############


def refresh_date():
    """Gets and returns current date and time"""

    today = datetime.today()
    today = today.strftime("%Y%m%d_%H%M%S")
    return today


def read_cog_jsons(dir_path: str):
    """Reads in COG JSON files and return concatenated DataFrame.

    Args:
        dir_path (str): The directory path containing the JSON files
            to be transformed

    Returns:
        pd.DataFrame: A DataFrame object that is a concatenation of the JSON files read into DataFrames

    Raises:
        ValueError: If a given JSON file cannot be properly read and loaded in as a pandas DataFrame object

    Notes:
        The object_pairs_hook parameter allows you to intercept the
            key-value pairs of the JSON object before they are converted
            into a dictionary; aids in accounting for multiple `data` keys
    """
    concatenated_df = pd.DataFrame()
    df_list = []  # List to hold DataFrames

    success_count = 0  # count of JSON files successfully processed
    error_count = 0  # count of JSON files not processed

    for filename in os.listdir(dir_path):
        if filename.endswith(".json"):
            file_path = os.path.join(dir_path, filename)
            try:
                with open(file_path, "r") as f:
                    # Read the file as a string
                    json_str = f.read()

                    # Parse the string manually to capture all `data` sections
                    json_data = json.loads(
                        json_str, object_pairs_hook=custom_json_parser
                    )

                    # Normalize the JSON data into a DataFrame
                    df = pd.json_normalize(json_data)

                    # append to list of DataFrames
                    df_list.append(df)
                    success_count += 1

            except ValueError as e:
                error_count += 1
                logging.error(f" Error reading {filename}: {e}")


    # Concatenate all the DataFrames
    if len(df_list) > 0:
        concatenated_df = pd.concat(df_list, ignore_index=True)
        return concatenated_df, success_count, error_count
    else:
        logging.error(" No valid JSON files found.")
        sys.exit("\n\t>>> Process Exited: No valid JSON files found.")


def custom_json_parser(pairs: dict):
    """Function to preserve duplicate key values.

    Args:
        pairs (dict): key, value pairs recursively fed in from json.loads()

    Returns:
        dict: A key-value pair in python dict type
    """

    # Initialize a dictionary to handle duplicated keys
    result = defaultdict(list)

    # if value of k, v pair is dict
    # append to new dict to store values
    for key, value in pairs:
        if isinstance(value, dict):
            result[key].append(custom_json_parser(value.items()))
        else:
            result[key].append(value)

    # If there's only one value for a key,
    # flatten it (i.e., don't keep it as a list)
    result = {k: (v[0] if len(v) == 1 else v) for k, v in result.items()}

    return result


def expand_cog_df(df: pd.DataFrame):
    """Function to parse participant JSON and output TSV of values and column header reference

    Args:
        df (pd.DataFrame): DataFrame of concatenated, normalized JSONs

    Returns:
        pd.DataFrame: Transformed form values from JSON to pd.DataFrame with updated field names reflecting the form the field is derived from (e.g. DEMOGRAPHY.DM_BRTHDAT)
        pd.DataFrame: Column header reference (form field ID : SaS Label)

    Notes:
        To handle multiple instances of a given form (i.e. Follow-Ups),
        the parsed 'data' objects of the form type is expected as a list
        of lists of dictionaries, for example:
        [[{field : value}, {field : value}], [{field : value}, {field : value}]],
        where the sub-list is a form instance, and is itself a list of dicts.

        Each form instance will be output as a row in the TSV, i.e. multiple
        rows per participant if there are multiple instances of a form for
        the given participant.

    """

    # initialize output file lists to be converted to DataFrames
    expanded_data = []
    saslabel_data = []

    # Iterate through each row in the DataFrame
    for index, row in df.iterrows():
        expanded_rows = []  # Hold all rows for this UPI
        common_row = {
            "upi": row["upi"],
            "index_date_type": row["index_date_type"],
        }  # Store common fields

        # Process each form entry in the 'forms' column
        for form in row["forms"]:
            form_name = form["form_name"]

            # Get 'data' sections; ensure it's a list of lists of dictionaries
            data_sections = form.get("data")

            # Ensure that we handle list of lists or just a list properly
            if isinstance(data_sections, list) and all(
                isinstance(i, list) for i in data_sections
            ):
                pass  # If data_sections is already a list of lists, do nothing

            elif isinstance(data_sections, list):
                data_sections = [
                    data_sections
                ]  # If it's a list of dicts, wrap in another list
            else:
                # continue  # If data_sections is neither a list nor valid, skip this form
                upi = row['upi']
                logging.info(
                    f" Skipping data section(s) for upi {upi} form {form_name}, not in valid format for parsing"
                )

            # Generate rows for each 'data' section (now lists of lists)
            form_rows = []
            for data_block in data_sections:
                form_row = common_row.copy()  # Start with the common data
                for field in data_block:
                    # Check if it's a valid field dictionary
                    if isinstance(field, dict):
                        form_field_id = field.get("form_field_id")
                        SASLabel = field.get("SASLabel")
                        value = field.get("value")

                        # Ensure form_field_id exists
                        if form_field_id:
                            # Create the column name and add the value
                            column_name = f"{form_name}.{form_field_id}"
                            form_row[column_name] = value

                            # Collect SASLabel and column_name pair
                            saslabel_data.append(
                                {"column_name": column_name, "SASLabel": SASLabel}
                            )
                form_rows.append(form_row)

            # Append all form rows to the expanded rows for this UPI
            expanded_rows.append(form_rows)

        # Create all combinations of the rows from different forms
        if expanded_rows:  # Ensure there's at least one valid form row
            combinations = list(itertools.product(*expanded_rows))
            for combo in combinations:
                combined_row = {}
                for part in combo:
                    combined_row.update(
                        part
                    )  # Merge each part of the combo into one row
                expanded_data.append(combined_row)

    # Convert the expanded data into DataFrames
    df_expanded = pd.DataFrame(expanded_data).drop_duplicates()
    df_saslabels = pd.DataFrame(saslabel_data).drop_duplicates()

    return df_expanded, df_saslabels


def cog_to_tsv(dir_path: str):
    """
    Function to call the reading in and transformation of JSON files

    Args:
        dir_path (str): Path to directory containing JSON files

    Returns:
        pd.DataFrame: dataframe of transformed and aggregated JSON files
        pd.DataFrame: dataframe of 
        int: The count of JSON files successfully processed
        int: The count of JSON files unsuccessfully processed
    """

    # read in JSONs
    df_ingest, success_count, error_count = read_cog_jsons(dir_path)

    # transform JSONs and generate column name reference file
    df_reshape, df_saslabels = expand_cog_df(df_ingest)

    # FIX BACK AFTER TESTING
    # df_reshape.to_csv(f"JSON_table_conversion_{get_time}.tsv", sep="\t", index=False)
    # df_saslabels.to_csv(f"saslabels_{get_time}.tsv", sep="\t", index=False)

    # REMOVE AFTER TESTING
    df_reshape.to_csv(f"JSON_table_conversion.tsv", sep="\t", index=False)
    df_saslabels.to_csv(f"saslabels.tsv", sep="\t", index=False)

    return df_reshape, df_saslabels, success_count, error_count


##FP
def form_parser(df: pd.DataFrame, get_time: str) -> pd.DataFrame:
    """Split transformed JSON data into TSVs for each form type

    Args:
        df (pd.DataFrame): transformed form values from JSON to pd.DataFrame with updated field names reflecting the form the field is derived from (e.g. DEMOGRAPHY.DM_BRTHDAT)

    Returns:
        pd.DataFrame: parsed columns by form type written to separate tsv files

    """

    if type(df) == pd.core.frame.DataFrame:

        # make directory to store split TSVs
        directory_path = "Form_TSVs_" + get_time
        os.mkdir(directory_path)

        # grab indexing columns
        index_cols = list(df.columns[:2])

        # grab form names from column headers
        forms = list(set([col.split(".")[0] for col in df.columns if "." in col]))

        # split columns by form and write to file
        for form in forms:
            subset = [col for col in df.columns if form in col]
            temp_df = df[index_cols + subset]
            temp_df.to_csv(directory_path + "/" + form + ".tsv", sep="\t", index=False)

    else:
        logger.error(
            "No valid DataFrame found to \
            parse into form-level TSVs"
        )
        sys.exit(
            "\n\t>>> Process Exited: No valid DataFrame found to \
            parse into form-level TSVs"
        )

    return None


# main function


def main():
    start_time = datetime.now()

    print("\n\t>>> Running MCI_JSON2TSV.py ....")

    get_time = refresh_date()

    parser = argparse.ArgumentParser(
        prog="MCI_JSON2TSV.py",
        description="This script will take a folder of JSON files, \
        both COG and IGM, for the MCI project and return a TSV data file \
        and data dictionary.",
    )

    # remove built in argument group
    parser._action_groups.pop()

    # create a required arguments group
    required_arg = parser.add_argument_group("required arguments")
    optional_arg = parser.add_argument_group("optional arguments")  ##FP

    required_arg.add_argument(
        "-d",
        "--directory",
        type=str,
        help="A directory of MCI JSON files, COG and/or IGM.",
        required=True,
    )

    ##FP
    optional_arg.add_argument(
        "-f",
        "--form_parsing",
        type=bool,
        help="Boolean indicating if parsing out TSVs by form should occur",
        required=False,
        default=False,
    )

    args = parser.parse_args()

    # pull in args as variables
    json_dir_path = args.directory
    form_parse = args.form_parsing  ##FP

    # call cog_to_tsv function to read in and transform JSON files to TSV
    df_reshape, df_saslabels, success_count, error_count = cog_to_tsv(json_dir_path)

    ##FP
    # if option to parse by form, run form_parser
    if form_parse:
        form_parser(df_reshape, get_time)

    end_time = datetime.now()
    time_diff = end_time - start_time
    print("\n\t>>> Time to Completion: " + str(time_diff))
    print("\n\t>>> # Files Successfully Transformed: " + str(success_count))
    print("\n\t>>> # Files NOT Transformed (Errors): " + str(error_count) + "\n")


if __name__ == "__main__":
    main()


logging.shutdown()