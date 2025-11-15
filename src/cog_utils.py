"""Utility functions for transforming and parsin COG JSON files"""

import os
import re
import sys
import json
import pandas as pd
import itertools
from collections import defaultdict
import logging

logger = logging.getLogger("cog_utils")

def read_cog_jsons(dir_path: str, cog_jsons: list):
    """Reads in COG JSON files and return concatenated DataFrame.

    Args:
        dir_path (str): The directory path containing the JSON files
            to be transformed
        cog_jsons (list): List of file names in directory path that are COG JSONs

    Returns:
        pd.DataFrame: A DataFrame object that is a concatenation of the JSON files read into DataFrames
        int: success count
        int: error count

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

    for filename in cog_jsons:
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
            logger.error(f" Error reading {filename}: {e}")

    # Concatenate all the DataFrames
    if len(df_list) > 0:
        concatenated_df = pd.concat(df_list, ignore_index=True)
        return concatenated_df, success_count, error_count
    else:
        logger.error(" No valid COG JSON files found and/or failed to open.")
        #sys.exit("\n\t>>> Process Exited: No valid JSON files found.")
        return pd.DataFrame(), success_count, error_count


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

def fix_encoding_issues(text):
    """
    Fix common encoding issues in text, particularly UTF-8 mangled characters.
    
    Args:
        text (str): Text that may contain encoding issues
        
    Returns:
        str: Text with encoding issues corrected
    """
    if not isinstance(text, str):
        return text

    # replace non-ascii or unicode characters with nothing
    text = "".join([i if ord(i) < 128 else '' for i in text])

    
    # Apply all encoding fixes
    encoding_fixes = {
        'â€™': "'",  # right single quote
        'â€œ': '"',  # left double quote
        'â€': '"',  # right double quote
        'â€“': '-',  # en dash
        'â€”': '-',  # em dash
        'â€˜': "'",  # left single quote
        'â€¢': '-',  # bullet
        'â€¦': '...',  # ellipsis
        '‚Äú': '"',  # left double quote (alternative)
        '‚Äù': '"',  # right double quote (alternative)
        '‚Äô': "'",  # right single quote (alternative)
        '”': '"',  # right double quote (unicode)
        '“': '"',  # left double quote (unicode)
        '‘': "'",  # left single quote (unicode)
        '’': "'",  # right single quote (unicode)
        '–': '-',  # en dash (unicode)
        '—': '-',  # em dash (unicode)
        '•': '-',  # bullet (unicode),
        '\u201c': '"',  # left double quote
        '\u201d': '"',  # right double quote
        '\u2018': "'",  # left single quote
        '\u2019': "'",  # right single quote
        '\u2013': '-',  # en dash
        '\u2014': '-',  # em dash
        '\u2026': '...',  # ellipsis
        '“': '"',  # left double quote (alternative)
        '”': '"',  # right double quote (alternative)
    }
    
    for mangled, correct in encoding_fixes.items():
        text = text.replace(mangled, correct)

    # also replace all html entities, i.e. <p>, </p>, <strong>, <em>, etc
    html_entities = {
        '<p>': '', '</p>': '', '<strong>': '', '</strong>': '', 
        '<em>': '', '</em>': ''
    }

    for entity, replacement in html_entities.items():
        text = text.replace(entity, replacement)

    #### whitespace fixes ####

    # remove carriage returns
    text = text.replace('\r\n', ';')
    text = text.replace('\r', ' ')
    # replace tabs with space
    text = text.replace('\t+', ' ')
    
    # replace multiple spaces with single space
    text = re.sub(r' +', ' ', text)
    
    # remove any consecutive instances of newline chars with ;
    text = re.sub(r'\n+', '; ', text)

    return text


def pv_checks_convert(df: pd.DataFrame, sas_labels: pd.DataFrame):
    """Function to convert checked/unchecked groups of fields into one field that is ; delimited set of checked values

    Args:
        df (pd.DataFrame): DataFrame containing the fields to check
        sas_labels (pd.DataFrame): DataFrame containing the SAS labels for the fields

    Returns:
        pd.DataFrame: Updated dataframe with converted checked values
    """
    
    # find all columns in df that have checked or unchecked values in rows
    checked_cols = df.columns[df.isin(["checked", "unchecked"]).any()]

    # find all rows in sas_labels that are in checked_cols 
    matching_rows = sas_labels[sas_labels["column_name"].isin(checked_cols)].copy()

    # convert cols column_name and SASLabel to dict
    col_sas_dict = dict(zip(matching_rows["column_name"], matching_rows["SASLabel"]))

    # Create a copy of the original DataFrame to avoid modifying it directly
    df_checked = df.copy()

    # Iterate through each column in the DataFrame
    for col in df.columns:
        if col in col_sas_dict:
            # For values in column that are equal to checked,
            # replace checked with corresponding value in col_sas_dict
            df_checked[col] = df_checked[col].replace("checked", col_sas_dict[col])
            # else if value is unchecked, replace with empty string
            df_checked[col] = df_checked[col].replace("unchecked", "")

    # create new col in matching_rows where the last "_" delimited substring/part is removed
    matching_rows["group_column_name"] = matching_rows["column_name"].apply(lambda x: "_".join(x.split("_")[:-1]) if "_" in x else x)

    # create new dict to map group_column_name to original column names
    group_col_dict = matching_rows.groupby("group_column_name")["column_name"].apply(list).to_dict()

    # for each key in group_col_dict, create new columns in a dict
    new_cols = {}
    for group_name, col_names in group_col_dict.items():
        new_col = df_checked[col_names].apply(lambda x: ';'.join(x.dropna().astype(str)), axis=1)
        # strip leading/trailing ';' and replace consecutive ';'
        new_col = new_col.str.strip(';').str.replace(';;+', ';', regex=True)
        new_cols[group_name] = new_col

    # Concatenate all new columns at once
    df_checked = pd.concat([df_checked.drop(columns=sum(group_col_dict.values(), []), errors='ignore'),
                            pd.DataFrame(new_cols)], axis=1)

    return df_checked

def pv_convert_checked_no_collapse(df: pd.DataFrame, sas_labels: pd.DataFrame):
    """Function to replace checked/unchecked values with SAS labels without collapsing into single field

    Args:
        df (pd.DataFrame): DataFrame containing the fields to check
        sas_labels (pd.DataFrame): DataFrame containing the SAS labels for the fields
    """
    
    # find all columns in df that have checked or unchecked values in rows
    checked_cols = df.columns[df.isin(["checked", "unchecked"]).any()]

    # find all rows in sas_labels that are in checked_cols 
    matching_rows = sas_labels[sas_labels["column_name"].isin(checked_cols)].copy()

    # convert cols column_name and SASLabel to dict
    col_sas_dict = dict(zip(matching_rows["column_name"], matching_rows["SASLabel"]))

    # Create a copy of the original DataFrame to avoid modifying it directly
    df_checked = df.copy()

    # Iterate through each column in the DataFrame
    for col in df.columns:
        if col in col_sas_dict:
            # For values in column that are equal to checked,
            # replace checked with corresponding value in col_sas_dict
            df_checked[col] = df_checked[col].replace("checked", col_sas_dict[col])
            # else if value is unchecked, replace with empty string
            df_checked[col] = df_checked[col].replace("unchecked", "")
            
    return df_checked

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
                upi = row["upi"]
                logger.info(
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
                        cde_id = field.get("cde_id")

                        # Ensure form_field_id exists
                        if form_field_id:
                            # Create the column name and add the value
                            column_name = f"{form_name}.{form_field_id}"
                            form_row[column_name] = value

                            # Collect SASLabel, column_name, and cde_id for reference
                            saslabel_data.append(
                                {"column_name": column_name, "SASLabel": SASLabel.strip(), "cde_id" : str(cde_id)}
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


def cog_to_tsv(dir_path: str, cog_jsons: list, cog_op: str, timestamp: str):
    """
    Function to call the reading in and transformation of COG JSON files

    Args:
        dir_path (str): Path to directory containing COG JSON files
        cog_jsons (list): List of COG JSON filenames located in dir_path
        cog_op (str): Path to directory to output transformed COG TSV files
        timestamp (str): Date-time of when script run

    Returns:
        pd.DataFrame: dataframe of transformed and aggregated JSON files
        str: Name of the flattenend and aggreated COG forms file
        int: The count of JSON files successfully processed
        int: The count of JSON files unsuccessfully processed
    """

    # read in JSONs
    df_ingest, success_count, error_count = read_cog_jsons(dir_path, cog_jsons)

    if success_count > 0:

        # transform JSONs and generate column name reference file
        df_reshape, df_saslabels = expand_cog_df(df_ingest)

        # save data files to output COG directory
        
        df_saslabels.to_csv(
            f"{cog_op}/COG_saslabels_{timestamp}.tsv", sep="\t", index=False
        )
        
        # transpose df_saslabels and then concat to annotated 
        
        df_saslabels_T = df_saslabels.T.reset_index().drop('index', axis=1)
        # header is first row
        df_saslabels_T.columns = df_saslabels_T.iloc[0]
        df_saslabels_T = df_saslabels_T[1:].reset_index(drop=True)
        
        # concat and keep all cols unique to dataframes
        df_reshape_A = pd.concat([df_saslabels_T, df_reshape.reset_index()])
        
        # drop index col and put upi and index_date_type at the front
        df_reshape_A = df_reshape_A[['upi', 'index_date_type'] + df_reshape_A.columns.tolist()[:-3]]
        
        # fix encoding issues in all string columns
        for col in df_reshape_A.select_dtypes(include=['object']).columns:
            df_reshape_A[col] = df_reshape_A[col].apply(fix_encoding_issues)

        cog_flattened_file_name = f"{cog_op}/COG_JSON_table_conversion_raw_{timestamp}.tsv"
        
        df_reshape_A.to_csv(
            cog_flattened_file_name, sep="\t", index=False
        )

        decoded_df = pv_checks_convert(df_reshape_A, df_saslabels).reset_index(drop=True)

        raw_decoded_df = pv_convert_checked_no_collapse(df_reshape_A, df_saslabels).reset_index(drop=True)

        # add cadsr and saslabel
        decoded_df.loc[0,'index_date_type'] = 'SAS Label Description'
        decoded_df.loc[1,'index_date_type'] = 'caDSR Code'
        
        # add cadsr and saslabel
        raw_decoded_df.loc[0,'index_date_type'] = 'SAS Label Description'
        raw_decoded_df.loc[1,'index_date_type'] = 'caDSR Code'

        decoded_df_file_name = f"{cog_op}/COG_JSON_table_conversion_decoded_{timestamp}.tsv"
        raw_decoded_df_file_name = f"{cog_op}/COG_JSON_table_conversion_raw_decoded_{timestamp}.tsv"

        decoded_df.to_csv(
            decoded_df_file_name, sep="\t", index=False
        )

        raw_decoded_df.to_csv(
            raw_decoded_df_file_name, sep="\t", index=False
        )

        return decoded_df, decoded_df_file_name, success_count, error_count

    else:
        # return empty dataframe since no files to process
        return pd.DataFrame(), "", success_count, error_count


##FP
def form_parser(df: pd.DataFrame, timestamp: str, cog_op: str) -> pd.DataFrame:
    """Split transformed JSON data into TSVs for each form type

    Args:
        df (pd.DataFrame): transformed form values from JSON to pd.DataFrame with updated field names reflecting the form the field is derived from (e.g. DEMOGRAPHY.DM_BRTHDAT)
        timestamp (str): Date-time of when script run
        cog_op (str): Path to output directory for COG files

    Returns:
        pd.DataFrame: parsed columns by form type written to separate tsv files

    """

    if type(df) == pd.core.frame.DataFrame:

        # make directory to store split TSVs
        directory_path = f"{cog_op}/COG_form_level_TSVs_{timestamp}/"

        if not os.path.exists(directory_path):
            os.mkdir(directory_path)

        # grab indexing columns
        index_cols = list(df.columns[:2])

        # grab form names from column headers
        forms = list(set([col.split(".")[0] for col in df.columns if "." in col]))

        # split columns by form and write to file
        for form in forms:
            subset = [col for col in df.columns if form in col]
            temp_df = df[index_cols + subset].drop_duplicates()
            temp_df.to_csv(f"{directory_path}/{form}.tsv", sep="\t", index=False)

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