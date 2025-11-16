"""Utility functions for transforming and parsing IGM JSON files"""

import os
import re
import json
import pandas as pd
import logging
from collections import defaultdict
from cog_utils import fix_encoding_issues

logger = logging.getLogger("igm_utils")

CORE_FIELDS = [
    "version",
    "subject_id",
    "report_type",
    "title",
    "service",
    "report_version",
    "disease_group",
    "percent_tumor",
    "percent_necrosis",
    "indication_for_study",
    "amendments",
]

def null_n_strip(value):
    """Format strings in IGM JSONs

    Args:
        value : Value read in from key:value pair in IGM JSON

    Returns:
        If str, formatted str; elif None, empty str; else original value argument
    """

    if value is None:
        return ""
    elif isinstance(value, str):
        return value.strip()
    else:
        return value

def rem_single_quotes(value: list):
    """Remove single quotes from array by converting array to comma-separated string without quotes

    Args:
        value (list): List representation as string from IGM JSON
    Returns:
        str: String representation of list with single quotes removed
    """
    if isinstance(value, list):
        return "[" + ", ".join([str(v) for v in value]) + "]"
    else:
        return value

def flatten_igm(json_obj: dict, parent_key="", flatten_dict=None, parse_type=None):
    """Recursive function to un-nest a nested dictionary for WXS and Archer Fusion

    Args:
        json_obj (dict): Nested JSON IGM form
        parent_key (str, optional): The inherited key from previous recursive run. Defaults to ''.
        flatten_dict (dict, optional): The inherited 'flattened' JSON from previous recursive run. Defaults to {}.
        parse_type (str, optional): When specified as 'cnv', for any key == 'disease_associated_gene_content', do not flatten value for that key

    Returns:
        dict: Un-nested dict/JSON
    """

    # init flatten_dict
    if flatten_dict is None:
        flatten_dict = {}

    # if value of key: value pair is another dict obj
    if isinstance(json_obj, dict):
        for key, value in json_obj.items():
            if key == "disease_associated_gene_content":
                #if (
                    #parse_type == "cnv"
                #):  # preserve gene list as list to iterate thru for results parsing
                new_key = f"{parent_key}.{key}" if parent_key else key
                flatten_dict[new_key] = value
                #else:
                    #new_key = f"{parent_key}.{key}" if parent_key else key
                    #flatten_igm(value, new_key, flatten_dict, parse_type)
            if not isinstance(value, dict):
                if not isinstance(value, list):
                    new_key = f"{parent_key}.{key}" if parent_key else key
                    flatten_dict[new_key] = null_n_strip(value)
                    flatten_igm(null_n_strip(value), new_key, flatten_dict, parse_type)
                else:
                    flatten_igm(
                        value,
                        f"{parent_key}.{key}" if parent_key != "" else key,
                        flatten_dict,
                        parse_type,
                    )  # Recurse into nested dictionary
            else:
                flatten_igm(
                    value,
                    f"{parent_key}.{key}" if parent_key != "" else key,
                    flatten_dict,
                    parse_type,
                )  # Recurse into nested dictionary

    # if value of key: value pair is a list obj
    elif isinstance(json_obj, list):
        if len(json_obj) > 0:
            if "disease_associated_gene_content" in parent_key: #and parse_type == "cnv":
                pass  # gene list preserved as list in above logic
            else:
                for i, item in enumerate(json_obj):
                    if not isinstance(item, dict):
                        if not isinstance(item, list):
                            new_key = f"{parent_key}.{i}" if parent_key else str(i)
                            flatten_dict[new_key] = null_n_strip(item)
                            flatten_igm(item, new_key, flatten_dict, parse_type)
                        else:
                            flatten_igm(
                                item, f"{parent_key}.{i}", flatten_dict, parse_type
                            )  # Recurse into list elements
                    else:
                        flatten_igm(
                            item, f"{parent_key}.{i}", flatten_dict, parse_type
                        )  # Recurse into list elements
        else:  # empty list variables
            flatten_dict.update({parent_key: ""})

    else:
        pass

    return flatten_dict


def full_form_convert(flatten_dict: dict):
    """Convert flattened JSON to pd.DataFrame

    Args:
        flatten_dict (dict): IGM nested JSON that has been flattened to un-nested JSON

    Returns:
        pd.DataFrame: The flattened JSON converted to pd.DataFrame
    """

    try:
        return pd.DataFrame([flatten_dict])
    except Exception as e:
        logger.error(f"Error converting flattened JSON to pd.DataFrame: {e}")
        return pd.DataFrame()


def igm_to_tsv(
    dir_path: str,
    igm_jsons: list,
    assay_type: str,
    igm_op: str,
    timestamp: str,
):
    """Function to call the reading in and transformation of IGM JSON files

    Args:
        dir_path (str): Path to directory containing COG JSON files
        igm_jsons (list): List of COG JSON filenames located in dir_path
        assay_type (str): Molecular assay type of IGM JSONs (i.e. Archer Fusion, WXS or methylation)
        igm_op (str): Path to directory to output transformed IGM TSV files
        timestamp (str): Date-time of when script run

    Returns:
        dict: dict of results-level parsing types and file path
        int: The count of JSON files successfully processed
        int: The count of JSON files unsuccessfully processed
    """

    valid = ["igm.tumor_normal", "igm.archer_fusion", "igm.methylation"]

    if assay_type not in valid:
        raise ValueError(f"assay_type {assay_type} is not one of {valid}.")

    df_list = []  # List to hold individual JSON DataFrames
    
    parsed_results = [] # List to hold result files and their types for COG IGM integration

    success_count = 0  # count of JSON files successfully processed
    error_count = 0  # count of JSON files not processed

    for filename in igm_jsons:
        file_path = os.path.join(dir_path, filename)
        try:
            file_2_flat = json.load(open(file_path))
            flatten_dict1 = flatten_igm(file_2_flat)

            flatten_dict_df = full_form_convert(flatten_dict1)

            df_list.append(flatten_dict_df)

            success_count += 1

        except Exception as e:
            error_count += 1
            logger.error(f" Error converting IGM JSON to TSV for file {file_path}: {e}")

    # variant results section parse
    # make output dir
    directory_path = f"{igm_op}/IGM_results_level_TSVs_{timestamp}"

    if not os.path.exists(directory_path):
        os.mkdir(directory_path)

    if assay_type == "igm.archer_fusion":
        results_types = [
            "fusion_tier_one_or_two_result",
            "fusion_tier_three_result",
            "single_tier_one_or_two_result",
            "single_tier_three_result",
        ]
    elif assay_type == "igm.tumor_normal":
        results_types = [
            "germline_cnv_results",
            "germline_results",
            "somatic_cnv_results",
            "somatic_results",
        ]
    elif assay_type == "igm.methylation":
        results_types = ['final_diagnosis']

    op_dict = defaultdict(list)

    for filename in igm_jsons:
        file_path = os.path.join(dir_path, filename)
        try:
            parsed_results = igm_results_variants_parsing(
                json.load(open(file_path)), filename, assay_type, results_types
            )

            for key in parsed_results.keys():
                op_dict[key].append(parsed_results[key])

        except Exception as e:
            logger.error(
                f"Could not parse results section from file {file_path}, please check and try again: {e}"
            )
    for result_type in op_dict.keys():
        concat_variant_result_df = pd.concat(op_dict[result_type])
        concat_variant_result_df = concat_variant_result_df.map(fix_encoding_issues)
        
        # for cols with 'disease_associated_gene_content' in name, remove single quotes from list str representation
        for col in concat_variant_result_df.columns:
            if 'disease_associated_gene_content' in col:
                concat_variant_result_df[col] = concat_variant_result_df[col].apply(rem_single_quotes)
        
        concat_variant_result_df_file_name = f"{directory_path}/IGM_{assay_type.replace('igm.', '')}_{result_type}_variant_data_{timestamp}.tsv"
        concat_variant_result_df.to_csv(
            concat_variant_result_df_file_name,
            sep="\t",
            index=False,
        )
        parsed_results[result_type] = concat_variant_result_df_file_name
    
    # concat all raw, flattened processed JSONs together
    if len(df_list) > 0:
        concatenated_df = pd.concat(df_list, ignore_index=True)

        # fix encoding issues
        concatenated_df = concatenated_df.map(fix_encoding_issues)
        
        # order non CORE fields alphanumerically after CORE_FIELDS
        non_core_cols = [col for col in concatenated_df.columns if col not in CORE_FIELDS]
        ordered_cols = CORE_FIELDS + sorted(non_core_cols)
        concatenated_df = concatenated_df[ordered_cols]
        
        # for cols with 'disease_associated_gene_content' in name, remove single quotes from list str representation
        for col in concatenated_df.columns:
            if 'disease_associated_gene_content' in col:
                concatenated_df[col] = concatenated_df[col].apply(rem_single_quotes)

        concatenated_df.to_csv(
            f"{igm_op}/IGM_{assay_type.replace('igm.', '')}_JSON_table_conversion_{timestamp}.tsv",
            sep="\t",
            index=False,
        )
        return parsed_results, success_count, error_count
    else:
        logger.error(
            f" No valid IGM JSON files found and/or failed to open for assay_type {assay_type}."
        )
        # sys.exit("\n\t>>> Process Exited: No valid JSON files found.")
        return "", success_count, error_count


def igm_results_variants_parsing(
    form: dict, form_name: str, assay_type: str, results_types: list
):
    """Results section specific parsing (long format)

    Args:
        form (dict): JSON form loaded in
        form_name (str): File name of form data is sourced from
        assay_type (str): Molecular assay type of IGM JSONs (i.e. Archer Fusion, WXS or methylation)
        results_types (list): Potential results sections that may appear in form to parse

    Raises:
        ValueError: If assay_type is not acceptable value

    Returns:
        dict: dict of dataframes of parsed and formatted results section(s)
    """

    # valid types check
    valid = ["igm.tumor_normal", "igm.archer_fusion", "igm.methylation"]

    if assay_type not in valid:
        raise ValueError(f"assay_type {assay_type} is not one of {valid}.")

    # check form type
    if not isinstance(form, dict):
        raise ValueError(f"Form is not of type dict.")

    all_output = {}  # init dict of dfs of each results section
    core_header = ["form_name"] + [field for field in CORE_FIELDS]
    core_fields = [form_name] + [form[field] for field in CORE_FIELDS]
    for results_type in results_types:
        output = []  # init list of dicts to make df of form specific results
        found = False  # if results/variants section found or not
        if results_type in form.keys():
            if assay_type == "igm.methylation":
                if len(form[results_type]) > 0:
                    found = True
                    temp_header = list(form[results_type].keys())
                    temp_fields = [null_n_strip(i) for i in form[results_type].values()]
                    output.append(
                        dict(
                            zip(
                                core_header + temp_header, core_fields + temp_fields
                            )
                        )
                    )

            else:  # archer fusion and wxs
                if (
                    "variants" in form[results_type].keys()
                    and len(form[results_type]["variants"]) > 0
                ):
                    found = True
                    for result in form[results_type]["variants"]:
                        if results_type in [
                            "somatic_cnv_results",
                            "germline_cnv_results",
                        ]:
                            flatten_temp = flatten_igm(result) #, parse_type="cnv")
                            genes = flatten_temp["disease_associated_gene_content"]
                            flatten_temp.pop("disease_associated_gene_content")
                            #for gene in genes:
                            temp_header = list(flatten_temp.keys()) + ["disease_associated_gene_content"]
                            temp_fields = [
                                null_n_strip(i) for i in flatten_temp.values()
                            ] + [genes]
                            output.append(
                                dict(
                                    zip(
                                        core_header + temp_header,
                                        core_fields + temp_fields,
                                    )
                                )
                            )
                        else:
                            flatten_temp = flatten_igm(result)
                            temp_header = list(flatten_temp.keys())
                            temp_fields = [
                                null_n_strip(i) for i in flatten_temp.values()
                            ]
                            output.append(
                                dict(
                                    zip(
                                        core_header + temp_header,
                                        core_fields + temp_fields,
                                    )
                                )
                            )
        else:
            found = False
        if (
            found == False
        ):  # if never found results section, append df indicating no data for file
            output.append(dict(zip(core_header, core_fields)))

        all_output[results_type] = pd.DataFrame(output)

    return all_output
