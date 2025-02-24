"""Utility functions for transforming and parsin IGM JSON files"""

import os
import json
import pandas as pd
import logging
from collections import defaultdict

logger = logging.getLogger("igm_utils")

CORE_FIELDS = ['version', 
            'subject_id', 
            'report_type', 
            'title', 
            'service', 
            'report_version', 
            'disease_group', 
            'percent_tumor', 
            'percent_necrosis', 
            'indication_for_study', 
            'amendments']

    
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

def flatten_igm(json_obj: dict, parent_key='', flatten_dict=None, parse_type=None):
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
            if key == 'disease_associated_gene_content':
                if parse_type == 'cnv': # preserve gene list as list to iterate thru for results parsing
                    new_key = f"{parent_key}.{key}" if parent_key else key
                    flatten_dict[new_key] = value
                else:
                    new_key = f"{parent_key}.{key}" if parent_key else key
                    flatten_igm(value, new_key, flatten_dict, parse_type)
            if not isinstance(value, dict):
                if not isinstance(value, list):
                    new_key = f"{parent_key}.{key}" if parent_key else key
                    flatten_dict[new_key] = null_n_strip(value)
                    flatten_igm(null_n_strip(value), new_key, flatten_dict, parse_type)
                else:
                    flatten_igm(value, f"{parent_key}.{key}" if parent_key != '' else key, flatten_dict, parse_type)  # Recurse into nested dictionary
            else:
                flatten_igm(value, f"{parent_key}.{key}" if parent_key != '' else key, flatten_dict, parse_type)  # Recurse into nested dictionary

    # if value of key: value pair is a list obj
    elif isinstance(json_obj, list):
        if len(json_obj) > 0:
            if 'disease_associated_gene_content' in parent_key and parse_type == 'cnv':
                pass #gene list preserved as list in above logic
            else:
                for i, item in enumerate(json_obj):
                    if not isinstance(item, dict):
                        if not isinstance(item, list):
                            new_key = f"{parent_key}.{i}" if parent_key else str(i)
                            flatten_dict[new_key] = null_n_strip(item)
                            flatten_igm(item, new_key, flatten_dict, parse_type)
                        else:
                            flatten_igm(item, f"{parent_key}.{i}", flatten_dict, parse_type)  # Recurse into list elements
                    else:
                        flatten_igm(item, f"{parent_key}.{i}", flatten_dict, parse_type) # Recurse into list elements
        else: #empty list variables
            flatten_dict.update({parent_key : ""})
        
    else: #TODO make sure this is correct behavior here
        #keys.append(parent_key)  # Base case: Add key when it's a leaf node (not dict or list)
        #print(parent_key, "LEAF NODE")
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
    

def igm_to_tsv(dir_path: str, igm_jsons: list, assay_type: str, igm_op: str, timestamp: str, results_parse: bool):
    """Function to call the reading in and transformation of IGM JSON files

    Args:
        dir_path (str): Path to directory containing COG JSON files
        igm_jsons (list): List of COG JSON filenames located in dir_path
        assay_type (str): Molecular assay type of IGM JSONs (i.e. Archer Fusion, WXS or methylation)
        igm_op (str): Path to directory to output transformed IGM TSV files
        timestamp (str): Date-time of when script run
        results_parse (bool): If True, parse out results specific sections to separate form in long format TSV

    Returns:
        pd.DataFrame: pandas DataFrame of converted JSON data
        pd.DataFrame: pandas DataFrame of converted JSON data from results sections to long format TSV(s)
        int: The count of JSON files successfully processed
        int: The count of JSON files unsuccessfully processed
    """

    valid = ["igm.tumor_normal", "igm.archer_fusion", "igm.methylation"]
    
    if assay_type not in valid:
        raise ValueError(f"assay_type {assay_type} is not one of {valid}.")

    df_list = []  # List to hold individual JSON DataFrames

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
    
    if results_parse:
        # make output dir
        directory_path = f"{igm_op}/IGM_results_level_TSVs_{timestamp}"

        if not os.path.exists(directory_path):
            os.mkdir(directory_path)

        if assay_type in ["igm.archer_fusion", "igm.methylation"]: #only 1 results section to parse out
            results = []
            for filename in igm_jsons:
                file_path = os.path.join(dir_path, filename)
                try:
                    results.append(igm_results_variants_parsing(json.load(open(file_path)), filename, assay_type, igm_op, timestamp))
                except Exception as e:
                    logger.error(f"Could not parse results section from file {file_path}, please check and try again.")
            pd.concat(results).to_csv(f"{directory_path}/IGM_{assay_type.replace('igm.', '')}_results_specific_data.tsv", sep="\t", index=False)
        else:
            #somatic_results, germline_results, somatic_cnv_results = [], [], []
            results_types = ["somatic_results", "germline_results", "somatic_cnv_results"]
            wxs = defaultdict(list)
            for filename in igm_jsons:
                file_path = os.path.join(dir_path, filename)
                try:
                    parsed_results = dict(zip(results_types, igm_results_variants_parsing(json.load(open(file_path)), filename, assay_type, igm_op, timestamp)))
                    
                    for key in parsed_results.keys():
                        wxs[key].append(parsed_results[key])
                    """somatic_results.append(parsed_results[0])
                    germline_results.append(parsed_results[1])
                    somatic_cnv_results.append(parsed_results[2])"""

                except Exception as e:
                    logger.error(f"Could not parse results sections from file {file_path}, please check and try again: {e}")
            # output 
            for result_type in wxs.keys():
                pd.concat(wxs[result_type]).to_csv(f"{directory_path}/IGM_{assay_type.replace('igm.', '')}_{result_type}_variant_data.tsv", sep="\t", index=False)
            """pd.concat(somatic_results).to_csv(f"{directory_path}/IGM_{assay_type.replace('igm.', '')}_somatic_results_specific_data.tsv", sep="\t", index=False)
            pd.concat(germline_results).to_csv(f"{directory_path}/IGM_{assay_type.replace('igm.', '')}_germline_results_specific_data.tsv", sep="\t", index=False)
            pd.concat(somatic_cnv_results).to_csv(f"{directory_path}/IGM_{assay_type.replace('igm.', '')}_somatic_cnv_results_specific_data.tsv", sep="\t", index=False)"""

    # concat all processed JSONs together
    if len(df_list) > 0:
        concatenated_df = pd.concat(df_list, ignore_index=True)

        concatenated_df.to_csv(
            f"{igm_op}/IGM_{assay_type.replace('igm.', '')}_JSON_table_conversion_{timestamp}.tsv", sep="\t", index=False
        )
        return concatenated_df, success_count, error_count
    else:
        logger.error(f" No valid IGM JSON files found and/or failed to open for assay_type {assay_type}.")
        #sys.exit("\n\t>>> Process Exited: No valid JSON files found.")
        return pd.DataFrame, success_count, error_count


def igm_results_variants_parsing(form: dict, form_name: str, assay_type: str):
    """Results section specific parsing (long format)

    Args:
        form (dict): JSON form loaded in 
        form_name (str): File name of form data is sourced from
        assay_type (str): Molecular assay type of IGM JSONs (i.e. Archer Fusion, WXS or methylation)

    Raises:
        ValueError: If assay_type is not acceptable value

    Returns:
        pd.DataFrame: dataframe of parsed and formatted results section(s)
    """

    # valiud types check
    valid = ["igm.tumor_normal", "igm.archer_fusion", "igm.methylation"]
    
    if assay_type not in valid:
        raise ValueError(f"assay_type {assay_type} is not one of {valid}.")
    
    #check form type
    if not isinstance(form, dict):
        raise ValueError(f"Form is not of type dict.")

    
    if assay_type == 'igm.methylation':
        output = [] # init list of lists to make df of form specific results
        core_header = ['form_name'] + [field for field in CORE_FIELDS]
        core_fields = [form_name] + [form[field] for field in CORE_FIELDS]
        found = False
        if 'results' in form.keys() and en(form['results']) > 0:
            found = True
            for result in form['results']:
                temp_header = list(result.keys())
                temp_fields = [i.strip() for i in result.values()]
                output.append(dict(zip(core_header+temp_header, core_fields+temp_fields)))
        
        if found == False: # if never found results section, append df indicating no data for file
            output.append(dict(zip(core_header, core_fields)))

        return pd.DataFrame(output)
    
    elif assay_type == 'igm.archer_fusion':
        output = [] # init list of dicts to make df of form specific results
        found = False
        core_header = ['form_name'] + [field for field in CORE_FIELDS]
        core_fields = [form_name] +  [form[field] for field in CORE_FIELDS]
        if 'fusion_tier_one_or_two_result' in form.keys():
            if 'variants' in form['fusion_tier_one_or_two_result'].keys() and len(form['fusion_tier_one_or_two_result']['variants']) > 0:
                found = True
                for result in form['fusion_tier_one_or_two_result']['variants']:
                    flatten_temp = flatten_igm(result)
                    temp_header = list(flatten_temp.keys())
                    temp_fields = [i.strip() for i in flatten_temp.values()]
                    output.append(dict(zip(core_header+temp_header, core_fields+temp_fields)))
        if found == False: # if never found results section, append df indicating no data for file
            output.append(dict(zip(core_header, core_fields)))

        return pd.DataFrame(output)
    
    else: # i.e. assay_type == 'igm/tumor_normal'
        all_output = [] #init list of dfs of each results section
        core_header = ['form_name'] + [field for field in CORE_FIELDS]
        core_fields = [form_name] + [form[field] for field in CORE_FIELDS]
        for results_type in ["somatic_results", "germline_results", "somatic_cnv_results"]:
            output = [] # init list of dicts to make df of form specific results
            found = False #if results/variants section found or not
            if results_type in form.keys():
                if 'variants' in form[results_type].keys() and len(form[results_type]['variants']) > 0:
                    found = True
                    for result in form[results_type]['variants']:
                        if results_type == 'somatic_cnv_results':
                            flatten_temp = flatten_igm(result, parse_type="cnv")
                            genes = flatten_temp['disease_associated_gene_content']
                            flatten_temp.pop('disease_associated_gene_content')
                            for gene in genes:
                                temp_header = list(flatten_temp.keys()) + ['gene']
                                temp_fields = [null_n_strip(i) for i in flatten_temp.values()] + [gene]
                                output.append(dict(zip(core_header+temp_header, core_fields+temp_fields)))
                        else:
                            flatten_temp = flatten_igm(result)
                            temp_header = list(flatten_temp.keys())
                            temp_fields = [null_n_strip(i) for i in flatten_temp.values()]
                            output.append(dict(zip(core_header+temp_header, core_fields+temp_fields)))
            if found == False: # if never found results section, append df indicating no data for file
                output.append(dict(zip(core_header, core_fields)))

            all_output.append(pd.DataFrame(output))

        return all_output
            


