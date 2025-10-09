#!/usr/bin/env python3

"""MCI_JSON2TSV.py: Script to transform clinical JSON files to TSV format.
"""

##############
#
# Env. Setup
#
##############

import sys
import pandas as pd
import openpyxl
import os
import argparse
import logging
from datetime import datetime
import shutil

# utils
from cog_utils import cog_to_tsv, form_parser
from igm_utils import igm_to_tsv

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

import pandas as pd

import pandas as pd

def collapse_rows_to_wide(df, groupby_cols, agg_cols, delimiter=';'):
    """
    Collapse long-format rows into wide format by joining values of agg_cols with a delimiter,
    grouped by groupby_cols. All other columns are preserved by taking the first value.

    Parameters:
        df (pd.DataFrame): Input long-format DataFrame
        groupby_cols (list or str): Column(s) to group by
        agg_cols (list or str): Column(s) to aggregate and join
        delimiter (str): Delimiter to join values (default: ';')

    Returns:
        pd.DataFrame: Transformed wide-format DataFrame
    """
    if isinstance(groupby_cols, str):
        groupby_cols = [groupby_cols]
    if isinstance(agg_cols, str):
        agg_cols = [agg_cols]

    # All columns to preserve (agg + others)
    all_cols = set(df.columns)
    other_cols = list(all_cols - set(groupby_cols) - set(agg_cols))

    # Build aggregation dictionary
    agg_dict = {col: (lambda x: delimiter.join(map(str, x))) for col in agg_cols}
    agg_dict.update({col: 'first' for col in other_cols})

    # Group and aggregate
    result = df.groupby(groupby_cols).agg(agg_dict).reset_index()

    return result




def distinguisher(f_path: str, logger):
    """Attempt to load json and determine type

    Args:
        f_path (str): Path to JSON file

    Returns:
        str: File type (COG JSON, IGM JSON or other) or error
    """
    try:
        f_begin = open(f_path, "rb").read(1000)  # read in first 1000 bytes of file

        # check for identifiers in beginning of file:
        if "upi" in str(f_begin):
            return "cog"
        elif "report_type" in str(f_begin):
            if "archer_fusion" in str(f_begin):
                return "igm.archer_fusion"
            elif "tumor_normal" in str(f_begin):
                return "igm.tumor_normal"
            elif "methylation" in str(f_begin):
                return "igm.methylation"
            else:  # not known
                logger.error(f"Error reading file at {f_path}: IGM assay type unknown.")
                return "error"
        else:
            return "other"
    except Exception as e:
        logger.error(f"Error reading file at {f_path}: {e}")
        return "error"


def distinguish(dir_path: str, logger):
    """Function to distinguish between file types (COG JSON, IGM JSON or other)

    Args:
        dir_path (str): Inout path containing files to convert

    Returns:
        dict: Sorting of file names for files by type (COG JSON, IGM JSON, other file or error file)
    """

    # initialize dict of files by type
    sorted_dict = {
        key: []
        for key in [
            "cog",
            "igm.tumor_normal",
            "igm.archer_fusion",
            "igm.methylation",
            "other",
            "error",
        ]
    }

    # get list of files in directory
    if os.path.exists(dir_path):
        # filter out those that have suffix json
        json_files = [i for i in os.listdir(dir_path) if i.endswith(".json")]
        if len(json_files) == 0:
            logger.error(f"Input path {dir_path} does not contain any JSON files.")
            sys.exit(
                f"Process exited: Input path {dir_path} does not contain any JSON files, please check and try again."
            )
        else:
            for f in json_files:
                sorted_dict[distinguisher(f"{dir_path}/{f}", logger)].append(f)
    else:
        logger.error(f"Input path {dir_path} does not exist.")
        sys.exit(
            f"Process exited: Input path {dir_path} does not exist, please check and try again."
        )

    # attempt to read them in and check if they have IGM or COG key identifiers
    # segregate into dict of lists and return dict
    return sorted_dict


# main function
def json2tsv(json_dir_path, output_path):
    start_time = datetime.now()

    print("\n\t>>> Running MCI_JSON2TSV.py ....")

    # init logging
    logger = logging.getLogger("MCI_JSON2TSV")
    

    # logging config
    logging.basicConfig(
        filename=f"JSON2TSV.log",
        encoding="utf-8",
        filemode="w",
        level=logging.INFO,
        format="%(name)s - %(levelname)s - %(message)s",
)

    logger.info("Running MCI_JSON2TSV.py ....")
    get_time = refresh_date()

    # tracking for COG IGM integration
    integration_files = {}
    
    # make output_dir path if needed
    if not os.path.exists(output_path):
        os.mkdir(output_path)
    elif os.path.exists(output_path):
        # check if output path is empty
        if len(os.listdir(output_path)) > 0:
            logger.error(
                f"Output path {output_path} is not empty, please provide a new output path."
            )
            sys.exit(
                f"Process exited: Output path {output_path} is not empty, please provide a new output path."
            )

    ## function called here to distinguish between COG, IGM and non JSON in input path
    json_sorted = distinguish(json_dir_path, logger)

    ## if len(cog_jsons) AND len(igm_json) == 0, call error and sysexit
    #if len(json_sorted["cog"]) == 0 and len(json_sorted["igm"]) == 0:
    if sum([len(json_sorted[k]) for k in ["cog", "igm.methylation", "igm.archer_fusion", "igm.tumor_normal"]]) == 0:
        sys.exit(
            f"\n\t>>> No COG or IGM JSON files to covert in input directory {json_dir_path}, please check and try again."
        )

    # call cog_to_tsv function to read in and transform JSON files to TSV
    if len(json_sorted["cog"]) > 0:
        # make cog output dir path
        cog_op = f"{output_path}/COG"
        if not os.path.exists(cog_op):
            os.mkdir(cog_op)

        # transform COG JSONs and concatenate
        df_reshape, cog_flattened_file_name, cog_success_count, cog_error_count = cog_to_tsv(
            json_dir_path, json_sorted["cog"], cog_op, get_time
        )

        integration_files['COG'] = cog_flattened_file_name

        # perform form-level parsing for COG JSONs
        if len(df_reshape) > 0:
            form_parser(df_reshape, get_time, cog_op)
        else:
            logger.error(
                "Cannot perform COG form-level parsing, no valid COG JSONs read in."
            )
    else:
        cog_success_count = 0
        cog_error_count = 0

    if (
        len(
            json_sorted["igm.archer_fusion"]
            + json_sorted["igm.tumor_normal"]
            + json_sorted["igm.methylation"]
        )
        > 0
    ):
        # init counts
        igm_success_count = 0
        igm_error_count = 0

        # make igm output dir path
        igm_op = f"{output_path}/IGM"
        if not os.path.exists(igm_op):
            os.mkdir(igm_op)

        # for each assay type, flatten JSON files and concatenate
        for assay_type in ["igm.tumor_normal", "igm.archer_fusion", "igm.methylation"]:
            if len(json_sorted[assay_type]) > 0:
                parsed_results_dict, temp_success_count, temp_error_count = igm_to_tsv(
                    json_dir_path, json_sorted[assay_type], assay_type, igm_op, get_time
                )

                igm_success_count += temp_success_count
                igm_error_count += temp_error_count
                # add to integration files dict
                for result_type, parsed_results_file in parsed_results_dict.items():
                    integration_files[result_type] = parsed_results_file

            else:
                print(f"No IGM JSONs of type {assay_type}")
    else:
        igm_success_count = 0
        igm_error_count = 0
    
    ###### IGM COG INTEGRATION ######
    
    print("\n\t>>> Performing COG and IGM data integration ...")
    
    print(f"\t>>> COG files to integrate: {integration_files}")
    
    # check if COG and IGM files to parse
    COG = False
    IGM = False
    
    # list of dataframes to iterative merge
    df_list = []

    if (cog_success_count > 0) & (igm_success_count > 0):
        rem = []
        for k, v in integration_files.items():
            if (k == 'COG') & (v != ''):
                if os.path.exists(v):
                    COG = True
                else:
                    logger.error(f"Parsed file {v} does not exist at path, not integrating in COG/IGM integration.")
            elif v != '':
                if os.path.exists(v):
                    IGM = True
                else:
                    logger.error(f"Parsed file {v} does not exist at path, will not be included for COG/IGM integration.")
                    rem.append(k)
                    
        print(f"\t>>> COG files found: {COG}, IGM files found: {IGM}")
        
        # perform removals for any files not found
        for k in rem:
            del integration_files[k]
        
        print(f"\t>>> files to integrate: {integration_files}")
        
        if (COG == False) | (IGM == False):
            logger.info("No COG and/or IGM files data to integrate.")
        elif len(integration_files) < 2:
            logger.error("Missing COG or IGM files to integrate, will not perform integration.")
        else:                
            # read in integration mapping dict
            # Get the directory of the current script
            script_dir = os.path.dirname(os.path.abspath(__file__))

            # Construct full path to the TSV file
            file_path = os.path.join(script_dir, 'integration_mapping_dict.tsv')

            int_df = pd.read_csv(file_path, sep="\t")

            # read in each file, parse out columns, merge on participant, remove dups
            for k, v in integration_files.items():
                if k == 'COG':
                    # data starts at row 2, drop rows missing upi
                    temp_df = pd.read_csv(v, sep="\t", skiprows=[1,2])
                else:
                    temp_df = pd.read_csv(v, sep="\t")
                    print(f"source: {k}")
                
                # get cols
                cols = int_df[int_df['source'] == k]['data_elements'].to_list()
                
                print(cols)
                
                #check if cols specified in cols in temp_df
                if not all(col in temp_df.columns for col in cols):
                    logger.error(f"Not all columns specified in integration mapping found in {k} dataframe.")
                    continue

                # subset
                temp_df = temp_df[cols]
                
                # check in int_df for data_elements that need to be transposed
                # i.e. transpose == 'yes'

                to_transpose = int_df[(int_df['source'] == k) & (int_df['modifier'] == 'transpose')]['data_elements'].to_list()
                
                if len(to_transpose) > 0:
                    # collapse rows to wide format
                    temp_df = collapse_rows_to_wide(temp_df, groupby_cols='subject_id', agg_cols=to_transpose, delimiter=';')

                # check for multi-field labels
                combos = int_df[int_df['source'] == k].groupby('label').size().reset_index(name='counts')
                multi_field_labels = combos[combos['counts'] > 1]['label'].to_list()

                # for multi-field labels, find the data_elements
                # and create a new col with label name with concatenated values
                # from the data_elements cols
                if len(multi_field_labels) > 0:
                    for m in multi_field_labels:
                        mult_cols = int_df[(int_df['source'] == k) & (int_df['label'] == m)]['data_elements'].to_list()
                        
                        # create new col with concatenated values, excluding nans
                        temp_df[m] = temp_df[mult_cols].apply(lambda x: ';'.join(x.dropna().astype(str)), axis=1)

                        # drop the individual cols
                        temp_df = temp_df.drop(columns=mult_cols)

                # rename cols to labels for cols in temp_df
                col_headers = int_df[(int_df['source'] == k) & (int_df['data_elements'].isin(temp_df.columns))]['label'].to_list()
                temp_df = temp_df.rename(columns=dict(zip(cols, col_headers)))
                
                print(temp_df)
                
                # append to list if has data/not Empty Data Frame
                if not temp_df.empty:
                    df_list.append([k, temp_df])

            # output to file
            if len(df_list) > 0:
                print(f"\t>>> Merging {len(df_list)} dataframes ...")
                
                workbook_name = f"{output_path}/COG_IGM_integrated_{get_time}.xlsx"
                
                sheet_names = {
                    'COG': 'COG',
                    "fusion_tier_one_or_two_result": "IGM ArcherFusion",
                    "single_tier_one_or_two_result": "IGM ArcherFusion IntraGene",
                    "germline_cnv_results": "IGM TmrNmrl CNV Variants",
                    "germline_results": "IGM TmrNmrl Germline Variants",
                    "somatic_cnv_results": "IGM TmrNmrl CNV Variants",
                    "somatic_results": "IGM TmrNmrl Somatic Variants",
                    "final_diagnosis": "IGM Methylation Classifier"
                }
                
                # Merge on shared column, e.g., 'Subject ID'
                # if cols already exist in merged_df, drop them
                merged_df = df_list[0][1]
                
                # save first dataframe as new sheet to workbook
                with pd.ExcelWriter(workbook_name, engine='openpyxl', mode='w') as writer:
                    merged_df.fillna('').replace('nan', '').to_excel(writer, sheet_name=sheet_names[df_list[0][0]], index=False)

                for df in df_list[1:]:
                    # save to workbook
                    with pd.ExcelWriter(workbook_name, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                        df[1].fillna('').replace('nan', '').to_excel(writer, sheet_name=sheet_names[df[0]], index=False)

                    # if cols already exist in merged_df, drop them
                    shared_cols = set(merged_df.columns).intersection(df[1].columns) - {"Subject ID"}
                    df[1] = df[1].drop(columns=shared_cols, errors='ignore')
                    merged_df = pd.merge(merged_df, df[1], on="Subject ID", how="outer")

                # drop duplicates if any
                merged_df = merged_df.drop_duplicates()
                
                # get col order from int_df
                col_order = int_df[int_df['label'].isin(merged_df.columns)].sort_values('order')['label'].drop_duplicates().to_list()
                merged_df = merged_df[list(col_order)].fillna("")

                with pd.ExcelWriter(workbook_name, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                    merged_df.fillna('').replace('nan', '').to_excel(writer, sheet_name="COG IGM Integrated Results", index=False, na_rep='')

    else:
        if cog_success_count == 0:
            logger.info("No COG files data to integrate with IGM files")
        if igm_success_count == 0:
            logger.info("No IGM files data to integrate with COG files")
    

    if len(json_sorted["other"]) > 0:
        # save list of others to output dir
        with open(f"{output_path}/other_jsons_{get_time}.txt", "w+") as w:
            w.write("\n".join(json_sorted["other"]))
        w.close()

    if len(json_sorted["error"]) > 0:
        # save list of error JSONs that could not have type determined to output dir
        with open(f"{output_path}/undertermined_jsons_{get_time}.txt", "w+") as w:
            w.write("\n".join(json_sorted["error"]))
        w.close()

    end_time = datetime.now()
    time_diff = end_time - start_time
    print(f"\n\t>>> Time to Completion: {time_diff}")
    logger.info(f"Time to Completion: {time_diff}")
    print(f"\t>>> # COG JSON Files Successfully Transformed: {cog_success_count}")
    if cog_error_count > 0:
        print(
            f"\t>>> # COG JSON Files NOT Transformed (Errors): {cog_error_count}, check log file {output_path}/JSON2TSV_{get_time}.log for errors"
        )
    else:
        print(f"\t>>> # COG JSON Files NOT Transformed (Errors): {cog_error_count}")
    print(f"\t>>> # IGM JSON Files Successfully Transformed: {igm_success_count}")
    if igm_error_count > 0:
        print(
            f"\t>>> # IGM JSON Files NOT Transformed (Errors): {igm_error_count}, check log file {output_path}/JSON2TSV_{get_time}.log for errors \n"
        )
    else:
        print(f"\t>>> # IGM JSON Files NOT Transformed (Errors): {igm_error_count}")
    print(
        f"\t>>> Check log file JSON2TSV_{get_time}.log for additional information\n"
    )

    # move log file to output dir and shutdown logging
    logging.shutdown()
    shutil.move("JSON2TSV.log", f"{output_path}/JSON2TSV_{get_time}.log")



if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(
        prog="MCI_JSON2TSV.py",
        description="This script will take a folder of JSON files, \
        both COG and IGM, for the MCI project and return a TSV data file \
        and data dictionary. JSON files MUST have suffix .json to be included in conversion.",
    )

    # remove built in argument group
    parser._action_groups.pop()

    # create a required arguments group
    required_arg = parser.add_argument_group("required arguments")

    required_arg.add_argument(
        "-d",
        "--directory",
        type=str,
        help="A directory of MCI JSON files, COG and/or IGM.",
        required=True,
    )

    required_arg.add_argument(
        "-o",
        "--output_path",
        type=str,
        help="Path to output directory to direct file outputs.",
        required=True,
    )

    args = parser.parse_args()

    # pull in args as variables
    json_dir_path = args.directory
    output_path = args.output_path

    json2tsv(json_dir_path, output_path)
