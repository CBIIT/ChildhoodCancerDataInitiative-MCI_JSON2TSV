#!/usr/bin/env python3

"""MCI_JSON2TSV.py: Script to transform clinical JSON files to TSV format."""

##############
#
# Env. Setup
#
##############

import sys
import pandas as pd
import os
import argparse
import logging
from datetime import datetime
import shutil

# utils
from cog_utils import cog_to_tsv, form_parser
from igm_utils import igm_to_tsv
from cog_igm_integration import cog_igm_integrate

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
def json2tsv(json_dir_path, output_path, logger=None):
    start_time = datetime.now()

    print("\n\t>>> Running MCI_JSON2TSV.py ....")
    get_time = refresh_date()

    if not logger:
        logger = logging.getLogger("MCI_JSON2TSV")
        
        logging.basicConfig(
        filename=f"JSON2TSV.log",
        encoding="utf-8",
        filemode="w",
        level=logging.INFO,
        format=">>> %(name)s - %(asctime)s - %(levelname)s - %(message)s\n",
        force=True,
    )

    logger.info("Running MCI_JSON2TSV.py ....")    

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
    # if len(json_sorted["cog"]) == 0 and len(json_sorted["igm"]) == 0:
    if (
        sum(
            [
                len(json_sorted[k])
                for k in [
                    "cog",
                    "igm.methylation",
                    "igm.archer_fusion",
                    "igm.tumor_normal",
                ]
            ]
        )
        == 0
    ):
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
        df_reshape, cog_flattened_file_name, cog_success_count, cog_error_count = (
            cog_to_tsv(json_dir_path, json_sorted["cog"], cog_op, get_time)
        )

        integration_files["COG"] = cog_flattened_file_name

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

    # perform COG and IGM data integration if both COG and IGM JSONs were present
    integrate = cog_igm_integrate(
        cog_success_count, igm_success_count, integration_files, output_path, get_time, logger
    )

    if integrate:
        print("\n\t>>> COG and IGM data integration complete.")
        logger.info("COG and IGM data integration complete.")
    else:
        print(
            "\n\t>>> COG and IGM data integration not performed. If expected to be performed, please check log for errors."
        )
        logger.info(
            "COG and IGM data integration not performed. If expected to be performed, please check log for errors."
        )

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
    logger.info(f"# COG JSON Files Successfully Transformed: {cog_success_count}")
    if cog_error_count > 0:
        print(
            f"\t>>> # COG JSON Files NOT Transformed (Errors): {cog_error_count}, check log file {output_path}/JSON2TSV_{get_time}.log for errors"
        )
        logger.info(
            f"# COG JSON Files NOT Transformed (Errors): {cog_error_count}, check log file {output_path}/JSON2TSV_{get_time}.log for errors"
        )
    else:
        print(f"\t>>> # COG JSON Files NOT Transformed (Errors): {cog_error_count}")
        logger.info(f"# COG JSON Files NOT Transformed (Errors): {cog_error_count}")
    print(f"\t>>> # IGM JSON Files Successfully Transformed: {igm_success_count}")
    logger.info(f"# IGM JSON Files Successfully Transformed: {igm_success_count}")
    if igm_error_count > 0:
        print(
            f"\t>>> # IGM JSON Files NOT Transformed (Errors): {igm_error_count}, check log file {output_path}/JSON2TSV_{get_time}.log for errors \n"
        )
        logger.info(
            f"# IGM JSON Files NOT Transformed (Errors): {igm_error_count}, check log file {output_path}/JSON2TSV_{get_time}.log for errors"
        )
    else:
        print(f"\t>>> # IGM JSON Files NOT Transformed (Errors): {igm_error_count}")
        logger.info(f"# IGM JSON Files NOT Transformed (Errors): {igm_error_count}")
    print(f"\t>>> Check log file JSON2TSV_{get_time}.log for additional information\n")
    logger.info(f"Check log file JSON2TSV_{get_time}.log for additional information")

    # move log file to output dir and shutdown logging
    #logging.shutdown()
    #shutil.move("JSON2TSV.log", f"{output_path}/JSON2TSV_{get_time}.log")


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
    json_dir_path = args.directory
    output_path = args.output_path

    try:
        json2tsv(json_dir_path, output_path)
    except Exception as e:
        logging.error(f"Unhandled exception: {e}", exc_info=True)
        print(f"Error occurred: {e}")
    """finally:
        logging.shutdown()
        # Move log file if it exists
        log_file = "JSON2TSV.log"
        if os.path.exists(log_file):
            # Use a timestamp for consistency
            from datetime import datetime

            get_time = datetime.today().strftime("%Y%m%d_%H%M%S")
            shutil.move(log_file, f"{output_path}/JSON2TSV_{get_time}.log")"""
