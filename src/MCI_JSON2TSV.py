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


def main():
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

    logger.info(">>> Running cog_igm_transformer.py ....")

    get_time = refresh_date()

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
    optional_arg = parser.add_argument_group("optional arguments")  ##FP

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

    optional_arg.add_argument(
        "-f",
        "--form_parse",
        help="Flag to indicate if parsing out COG TSVs by form should occur.",
        default=False,
        action="store_true",
    )

    optional_arg.add_argument(
        "-r",
        "--results_variants_section_parse",
        help="Flag to indicate if parsing out IGM variant results sections should occur.",
        default=False,
        action="store_true",
    )

    args = parser.parse_args()

    # pull in args as variables
    json_dir_path = args.directory
    output_path = args.output_path
    form_parse = args.form_parse  ##FP
    results_parse = args.results_variants_section_parse

    # make output_dir path if needed
    if not os.path.exists(output_path):
        os.mkdir(output_path)

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
        df_reshape, cog_success_count, cog_error_count = cog_to_tsv(
            json_dir_path, json_sorted["cog"], cog_op, get_time
        )

        # if -f option to parse by form, run form_parser
        if form_parse:
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
                df_reshape, temp_success_count, temp_error_count = igm_to_tsv(
                    json_dir_path, json_sorted[assay_type], assay_type, igm_op, get_time, results_parse
                )

                igm_success_count += temp_success_count
                igm_error_count += temp_error_count

            else:
                print(f"No IGM JSONs of type {assay_type}")
    else:
        igm_success_count = 0
        igm_error_count = 0
        

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
    logger.info(f">>> Time to Completion: {time_diff}")
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

    main()
