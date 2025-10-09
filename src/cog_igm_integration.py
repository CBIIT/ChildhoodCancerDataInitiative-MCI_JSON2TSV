import openpyxl
from openpyxl import load_workbook
import sys
import pandas as pd
import os
import argparse
import logging
from datetime import datetime

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

def save_to_workbook(workbook_name, df, sheet_name, mode, reorder=None):
    # Only save if DataFrame is not empty
    if df.empty:
        logging.warning(f"Sheet '{sheet_name}' not saved because DataFrame is empty.")
        return

    if mode == 'a':
        with pd.ExcelWriter(workbook_name, engine='openpyxl', mode=mode, if_sheet_exists='replace') as writer:
            df.fillna('').replace('nan', '').to_excel(writer, sheet_name=sheet_name, index=False, na_rep='')
    elif mode == 'w':
        with pd.ExcelWriter(workbook_name, engine='openpyxl', mode=mode) as writer:
            df.fillna('').replace('nan', '').to_excel(writer, sheet_name=sheet_name, index=False, na_rep='')
    else:
        raise ValueError("Invalid mode. Use 'a' for append or 'w' for write.")

    if reorder:
        
        # Move the sheet to the front
        wb = load_workbook(workbook_name)
        ws = wb[sheet_name]
        wb._sheets.remove(ws)
        wb._sheets.insert(0, ws)
        wb.save(workbook_name)

###### IGM COG INTEGRATION ######
def cog_igm_integrate(cog_success_count, igm_success_count, integration_files, output_path, get_time):
    """Integrate COG and IGM data.

    Args:
        cog_success_count (int): Number of successful COG file conversions.
        igm_success_count (int): Number of successful IGM file conversions.
        integration_files (dict): Dictionary of integration file types and paths.
        output_path (str): Path to the output directory.
        get_time (str): Timestamp for the output files.

    Returns:
        None

    """
    
    # init logging
    logger = logging.getLogger("MCI_JSON2TSV_COG_IGM_INTEGRATION")
    
    logger.info("Performing COG and IGM data integration ...")

    logger.info(f"Files to check to integrate: {integration_files}")

    if (cog_success_count == 0) | (igm_success_count == 0):
        if cog_success_count == 0:
            logger.info("No COG files data to integrate with IGM files")
        if igm_success_count == 0:
            logger.info("No IGM files data to integrate with COG files")
        return None
    
    # check if COG and IGM files to parse
    COG = False
    IGM = False
    
    # list of dataframes to iterative merge
    df_list = []

    # init rem list for any files not found
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

    logger.info(f"\t>>> COG files found: {COG}, IGM files found: {IGM}")

    if (COG == False) | (IGM == False):
        logger.info("No COG and/or IGM files data to integrate.")
        return None
    if len(integration_files) < 2:
        logger.info("Missing COG or IGM files to integrate, will not perform integration.")
        return None
    
    # perform removals for any files not found
    for k in rem:
        del integration_files[k]

    logger.info(f"Files to integrate: {integration_files}")

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
        
        # get cols
        cols = int_df[int_df['source'] == k]['data_elements'].to_list()
        
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
        
        # append to list if has data/not Empty Data Frame
        if not temp_df.empty:
            df_list.append([k, temp_df])

    # output to file
    if len(df_list) > 0:
        logger.info(f"Merging {len(df_list)} dataframes ...")

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
        
        # init merged_df
        merged_df = df_list[0][1]
        
        # save first dataframe as new sheet to workbook
        save_to_workbook(workbook_name, merged_df, sheet_names[df_list[0][0]], 'w')

        for df in df_list[1:]:
            # save individual data to workbook
            save_to_workbook(workbook_name, df[1], sheet_names[df[0]], 'a')

            # if cols already exist in merged_df, drop them
            shared_cols = set(merged_df.columns).intersection(df[1].columns) - {"Subject ID"}
            df[1] = df[1].drop(columns=shared_cols, errors='ignore')
            
            # merge to merged_df
            merged_df = pd.merge(merged_df, df[1], on="Subject ID", how="outer")

        # drop duplicates if any in merged_df
        merged_df = merged_df.drop_duplicates()
        
        # get col order from int_df
        col_order = int_df[int_df['label'].isin(merged_df.columns)].sort_values('order')['label'].drop_duplicates().to_list()
        merged_df = merged_df[list(col_order)].fillna("")

        # save final integrated results
        save_to_workbook(workbook_name, merged_df, "COG IGM Integrated Results", 'a', reorder=True)

        return None
