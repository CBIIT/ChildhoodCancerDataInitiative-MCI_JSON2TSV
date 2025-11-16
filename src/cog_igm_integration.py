import openpyxl
from openpyxl import load_workbook
import sys
import pandas as pd
import os
import argparse
import logging
from datetime import datetime

import pandas as pd

PRIMARY_GROUPS = {
    'CNS' : ['Central Nervous System', 'Germ Cell Tumor'],
    'STS' : ['Soft Tissue Sarcoma'],
    'NBL' : ['Neuroblastoma'],
    'RARE' : ['Rare Tumors'],
    'EWS' : ['Ewing Sarcoma Tumor']
}

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
    
    # Use lambda to safely capture delimiter
    def join_with_not_reported(x):
        values = []
        for val in x:
            if pd.isna(val) or str(val).strip() == '':
                values.append('Not Reported')
            else:
                values.append(str(val).strip())
        return delimiter.join(values) if values else ''
    
    if isinstance(groupby_cols, str):
        groupby_cols = [groupby_cols]
    if isinstance(agg_cols, str):
        agg_cols = [agg_cols]
        
    if 'ON_STUDY_DX.SUPPMH_QVAL_MHIDRF_LOG' in df.columns and 'ON_STUDY_DX.SUPPMH_QVAL_MHIDRF_LOG' in agg_cols:
        # perform aggregation of this column first to avoid issues with large text
        df['ON_STUDY_DX.SUPPMH_QVAL_MHIDRF_LOG'] = df.groupby(groupby_cols)['ON_STUDY_DX.SUPPMH_QVAL_MHIDRF_LOG'].transform(
            lambda x: delimiter.join([str(val).strip() for val in x if pd.notna(val) and str(val).strip() != '']) if not x.empty else 'Not Reported'
        )
        df['ON_STUDY_DX.SUPPMH_QVAL_MHIDRF_LOG'] = df['ON_STUDY_DX.SUPPMH_QVAL_MHIDRF_LOG'].str.strip(delimiter)
        df = df.drop_duplicates()
        agg_cols.remove('ON_STUDY_DX.SUPPMH_QVAL_MHIDRF_LOG')

    # Only keep valid columns in agg_cols
    agg_cols = [col for col in agg_cols if col in df.columns]
    groupby_cols = [col for col in groupby_cols if col in df.columns]
    
    if not agg_cols:
        raise ValueError("No valid agg_cols found in DataFrame.")
    
    all_cols = set(df.columns)
    other_cols = list(all_cols - set(groupby_cols) - set(agg_cols))

    # Build aggregation dictionary
    agg_dict = {col: join_with_not_reported for col in agg_cols}
    agg_dict.update({col: 'first' for col in other_cols})

    # Group and aggregate
    result = df.groupby(groupby_cols, dropna=False).agg(agg_dict).reset_index()
    
    # Drop duplicates if any
    result = result.drop_duplicates()

    # Clean up leading/trailing delimiters
    for col in agg_cols:
        result[col] = result[col].str.strip(delimiter)

    return result


def save_to_workbook(workbook_name, df, sheet_name, mode, reorder=None):
    # Only save if DataFrame is not empty
    if df.empty:
        logging.warning(f"Sheet '{sheet_name}' not saved because DataFrame is empty.")
        return
    
    # replace instances of a cell only containing 'Not Reported' with ''
    df = df.replace({'^Not Reported$': ''}, regex=True)

    if mode == 'a':
        with pd.ExcelWriter(workbook_name, engine='openpyxl', mode=mode, if_sheet_exists='replace') as writer:
            df.fillna('').to_excel(writer, sheet_name=sheet_name, index=False, na_rep='')
    elif mode == 'w':
        with pd.ExcelWriter(workbook_name, engine='openpyxl', mode=mode) as writer:
            df.fillna('').to_excel(writer, sheet_name=sheet_name, index=False, na_rep='')
    else:
        raise ValueError("Invalid mode. Use 'a' for append or 'w' for write.")

    if reorder:
        
        # Move the sheet to the front
        wb = load_workbook(workbook_name)
        ws = wb[sheet_name]
        wb._sheets.remove(ws)
        wb._sheets.insert(0, ws)
        wb.save(workbook_name)

def substudy_sheet(substudy: str, int_df: pd.DataFrame, df_list: list, igm_merged_df: pd.DataFrame, workbook_name: str, logger, other=False):
    
    if other:
        ss_cols = int_df[int_df['substudy'] == 'ALL']['label'].to_list()
    else:
        # get all cols with the substudy prefix OR == 'ALL'
        ss_cols = int_df[(int_df['substudy'] == substudy) | (int_df['substudy'] == 'ALL')]['label'].unique().tolist()
    
    # subset COG df in df_list with cols in ss_cols
    cog_df = [df for df in df_list if df[0] == 'COG'][0][1]
    cog_subset_df = cog_df[cog_df.columns.intersection(ss_cols)]
    
    # subset COG df for cases with in primary group that matches substudy
    
    if other:
        # subset COG df for cases with in primary group that does not match any substudy
        primary_disease_groups = [item for sublist in PRIMARY_GROUPS.values() for item in sublist]
        cog_prim_dis_subset_df = cog_subset_df[~cog_subset_df['Primary Disease Group/Sub-study'].isin(primary_disease_groups)]
    else:
        primary_disease_groups = PRIMARY_GROUPS.get(substudy, [])
        cog_prim_dis_subset_df = cog_subset_df[cog_subset_df['Primary Disease Group/Sub-study'].isin(primary_disease_groups)]


    if len(cog_prim_dis_subset_df) == 0:
        logger.warning(f"No COG cases found for primary disease group '{substudy}'")
        return None
    
    # merge with igm_merged_df on Subject ID
    merged_df = pd.merge(cog_prim_dis_subset_df, igm_merged_df, on="Subject ID", how="left").drop_duplicates()

    # if substudy != CNS, drop any columns with labels for type == 'igm.methylation' in int_df
    # except for Subject_ID
    if substudy != 'CNS' or other:
        igm_methylation_cols = int_df[(int_df['type'] == 'igm.methylation') & (int_df['label'] != 'Subject ID')]['label'].tolist()
        merged_df = merged_df.drop(columns=igm_methylation_cols, errors='ignore')

    # get col order from int_df
    col_order = int_df[int_df['label'].isin(merged_df.columns)].sort_values('integrated_sheet_order')['label'].drop_duplicates().to_list()
    merged_df = merged_df[list(col_order)].fillna("")
    
    # save merged dataframe to sheet with substudy name
    if other:
        save_to_workbook(workbook_name, merged_df, f"COG IGM Integrated - Other", 'a', reorder=True)
    else:
        save_to_workbook(workbook_name, merged_df, f"COG IGM Integrated - {substudy}", 'a', reorder=True)

    return None


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

    logger.info(f"COG files found: {COG}, IGM files found: {IGM}")

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
    file_path = os.path.join(script_dir, 'integration_mapping_dict.xlsx')

    int_df = pd.read_excel(file_path)

    # read in each file, parse out columns, merge on participant, remove dups
    for k, v in integration_files.items():
        if k == 'COG':
            # data starts at row 2, drop rows missing upi
            temp_df = pd.read_csv(v, sep="\t", skiprows=[1,2], low_memory=False)
        else:
            temp_df = pd.read_csv(v, sep="\t", low_memory=False)
            
            # remove all asterisks from data, either single or double (* or **)
            temp_df = temp_df.replace({r'\*{1,2}': ''}, regex=True)
        
        # get cols
        cols = int_df[int_df['source'] == k]['data_elements'].to_list()
        
        #check if column names specified in COG cols are present in COG temp_df
        if not all(col in temp_df.columns for col in cols) and k == 'COG':
            # record not found cols
            missing_cols = [col for col in cols if col not in temp_df.columns]
            logger.warning(f"Not all columns specified in integration mapping found in {k} dataframe. Columns not found: {missing_cols}")

            parse_cols = [col for col in cols if col in temp_df.columns]
        
        # check if IGM file has no data to parse
        elif not all(col in temp_df.columns for col in cols) and k != 'COG':
            # log info that file has no data to parse as not all cols
            # were produced from IGM parsing
            logger.info(f"Not all columns specified in integration mapping found in {k} dataframe. Source data may not have had variants/alterations identified for these participants. Skipping {k} dataframe for integration.")
            
            # skip rest of loop
            continue
        else: #all cols found
            parse_cols = cols
        
        # subset
        temp_df = temp_df[parse_cols]
        
        # check for multi-field labels
        combos = int_df[int_df['source'] == k].groupby('label').size().reset_index(name='counts')
        multi_field_labels = combos[combos['counts'] > 1]['label'].to_list()
        
        # for multi-field labels, find the data_elements
        # and create a new col with label name with concatenated values
        # from the data_elements cols
        if len(multi_field_labels) > 0:
            for m in multi_field_labels:
                mult_cols = int_df[(int_df['source'] == k) & (int_df['label'] == m) & (int_df['data_elements'].isin(parse_cols))]['data_elements'].to_list()
                
                # create new col with concatenated values, excluding nans
                temp_df[m] = temp_df[mult_cols].apply(lambda x: ','.join(x.dropna().astype(str)), axis=1)

                # drop the individual cols
                temp_df = temp_df.drop(columns=mult_cols)

        # replace instances of 2+ , with only one
        temp_df = temp_df.replace({',{2,}': ','}, regex=True)

        # remove trailing or lone ',' in dataframe
        temp_df = temp_df.replace({',$': '', '^,': ''}, regex=True)
        
        # check in int_df for data_elements that need to be transposed
        # i.e. modifier == 'transpose'

        to_transpose = int_df[(int_df['source'] == k) & (int_df['modifier'] == 'transpose')]['data_elements'].to_list()
        
        if len(to_transpose) > 0:
            if k == 'COG':
                # collapse rows to wide format
                temp_df = collapse_rows_to_wide(temp_df, groupby_cols='upi', agg_cols=to_transpose, delimiter=';')

                
            else:
                # collapse rows to wide format
                temp_df = collapse_rows_to_wide(temp_df, groupby_cols='subject_id', agg_cols=to_transpose, delimiter=';')

        
        # rename cols to labels for cols in temp_df
        # Create mapping for current columns in temp_df (after transpose/multi-field processing)
        current_cols = list(temp_df.columns)
        col_mapping = {}
        for col in current_cols:
            # Find the corresponding label for this data_element
            label_match = int_df[(int_df['source'] == k) & (int_df['data_elements'] == col)]['label']
            if not label_match.empty:
                col_mapping[col] = label_match.iloc[0]

        temp_df = temp_df.rename(columns=col_mapping)
        
        
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
            "germline_cnv_results": "IGM TmrNmrl Germ CNV Variants",
            "germline_results": "IGM TmrNmrl Germline Variants",
            "somatic_cnv_results": "IGM TmrNmrl Soma CNV Variants",
            "somatic_results": "IGM TmrNmrl Somatic Variants",
            "final_diagnosis": "IGM Methylation Classifier",
            "pertinent_negatives_results": "IGM Pertinent Negatives Results"
        }
        
        
        #merge igm dataframes iteratively together
        igm_dfs = [df for df in df_list if df[0] != 'COG']
        
        if len(igm_dfs) > 1:
            igm_merged_df = igm_dfs[0][1]
            
            # order cols in order from order in int_df
            col_order = int_df[int_df['label'].isin(igm_merged_df.columns)].sort_values('igm_sheet_order')['label'].drop_duplicates().to_list()
            igm_merged_df = igm_merged_df[list(col_order)]
            
            # save first dataframe as new sheet to workbook
            save_to_workbook(workbook_name, igm_merged_df, sheet_names[igm_dfs[0][0]], 'w')
            
            for df in igm_dfs[1:]:
                # order cols in order from order in int_df
                col_order = int_df[int_df['label'].isin(df[1].columns)].sort_values('igm_sheet_order')['label'].drop_duplicates().to_list()
                df[1] = df[1][list(col_order)]

                # save individual data to workbook
                save_to_workbook(workbook_name, df[1], sheet_names[df[0]], 'a')

                # if cols already exist in merged_df, drop them
                shared_cols = set(igm_merged_df.columns).intersection(df[1].columns) - {"Subject ID"}
                df[1] = df[1].drop(columns=shared_cols, errors='ignore')
                
                # merge to merged_df
                igm_merged_df = pd.merge(igm_merged_df, df[1], on="Subject ID", how="outer")
            
            # drop duplicates if any in merged_df
            igm_merged_df = igm_merged_df.drop_duplicates()
        
        else:
            # order cols in order from order in int_df
            col_order = int_df[int_df['label'].isin(igm_dfs[0][1].columns)].sort_values('igm_sheet_order')['label'].drop_duplicates().to_list()
            igm_merged_df = igm_dfs[0][1][list(col_order)]
            
            # save single dataframe as new sheet to workbook
            save_to_workbook(workbook_name, igm_merged_df, sheet_names[igm_dfs[0][0]], 'w')
        
        # get unique substudies from substudy col in int_df that != 'ALL'
        substudies = int_df[(int_df['substudy'] != 'ALL') & (int_df['substudy'].notnull())]['substudy'].unique().tolist()

        # run for other
        substudy_sheet(substudy='Other', int_df=int_df, df_list=df_list, igm_merged_df=igm_merged_df, workbook_name=workbook_name, logger=logger, other=True)
        
        for ss in substudies:
            substudy_sheet(substudy=ss, int_df=int_df, df_list=df_list, igm_merged_df=igm_merged_df, workbook_name=workbook_name, logger=logger, other=False)

        return True
