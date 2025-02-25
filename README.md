# ChildhoodCancerDataInitiative-MCI_JSON2TSV

## Contents

[Introduction](#introduction)

[Requirements](#requirements)

[Installation](#installation)

[Usage](#usage)

[Functions Descriptions](#functions-descriptions)

[Questions or Contributions](#questions-or-contributions)

## Introduction

The MCI_JSON2TSV script takes a directory of COG and/or IGM formatted **Clinical Report** JSON files and transforms them into a single TSV file that aggregates fields from all forms types (i.e. Demographic, Treatment, Follow Up etc). Options also provided to additionally parse COG JSONs into separate TSVs by form type (option -f, i.e, for TSVs by DEMOGRAPHY, FINAL_DIAGNOSIS etc forms) or IGM JSONs into parsed variant result information TSVs (option -r, TSVs by methylation results, somatic and germline variant results etc). Please note that this script is not intended for the transformation of IGM molecular assay JSONs. 

## Requirements

### Python Version

- Python 3.8 or higher

### Dependencies

- pandas 2.0 or higher
- numpy 2.0 or higher

For developers, unit tests were tested using the following dependencies:

- pytest 8.3.4
- pytest-mock 3.14.8

## Installation

You can download the python scripts from the `/src` directory or clone the directory using: 

```bash
git clone https://github.com/CBIIT/ChildhoodCancerDataInitiative-MCI_JSON2TSV.git
```

## Usage

`python MCI_JSON2TSV.py -d <DIR> (-f <Bool>)`

Required arguments:

`-d/--directory` : Path to directory containing JSON files to aggregate and transform.

`-o/--output_path` : Path to output directory to direct file outputs.

Optional arguments:

`-f/--form_parsing` : Flag to indicate if parsing out COG TSVs by form should occur.

`-r/--results_variants_section_parse` : Flag to indicate if parsing out IGM variant results sections should occur.

## Functions Descriptions

### MCI_JSON2TSV.refresh_date()

Gets and returns current date and time

Args:
- None

Returns:
- Today's date and time at the time of function call

### MCI_JSON2TSV.distinguisher(f_path: str)

Attempt to load json and determine type

Args:
- f_path (str): Path to JSON file

Returns:
- str: File type (COG JSON, IGM JSON or other) or error

### MCI_JSON2TSV.distinguish(dir_path: str)

Function to distinguish between file types (COG JSON, IGM JSON or other)

Args:
- dir_path (str): Inout path containing files to convert

Returns:
- list: Three lists of file names for files by type (COG JSON, IGM JSON or other file)

### cog_utils.read_cog_jsons(dir_path: str, cog_jsons: list)

Reads in COG JSON files and return concatenated DataFrame.

Args:
- dir_path (str): The directory path containing the JSON files to be transformed
- cog_jsons (list): List of file names in directory path that are COG JSONs

Returns:
- pd.DataFrame: A DataFrame object that is a concatenation of the JSON files read into DataFrames
- int: success count
- int: error count

Raises:
- ValueError: If a given JSON file cannot be properly read and loaded in as a pandas DataFrame object

### cog_utils.custom_json_parser(pairs: dict)

Function to preserve duplicate key values found in some forms (i.e. Follow-Up). This is passed to the `object_pairs_hook` parameter in json.loads()

> The object_pairs_hook parameter allows you to intercept the key-value pairs of the JSON object before they are converted into a dictionary. This aids in accounting for multiple data keys.

Args:
- pairs (dict): key, value pairs recursively fed in from json.loads()

Returns:
- dict: A key-value pair in python dict type

### cog_utils.expand_cog_df(df: DataFrame)

Function to parse participant JSON and output TSV of values and column header reference

Args:
- df (pd.DataFrame): DataFrame of concatenated, normalized JSONs

Returns:
- pd.DataFrame: Transformed form values from JSON to pd.DataFrame with updated field names reflecting the form the field is derived from (e.g. DEMOGRAPHY.DM_BRTHDAT)
- pd.DataFrame: Column header reference (i.e. form field ID : SaS Label)

> To handle multiple instances of a given form (i.e. Follow-Ups) for a given participant/subject, the parsed ‘data’ objects of the form type is expected as a list of lists of dictionaries, for example: [[{field : value}, {field : value}], [{field : value}, {field : value}]], where the sub-list is a form instance, and is itself a list of dicts. Each form instance will be output as a row in the TSV, i.e. multiple rows per participant if there are multiple instances of a form for   the given participant.

### cog_utils.cog_to_tsv(dir_path: str, cog_jsons: list, cog_op: str, timestamp: str)

Function to call the reading in and transformation of JSON files.

Args:
- dir_path (str): Path to directory containing COG JSON files
- cog_jsons (list): List of COG JSON filenames located in dir_path
- cog_op (str): Path to directory to output transformed COG TSV files
- timestamp (str): Date-time of when script run

Returns:
- pd.DataFrame: dataframe of transformed and aggregated JSON files
- int: The count of JSON files successfully processed
- int: The count of JSON files unsuccessfully processed

### cog_utils.form_parser(df: pd.DataFrame, timestamp: str, cog_op: str) → pd.DataFrame

Split transformed JSON data into *separate* TSVs for each form type.

Args:
- df (pd.DataFrame): transformed form values from JSON to pd.DataFrame with updated field names reflecting the form the field is derived from (e.g. DEMOGRAPHY.DM_BRTHDAT)
- timestamp (str): Date-time of when script run
- cog_op (str): Path to output directory for COG files

Returns:
- pd.DataFrame: dataframe of transformed and aggregated JSON files of a single form type (e.g. Demography, Follow-Up etc.)

### igm_utils.null_n_strip(df: DataFrame, get_time: str)

Format strings in IGM JSONs

Args:
- value : Value read in from key:value pair in IGM JSON

Returns:
- If str, formatted str; elif None, empty str; else original value argument

### igm_utils.flatten_igm(json_obj: dict, parent_key='', flatten_dict=None, parse_type=None) → dict

Recursive function to un-nest a nested dictionary for WXS and Archer Fusion

Args:
- json_obj (dict): Nested JSON IGM form
- parent_key (str, optional): The inherited key from previous recursive run. Defaults to ''.
- flatten_dict (dict, optional): The inherited 'flattened' JSON from previous recursive run. Defaults to {}.
- parse_type (str, optional): When specified as 'cnv', for any key == 'disease_associated_gene_content', do not flatten value for that key

Returns:
- dict: Un-nested dict/JSON

### igm_utils.full_form_convert(flatten_dict: dict)

Convert flattened JSON to pd.DataFrame

Args:
 - flatten_dict (dict): IGM nested JSON that has been flattened to un-nested JSON

Returns:
- pd.DataFrame: The flattened JSON converted to pd.DataFrame


### igm_utils.igm_to_tsv(dir_path: str, igm_jsons: list, assay_type: str, igm_op: str, timestamp: str, results_parse: bool)

Function to call the reading in and transformation of IGM JSON files

Args:
- dir_path (str): Path to directory containing COG JSON files
- igm_jsons (list): List of COG JSON filenames located in dir_path
- assay_type (str): Molecular assay type of IGM JSONs (i.e. Archer Fusion, WXS or methylation)
- igm_op (str): Path to directory to output transformed IGM TSV files
- timestamp (str): Date-time of when script run
- results_parse (bool): If True, parse out results specific sections to separate form in long format TSV

Returns:
- pd.DataFrame: pandas DataFrame of converted JSON data
- pd.DataFrame: pandas DataFrame of converted JSON data from results sections to long format TSV(s)
- int: The count of JSON files successfully processed
- int: The count of JSON files unsuccessfully processed

### igm_utils.igm_results_variants_parsing(form: dict, form_name: str, assay_type: str, results_types: list)

Results section specific parsing (long format)

Args:
- form (dict): JSON form loaded in 
- form_name (str): File name of form data is sourced from
- assay_type (str): Molecular assay type of IGM JSONs (i.e. Archer Fusion, WXS or methylation)
- results_types (list): Potential results sections that may appear in form to parse

Raises:
- ValueError: If assay_type is not acceptable value

Returns:
- dict: dict of dataframes of parsed and formatted results section(s)

## Questions or Contributions

For questions or to contribute, please reach out to **TBD**