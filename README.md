# ChildhoodCancerDataInitiative-MCI_JSON2TSV

## Contents

[Introduction](#introduction)

[Requirements](#requirements)

[Installation](#installation)

[Usage](#usage)

[Functions Descriptions](#functions-descriptions)

[Questions or Contributions](#questions-or-contributions)

## Introduction

The MCI_JSON2TSV script takes a directory of COG formatted JSON files and transforms them into a single TSV file that aggregates fields from all forms types (i.e. Demographic, Treatment, Follow Up etc.).

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

You can download the raw python script from the `/src` directory or clone the directory using: 

```bash
git clone <URL>
```

## Usage

`python MCI_JSON2TSV.py -d <DIR> (-f <Bool>)`

Required arguments:

`-d/--directory` : Path to directory containing JSON files to aggregate and transform.

Optional arguments:

`-f/--form_parsing` : Boolean indicating if parsing out TSVs by form type should occur.

## Functions Descriptions

### MCI_JSON2TSV.refresh_date()

Gets and returns current date and time

Args:
- None

Returns:
- Today's date and time at the time of function call

### MCI_JSON2TSV.read_cog_jsons(dir_path: str)

Reads in COG JSON files and return concatenated DataFrame.

Args:
- dir_path (str): The directory path containing the JSON files to be transformed

Returns:
- pd.DataFrame: A DataFrame object that is a concatenation of the JSON files read into DataFrames

Raises:
- ValueError: If a given JSON file cannot be properly read and loaded in as a pandas DataFrame object

### MCI_JSON2TSV.custom_json_parser(pairs: dict)

Function to preserve duplicate key values found in some forms (i.e. Follow-Up). This is passed to the `object_pairs_hook` parameter in json.loads()

> The object_pairs_hook parameter allows you to intercept the key-value pairs of the JSON object before they are converted into a dictionary. This aids in accounting for multiple data keys.

Args:
- pairs (dict): key, value pairs recursively fed in from json.loads()

Returns:
- dict: A key-value pair in python dict type

### MCI_JSON2TSV.expand_cog_df(df: DataFrame)

Function to parse participant JSON and output TSV of values and column header reference

Args:
- df (pd.DataFrame): DataFrame of concatenated, normalized JSONs

Returns:
- pd.DataFrame: Transformed form values from JSON to pd.DataFrame with updated field names reflecting the form the field is derived from (e.g. DEMOGRAPHY.DM_BRTHDAT)
- pd.DataFrame: Column header reference (i.e. form field ID : SaS Label)

> To handle multiple instances of a given form (i.e. Follow-Ups) for a given participant/subject, the parsed ‘data’ objects of the form type is expected as a list of lists of dictionaries, for example: [[{field : value}, {field : value}], [{field : value}, {field : value}]], where the sub-list is a form instance, and is itself a list of dicts. Each form instance will be output as a row in the TSV, i.e. multiple rows per participant if there are multiple instances of a form for   the given participant.

### MCI_JSON2TSV.cog_to_tsv(dir_path: str)

Function to call the reading in and transformation of JSON files.

Args:
- dir_path (str): Path to directory containing JSON files

Returns:
- pd.DataFrame: dataframe of transformed and aggregated JSON files
- pd.DataFrame: dataframe of Column header reference (i.e. form field ID : SaS Label)
- int: The count of JSON files successfully processed
- int: The count of JSON files unsuccessfully processed

### MCI_JSON2TSV.form_parser(df: DataFrame, get_time: str) → DataFrame

Split transformed JSON data into *separate* TSVs for each form type.

Args:
- df (pd.DataFrame): transformed form values from JSON to pd.DataFrame with updated field names reflecting the form the field is derived from (e.g. DEMOGRAPHY.DM_BRTHDAT)

Returns:
- pd.DataFrame: dataframe of transformed and aggregated JSON files of a single form type (e.g. Demography, Follow-Up etc.)

## Questions or Contributions

For questions or to contribute, please reach out to **some_email_here.**