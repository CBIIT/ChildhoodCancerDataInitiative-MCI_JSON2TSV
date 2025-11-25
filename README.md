# ChildhoodCancerDataInitiative-MCI_JSON2TSV

## Contents

[Introduction](#introduction)

[Requirements](#requirements)

[Installation](#installation)

[Usage](#usage)

[Questions or Contributions](#questions-or-contributions)

## Introduction

The MCI_JSON2TSV tool is a python-based script takes an input directory of COG and/or IGM formatted **Clinical Report** JSON files and transforms them into a set of parsed and flattened TSV files. Additionally, presence of both COG and IGM Clinical Report JSON files outputs an XLSX file that integrates data from both source types for given participants together for viewing. Please note that this script is not intended for the transformation of IGM molecular assay JSONs. Additionally, please consult the source files for additional clinical context. Parsed data are taken from clinician-interpreted forms that contain human written free-text and may contain typos or other human errors.

## Requirements

### Python Version

- Python 3.10 or higher

### Dependencies

- pandas 2.3 or higher
- numpy 2.3 or higher
- openpyxl 3.1 or higher

For developers, unit tests were tested using the following dependencies:

- pytest 8.3.4
- pytest-mock 3.14.8

## Installation

You can download the python scripts from the `/src` directory or clone the directory using: 

```bash
git clone https://github.com/CBIIT/ChildhoodCancerDataInitiative-MCI_JSON2TSV.git
```

To install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

`python MCI_JSON2TSV.py -d <input DIR> -o <output DIR>`

Required arguments:

`-d/--directory` : Path to directory containing JSON files to aggregate and transform.

`-o/--output_path` : Path to output directory to direct file outputs. Will throw error if directory already exists, to not overwrite existing directory.

## Questions or Contributions

For questions or to contribute, please reach out to <NCIChildhoodCancerDataInitiative@mail.nih.gov>
