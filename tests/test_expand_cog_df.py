import pytest
import pandas as pd
from src.cog_utils import expand_cog_df


def test_expand_cog_df_success():
    # mock a pd.DataFrame that is in the expected format

    mock_json = [
        {
            "upi": "SUBJ1",
            "version": "1.0.3.0",
            "index_date_type": "date_of_enrollment",
            "forms": [
                {
                    "form_name": "DEMOGRAPHY",
                    "form_id": "DEMOGRAPHY",
                    "data": [
                        {
                            "form_field_id": "DM_SEX",
                            "SASLabel": "Sex",
                            "cde_id": 6343385,
                            "value": "Male",
                        },
                        {
                            "form_field_id": "SC_SCORRES_CNTRYRES",
                            "SASLabel": "Country of Residence",
                            "cde_id": 2006183,
                            "value": "USA",
                        },
                    ],
                },
                {
                    "form_name": "COG_UPR_DX",
                    "form_id": "COG_UPR_DX",
                    "data": [
                        {
                            "form_field_id": "ADM_DX_CD_SEQ",
                            "SASLabel": "Diagnosis ID",
                            "cde_id": 2413226,
                            "value": "ABC123",
                        },
                        {
                            "form_field_id": "PRM_TU_DX_TXT",
                            "SASLabel": "Is this the diagnosis for which you are enrolling the patient?",
                            "cde_id": 2006870,
                            "value": "Yes",
                        },
                        {
                            "form_field_id": "TOPO_ICDO",
                            "SASLabel": "Primary site (TOPOGRAPHY) ICD-O CODE",
                            "cde_id": 3226281,
                            "value": "ABC123",
                        },
                        {
                            "form_field_id": "TOPO_TEXT",
                            "SASLabel": "Primary site (TOPOGRAPHY) ICD-O TERM",
                            "cde_id": 456692,
                            "value": "Blood",
                        },
                        {
                            "form_field_id": "MORPHO_ICDO",
                            "SASLabel": "Initial Diagnosis (MORPHOLOGY) ICD-O CODE",
                            "cde_id": 3226275,
                            "value": "9836/3",
                        },
                        {
                            "form_field_id": "MORPHO_TEXT",
                            "SASLabel": "Initial Diagnosis (MORPHOLOGY) ICD-O TERM",
                            "cde_id": 4567017,
                            "value": "Precursor B-cell lymphoblastic leukemia Pro-B ALL Common pre",
                        },
                        {
                            "form_field_id": "REG_STAGE_CODE_TEXT",
                            "SASLabel": "Registry Stage Code",
                            "cde_id": 4567051,
                            "value": "Not Applicable",
                        },
                    ],
                },
            ],
        },
        {
            "upi": "SUBJ2",
            "version": "1.0.3.0",
            "index_date_type": "date_of_enrollment",
            "forms": [
                {
                    "form_name": "DEMOGRAPHY",
                    "form_id": "DEMOGRAPHY",
                    "data": [
                        {
                            "form_field_id": "DM_SEX",
                            "SASLabel": "Sex",
                            "cde_id": 6343385,
                            "value": "Female",
                        },
                        {
                            "form_field_id": "SC_SCORRES_CNTRYRES",
                            "SASLabel": "Country of Residence",
                            "cde_id": 2006183,
                            "value": "Canada",
                        },
                    ],
                },
                {
                    "form_name": "COG_UPR_DX",
                    "form_id": "COG_UPR_DX",
                    "data": [
                        {
                            "form_field_id": "ADM_DX_CD_SEQ",
                            "SASLabel": "Diagnosis ID",
                            "cde_id": 2413226,
                            "value": "ABC123",
                        },
                        {
                            "form_field_id": "PRM_TU_DX_TXT",
                            "SASLabel": "Is this the diagnosis for which you are enrolling the patient?",
                            "cde_id": 2006870,
                            "value": "Yes",
                        },
                        {
                            "form_field_id": "TOPO_ICDO",
                            "SASLabel": "Primary site (TOPOGRAPHY) ICD-O CODE",
                            "cde_id": 3226281,
                            "value": "ABC123",
                        },
                        {
                            "form_field_id": "TOPO_TEXT",
                            "SASLabel": "Primary site (TOPOGRAPHY) ICD-O TERM",
                            "cde_id": 456692,
                            "value": "Spinal cord",
                        },
                        {
                            "form_field_id": "MORPHO_ICDO",
                            "SASLabel": "Initial Diagnosis (MORPHOLOGY) ICD-O CODE",
                            "cde_id": 3226275,
                            "value": "9538/1",
                        },
                        {
                            "form_field_id": "MORPHO_TEXT",
                            "SASLabel": "Initial Diagnosis (MORPHOLOGY) ICD-O TERM",
                            "cde_id": 4567017,
                            "value": "Clear cell meningioma",
                        },
                        {
                            "form_field_id": "REG_STAGE_CODE_TEXT",
                            "SASLabel": "Registry Stage Code",
                            "cde_id": 4567051,
                            "value": "Local",
                        },
                    ],
                },
            ],
        },
    ]
    mock_df = pd.json_normalize(mock_json)

    observed_table, observed_sas_labels = expand_cog_df(mock_df)

    #expected tables formatted
    expected_table = pd.DataFrame(
        [
            [
                "SUBJ1",
                "date_of_enrollment",
                "Male",
                "USA",
                "ABC123",
                "Yes",
                "ABC123",
                "Blood",
                "9836/3",
                "Precursor B-cell lymphoblastic leukemia Pro-B ALL Common pre",
                "Not Applicable",
            ],
            [
                "SUBJ2",
                "date_of_enrollment",
                "Female",
                "Canada",
                "ABC123",
                "Yes",
                "ABC123",
                "Spinal cord",
                "9538/1",
                "Clear cell meningioma",
                "Local",
            ],
        ],
        columns=[
            "upi",
            "index_date_type",
            "DEMOGRAPHY.DM_SEX",
            "DEMOGRAPHY.SC_SCORRES_CNTRYRES",
            "COG_UPR_DX.ADM_DX_CD_SEQ",
            "COG_UPR_DX.PRM_TU_DX_TXT",
            "COG_UPR_DX.TOPO_ICDO",
            "COG_UPR_DX.TOPO_TEXT",
            "COG_UPR_DX.MORPHO_ICDO",
            "COG_UPR_DX.MORPHO_TEXT",
            "COG_UPR_DX.REG_STAGE_CODE_TEXT",
        ],
    )

    expected_sas_labels = pd.DataFrame(
        [
            ["DEMOGRAPHY.DM_SEX", "Sex", 6343385],
            ["DEMOGRAPHY.SC_SCORRES_CNTRYRES", "Country of Residence", 2006183],
            ["COG_UPR_DX.ADM_DX_CD_SEQ", "Diagnosis ID", 2413226],
            [
                "COG_UPR_DX.PRM_TU_DX_TXT",
                "Is this the diagnosis for which you are enrolling the patient?",
                2006870,
            ],
            ["COG_UPR_DX.TOPO_ICDO", "Primary site (TOPOGRAPHY) ICD-O CODE", 3226281],
            ["COG_UPR_DX.TOPO_TEXT", "Primary site (TOPOGRAPHY) ICD-O TERM", 456692],
            ["COG_UPR_DX.MORPHO_ICDO", "Initial Diagnosis (MORPHOLOGY) ICD-O CODE", 3226275],
            ["COG_UPR_DX.MORPHO_TEXT", "Initial Diagnosis (MORPHOLOGY) ICD-O TERM", 4567017],
            ["COG_UPR_DX.REG_STAGE_CODE_TEXT", "Registry Stage Code", 4567051],
        ],
        columns=["column_name", "SASLabel", "cde_id"],
    )
    expected_sas_labels.cde_id = expected_sas_labels.cde_id.astype(str)

    assert expected_sas_labels.equals(observed_sas_labels)
    assert expected_table.equals(observed_table)

