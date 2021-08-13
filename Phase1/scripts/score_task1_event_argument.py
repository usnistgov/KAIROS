#!/usr/bin/env python3

__author__ = "Xiongnan Jin (xiongnan.jin@nist.gov)"
__version__ = "Development: 0.5.2"
__date__ = "02/22/2021"

# This software was developed by employees of the National Institute of
# Standards and Technology (NIST), an agency of the Federal
# Government. Pursuant to title 17 United States Code Section 105, works
# of NIST employees are not subject to copyright protection in the
# United States and are considered to be in the public
# domain. Permission to freely use, copy, modify, and distribute this
# software and its documentation without fee is hereby granted, provided
# that this notice and disclaimer of warranty appears in all copies.

# THE SOFTWARE IS PROVIDED 'AS IS' WITHOUT ANY WARRANTY OF ANY KIND,
# EITHER EXPRESSED, IMPLIED, OR STATUTORY, INCLUDING, BUT NOT LIMITED
# TO, ANY WARRANTY THAT THE SOFTWARE WILL CONFORM TO SPECIFICATIONS, ANY
# IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
# PURPOSE, AND FREEDOM FROM INFRINGEMENT, AND ANY WARRANTY THAT THE
# DOCUMENTATION WILL CONFORM TO THE SOFTWARE, OR ANY WARRANTY THAT THE
# SOFTWARE WILL BE ERROR FREE. IN NO EVENT SHALL NIST BE LIABLE FOR ANY
# DAMAGES, INCLUDING, BUT NOT LIMITED TO, DIRECT, INDIRECT, SPECIAL OR
# CONSEQUENTIAL DAMAGES, ARISING OUT OF, RESULTING FROM, OR IN ANY WAY
# CONNECTED WITH THIS SOFTWARE, WHETHER OR NOT BASED UPON WARRANTY,
# CONTRACT, TORT, OR OTHERWISE, WHETHER OR NOT INJURY WAS SUSTAINED BY
# PERSONS OR PROPERTY OR OTHERWISE, AND WHETHER OR NOT LOSS WAS
# SUSTAINED FROM, OR AROSE OUT OF THE RESULTS OF, OR USE OF, THE
# SOFTWARE OR SERVICES PROVIDED HEREUNDER.

# Distributions of NIST software should also include copyright and
# licensing statements of any third-party software that are legally
# bundled with the code in compliance with the conditions of those
# licenses.

######################################################################################
# extract ep and arg information from TA2 output JSON files and save as csv files
# assign LDC assessment to the extracted ep and arg csv files
# read assessed ep and arg csv files and compute precision, recall, and F1 score
######################################################################################
import configparser
import glob
import math
import sys
import time
from typing import Tuple

import pandas as pd
import os
import logging
import argparse
from tqdm.auto import tqdm
from scripts import extract_event_argument

scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../scripts")
sys.path.append(scripts_path)


def select_event_and_add_reference_columns(ev_arg_df: pd.DataFrame) -> pd.DataFrame:
    ev_arg_df.insert(18, 'ev_without_arg', 'no')
    for i, ev_arg_row in ev_arg_df.iterrows():
        arg_id = ev_arg_row.get('arg_id')
        if arg_id == 'empty':
            ev_arg_df.at[i, 'ev_without_arg'] = 'yes'

    ev_df = ev_arg_df.loc[:, 'file_name':'ev_without_arg']
    ev_df = ev_df.drop_duplicates(ignore_index=True)
    # add assessment column
    ev_df.insert(len(ev_df.columns), 'ev_reference_id', 'empty')
    ev_df.insert(len(ev_df.columns), 'ev_match_status', 'empty')
    ev_df.insert(len(ev_df.columns), 'ev_assessor_id', 0)

    return ev_df


def add_mapping_ke_id(ke_df: pd.DataFrame, ke_mapping_df: pd.DataFrame, ke: str,
                      assessed_ev_df: pd.DataFrame, assessed_rel_df: pd.DataFrame) -> Tuple[pd.DataFrame, bool]:
    exist_mapping = False
    ke_file_name = ke_df.iloc[0]['file_name']
    ke_file_name = ke_file_name + '.tab'
    file_ke_mapping_df = ke_mapping_df.loc[ke_mapping_df['filename'] == ke_file_name]

    ke_df.insert(len(ke_df.columns), ke + '_mapping_id', 'empty')
    if ke == 'arg':
        ke_df.insert(len(ke_df.columns), ke + '_remark', 'empty')

    if len(file_ke_mapping_df) > 0:
        for i, ke_row in ke_df.iterrows():
            # get ke_id
            ke_id = ke_row.get(ke + '_id')
            if ke_id == 'empty':
                continue
            # get ke_provenance and do cleaning
            if ke in ['ev', 'rel', 'ent']:
                ke_provenance = ke_row.get(ke + '_provenance')
            elif ke == 'arg':
                ke_provenance = ke_row.get('val_provenance')
            else:
                sys.exit('KE error')

            if not isinstance(ke_provenance, str):
                continue
            ke_provenance = ke_provenance.replace('[', '')
            ke_provenance = ke_provenance.replace(']', '')
            ke_provenance = ke_provenance.replace('\'', '')

            ev_seqnum = arg_role = rel_seqnum = None
            # if ke is event, get the first event provenance
            if ke == 'ev':
                if ',' in ke_provenance:
                    ke_provenance = ke_provenance.split(',')[0]
            elif ke == 'arg':
                ev_seqnum = get_ke_seqnum(ke_row, assessed_ev_df, 'ev')
                arg_role_full = ke_row.get('arg_role')
                arg_role_slash_list = arg_role_full.split('/')
                arg_role = arg_role_slash_list[len(arg_role_slash_list) - 1]
            elif ke == 'ent':
                rel_seqnum = get_ke_seqnum(ke_row, assessed_rel_df, 'rel')

            schema_id = ke_row.get('schema_id')
            schema_super = ke_row.get('schema_super')

            # compute mapping_df
            if ke == 'ev':
                mapping_df = file_ke_mapping_df.loc[
                    (file_ke_mapping_df['NIST eID'] == ke_id) &
                    (file_ke_mapping_df['uid'] == ke_provenance) &
                    (file_ke_mapping_df['instantiates'] == schema_super)
                    ]
            elif ke == 'arg':
                mapping_df = file_ke_mapping_df.loc[
                    (file_ke_mapping_df['instantiates'] == schema_super) &
                    (file_ke_mapping_df['ep_seqnum'] == ev_seqnum) &
                    (file_ke_mapping_df['arglabel'] == arg_role) &
                    (file_ke_mapping_df['prov_id'] == ke_provenance)
                    ]
            elif ke == 'rel':
                mapping_df = file_ke_mapping_df.loc[
                    (file_ke_mapping_df['NIST rID'] == ke_id) &
                    (file_ke_mapping_df['uid'] == ke_provenance) &
                    (file_ke_mapping_df['instantiates'] == schema_super)
                    ]
            elif ke == 'ent':
                mapping_df = file_ke_mapping_df.loc[
                    (file_ke_mapping_df['instantiates'] == schema_super) &
                    (file_ke_mapping_df['ep_seqnum'] == rel_seqnum) &
                    (file_ke_mapping_df['prov_id'] == ke_provenance)
                    ]
            else:
                sys.exit('KE error!')
            # if there is no mapping, exit with error message
            if len(mapping_df) == 0:
                sys.exit('No mapping LDC id for an SDF id: ' +
                         ke_file_name[:-4] + ';' + schema_super + ';' + ke_id)
            # if there is multiple mapping, exit with error message
            elif len(mapping_df) > 1:
                if ke == 'arg':
                    exist_mapping = True
                    ke_df.at[i, 'arg_remark'] = 'multiple_mappings'
                    ke_mapping_id = []
                    for j, mapping_row in mapping_df.iterrows():
                        mapping_id = mapping_row.get('system_ke_id')
                        ke_mapping_id.append(mapping_id)
                    # ke_df.at[i, ke + '_mapping_id'] = ke_mapping_id
                    ke_df.at[i, ke + '_mapping_id'] = ke_mapping_id[0]
                else:
                    sys.exit('Multiple mapping LDC ids for an SDF id: ' +
                             ke_file_name[:-4] + ';' + schema_super + ';' + ke_id)
            else:
                exist_mapping = True
                # get ke_mapping_id
                if ke in ['ev', 'rel']:
                    ke_mapping_id = mapping_df.iloc[0]['system_ep_id']
                elif ke in ['arg', 'ent']:
                    ke_mapping_id = mapping_df.iloc[0]['system_ke_id']
                else:
                    sys.exit('KE error!')

                ke_df.at[i, ke + '_mapping_id'] = ke_mapping_id

    return ke_df, exist_mapping


def transform_ke_mapping_id(ke_mapping_id: str) -> str:
    # transform from the format ISI_RES_1010_0001_SEP00001_SKE00001 to ISI_RES_1010_0001_SKE00001
    underbar_list = ke_mapping_id.split('_')
    underbar_list.pop(4)
    res = '_'.join(underbar_list)

    return res


def get_ke_reference_id_from_correctness(correctness_df: pd.DataFrame, ke_mapping_id: str, ke: str) -> str:
    if ke in ['ev', 'rel']:
        matching_df = correctness_df.loc[
            correctness_df['system_ep_id'] == ke_mapping_id
            ]
    elif ke in ['arg', 'ent']:
        matching_df = correctness_df.loc[
            correctness_df['system_ke_id'] == ke_mapping_id
            ]
    else:
        sys.exit('KE error!')

    if len(matching_df) == 0:
        return 'empty'
    elif len(matching_df) > 1:
        sys.exit('Multiple mapping correctness: ' + ke_mapping_id)
    else:
        correctness = matching_df.iloc[0]['correctness']
        if correctness == 'no':
            return 'no'
        elif correctness == 'yes':
            return ke_mapping_id
        else:
            return 'empty'


def get_mm_arg_reference_id_assessor_id_match_status(ke_mapping_id: list, file_ke_alignment_df: pd.DataFrame,
                                                     correctness_df: pd.DataFrame, ke: str) -> list:
    mm_reference_id_set = set()
    mm_assessor_id_set = set()
    mm_match_status_set = set()
    for mapping_id in ke_mapping_id:
        mapping_id = transform_ke_mapping_id(mapping_id)
        reference_df = file_ke_alignment_df.loc[file_ke_alignment_df['system_ke_id'] == mapping_id]
        if len(reference_df) > 0:
            ke_reference_id = reference_df.iloc[0]['reference_ke_id']
            ke_match_status = reference_df.iloc[0]['ke_match_status']

            if (ke_match_status != 'empty') and (not isinstance(ke_reference_id, str)):
                ke_reference_id = 'not_matched'
            elif ke_match_status == 'empty':
                ke_reference_id = 'empty'
            elif ke_match_status == 'extra-relevant':
                ke_reference_id = get_ke_reference_id_from_correctness(correctness_df, mapping_id, ke)

            # get and process ke_assessor_id
            ke_assessor_id = reference_df.iloc[0]['user_id']
            if not isinstance(ke_assessor_id, str):
                ke_assessor_id = '0'
            elif math.isnan(ke_assessor_id):
                ke_assessor_id = '0'

            mm_reference_id_set.add(ke_reference_id)
            mm_assessor_id_set.add(ke_assessor_id)
            mm_match_status_set.add(ke_match_status)

    return [mm_reference_id_set, mm_assessor_id_set, mm_match_status_set]


def add_ke_assessment_from_alignment_and_correctness(ke_df: pd.DataFrame, ke_alignment_df: pd.DataFrame,
                                                     correctness_df: pd.DataFrame, ke: str) -> pd.DataFrame:
    ke_file_name = ke_df.iloc[0]['file_name']
    ke_file_name = ke_file_name + '.tab'
    file_ke_alignment_df = ke_alignment_df.loc[ke_alignment_df['arf_filename'] == ke_file_name]

    if len(file_ke_alignment_df) == 0:
        return ke_df

    for i, ke_row in ke_df.iterrows():
        ke_id = ke_row.get(ke + '_id')
        # get ke_mapping_id
        ke_mapping_id = ke_row.get(ke + '_mapping_id')

        if isinstance(ke_mapping_id, str) and ke_mapping_id != 'empty':
            # compute reference_df
            if ke in ['ev', 'rel']:
                reference_df = file_ke_alignment_df.loc[file_ke_alignment_df['system_ep_id'] == ke_mapping_id]
            elif ke in ['arg', 'ent']:
                # transform from the format ISI_RES_1010_0001_SEP00001_SKE00001 to ISI_RES_1010_0001_SKE00001
                ke_mapping_id = transform_ke_mapping_id(ke_mapping_id)
                reference_df = file_ke_alignment_df.loc[file_ke_alignment_df['system_ke_id'] == ke_mapping_id]
            else:
                sys.exit('KE error!')

            if len(reference_df) > 0:
                # get ke_reference_id and ke_match_status
                if ke in ['ev', 'rel']:
                    ke_reference_id = reference_df.iloc[0]['reference_ep_id']
                    ke_match_status = reference_df.iloc[0]['ep_match_status']
                elif ke in ['arg', 'ent']:
                    ke_reference_id = reference_df.iloc[0]['reference_ke_id']
                    ke_match_status = reference_df.iloc[0]['ke_match_status']
                else:
                    sys.exit('KE error!')

                if (ke_match_status != 'empty') and (not isinstance(ke_reference_id, str)):
                    ke_reference_id = 'not_matched'
                elif ke_match_status == 'empty':
                    ke_reference_id = 'empty'
                if ke_match_status == 'extra-relevant':
                    ke_reference_id = get_ke_reference_id_from_correctness(correctness_df, ke_mapping_id, ke)
                # get and process ke_assessor_id
                ke_assessor_id = reference_df.iloc[0]['user_id']
                if isinstance(ke_assessor_id, str):
                    ke_assessor_id = '0'
                elif math.isnan(ke_assessor_id):
                    ke_assessor_id = '0'
                # update ke_reference_id, ke_assessor_id, ke_match_status
                ke_df.at[i, ke + '_reference_id'] = ke_reference_id
                ke_df.at[i, ke + '_assessor_id'] = int(ke_assessor_id)
                ke_df.at[i, ke + '_match_status'] = ke_match_status
        # if ke is arg and ke_mapping_id is a list of multiple mapping ids
        elif isinstance(ke_mapping_id, list):
            arg_mm_list = get_mm_arg_reference_id_assessor_id_match_status(ke_mapping_id, file_ke_alignment_df,
                                                                           correctness_df, ke)
            mm_reference_id_set = arg_mm_list[0]
            mm_assessor_id_set = arg_mm_list[1]
            mm_match_status_set = arg_mm_list[2]
            if len(mm_reference_id_set) > 1:
                sys.exit('Multi-mapping argument has different alignment results: ' +
                         ke_id + '; ' + str(ke_mapping_id))
            elif len(mm_reference_id_set) == 1:
                ke_reference_id = mm_reference_id_set.pop()
                ke_assessor_id = mm_assessor_id_set.pop()
                ke_match_status = mm_match_status_set.pop()
                # update ke_reference_id, ke_assessor_id, ke_match_status
                ke_df.at[i, ke + '_reference_id'] = ke_reference_id
                ke_df.at[i, ke + '_assessor_id'] = int(ke_assessor_id)
                ke_df.at[i, ke + '_match_status'] = ke_match_status

    return ke_df


def assign_ke_assessment_to_single_file(extracted_ke_df: pd.DataFrame, ke_alignment_df: pd.DataFrame,
                                        correctness_df: pd.DataFrame, ke_mapping_df: pd.DataFrame,
                                        assessed_ev_df: pd.DataFrame, assessed_rel_df: pd.DataFrame,
                                        ke: str) -> pd.DataFrame:
    if ke == 'ev':
        extracted_ke_df = select_event_and_add_reference_columns(extracted_ke_df)
    elif ke in ['arg', 'rel', 'ent']:
        extracted_ke_df = add_reference_columns(extracted_ke_df, ke)
    else:
        sys.exit('KE error!')

    mapped_ke_df, exist_mapping = add_mapping_ke_id(extracted_ke_df, ke_mapping_df, ke, assessed_ev_df, assessed_rel_df)

    if exist_mapping:
        assessed_ke_df = add_ke_assessment_from_alignment_and_correctness(mapped_ke_df, ke_alignment_df,
                                                                          correctness_df, ke)
        return assessed_ke_df
    else:
        return mapped_ke_df


def read_ke_alignment(assessment_directory: str, ke: str) -> pd.DataFrame:
    if ke in ['ev', 'rel']:
        ke_alignment_fn = assessment_directory + 'data/KAIROS_EVAL_ep_alignment.tab'
    elif ke in ['arg', 'ent']:
        ke_alignment_fn = assessment_directory + 'data/KAIROS_EVAL_ke_analysis.tab'
    else:
        sys.exit('KE error!')

    if not os.path.isfile(ke_alignment_fn):
        sys.exit('File not found: ' + ke_alignment_fn)

    ke_alignment_df = pd.read_table(ke_alignment_fn)

    return ke_alignment_df


def read_correctness(assessment_directory: str) -> pd.DataFrame:
    ke_correctness_fn = assessment_directory + 'data/KAIROS_EVAL_correctness.tab'

    if not os.path.isfile(ke_correctness_fn):
        sys.exit('File not found: ' + ke_correctness_fn)

    ke_correctness_df = pd.read_table(ke_correctness_fn)

    return ke_correctness_df


def replace_context(input_df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    # replace context with institute name to match input_id in TA2 task1 outputs
    string_replace_dict = dict(zip(["^https://www.ibm.com/CHRONOS/", "^https://www.cmu.edu/",
                                    "^https://isi.edu/kairos/", "^https://isi.edu/kairos",
                                    "^https://caci.com/kairos/", "^https://blender.cs.illinois.edu/kairos/"],
                                   ["ibm:", "cmu:", "isi:", "isi:", "caci:", "resin:"]))
    input_df[col_name] = input_df[col_name].replace(string_replace_dict, regex=True)
    return input_df


def read_ke_mapping(assessment_directory: str, caci_directory: str, ke: str) -> pd.DataFrame:
    if ke == 'ev':
        ldc_ke_mapping_fn = assessment_directory + 'docs/system_ep_id.events_v4.tab'
    elif ke == 'arg':
        ldc_ke_mapping_fn = assessment_directory + 'docs/system_ke_id.events_v4.tab'
    elif ke == 'rel':
        ldc_ke_mapping_fn = assessment_directory + 'docs/system_ep_id.relations_v4.tab'
    elif ke == 'ent':
        ldc_ke_mapping_fn = assessment_directory + 'docs/system_ke_id.relations_v4.tab'
    else:
        sys.exit('KE error!')

    if not os.path.isfile(ldc_ke_mapping_fn):
        sys.exit('File not found: ' + ldc_ke_mapping_fn)

    ldc_ke_mapping_df = pd.read_table(ldc_ke_mapping_fn)

    if ke in ['arg', 'ent']:
        return ldc_ke_mapping_df

    if ke == 'ev':
        caci_ke_mapping_fn = caci_directory + 'nisteIDs.tab'
    elif ke == 'rel':
        caci_ke_mapping_fn = caci_directory + 'nistrIDs.tab'
    else:
        sys.exit('KE error!')

    if not os.path.isfile(caci_ke_mapping_fn):
        sys.exit('File not found: ' + caci_ke_mapping_fn)

    caci_ke_mapping_df = pd.read_table(caci_ke_mapping_fn)
    augmented_ke_mapping_df = ldc_ke_mapping_df.join(caci_ke_mapping_df.set_index('system_ep_id'), on='system_ep_id')

    return augmented_ke_mapping_df


def assign_ke_assessment(ta2_team_name: str, task1_score_directory: str, assessment_directory: str,
                         caci_directory: str, ke: str, target_pairs: list) -> None:
    # get extracted_ke_directory
    assessed_ev_directory = assessed_rel_directory = ''
    if ke == 'ev':
        extracted_ke_directory = task1_score_directory + ta2_team_name + '/Event_arguments/'
        assessed_ke_directory = task1_score_directory + ta2_team_name + '/Assessed_events/'
    elif ke == 'arg':
        extracted_ke_directory = task1_score_directory + ta2_team_name + '/Event_arguments/'
        assessed_ke_directory = task1_score_directory + ta2_team_name + '/Assessed_arguments/'
        assessed_ev_directory = task1_score_directory + ta2_team_name + '/Assessed_events/'
        if not os.path.isdir(assessed_ev_directory):
            sys.exit('Directory not found: ' + assessed_ev_directory)
    elif ke == 'rel':
        extracted_ke_directory = task1_score_directory + ta2_team_name + '/Relations/'
        assessed_ke_directory = task1_score_directory + ta2_team_name + '/Assessed_relations/'
    elif ke == 'ent':
        extracted_ke_directory = task1_score_directory + ta2_team_name + '/Relation_entities/'
        assessed_ke_directory = task1_score_directory + ta2_team_name + '/Assessed_relation_entities/'
        assessed_rel_directory = task1_score_directory + ta2_team_name + '/Assessed_relations/'
        if not os.path.isdir(assessed_rel_directory):
            sys.exit('Directory not found: ' + assessed_rel_directory)
    else:
        sys.exit('KE error!')

    if not os.path.isdir(extracted_ke_directory):
        sys.exit('Directory not found: ' + extracted_ke_directory)
    # if assessed_ke_directory not empty clear contents; if not exist create one
    if not os.path.isdir(assessed_ke_directory):
        os.makedirs(assessed_ke_directory)
    else:
        files = glob.glob(assessed_ke_directory + '*')
        for f in files:
            os.remove(f)

    # read LDC alignment table
    print('Loading ' + ke.upper() + ' assessment and mapping files...')
    ke_alignment_df = read_ke_alignment(assessment_directory, ke)
    correctness_df = read_correctness(assessment_directory)

    # compute ke id mapping table
    ke_mapping_df = read_ke_mapping(assessment_directory, caci_directory, ke)
    ke_mapping_df['instantiates'] = ke_mapping_df['instantiates'].str.rstrip()
    ke_mapping_df = replace_context(ke_mapping_df, 'instantiates')
    if ke == 'ev':
        ke_mapping_df = replace_context(ke_mapping_df, 'NIST eID')
    elif ke in ['arg', 'ent']:
        ke_mapping_df['prov_id'] = ke_mapping_df['prov_id'].str.rstrip()
        ke_mapping_df = ke_mapping_df.loc[~ke_mapping_df['prov_id'].isnull()]
        ke_mapping_df = replace_context(ke_mapping_df, 'prov_id')
    elif ke == 'rel':
        ke_mapping_df = replace_context(ke_mapping_df, 'NIST rID')
    else:
        sys.exit('KE error!')

    print('Start to assign ' + ke.upper() + ' assessments to TA2_' + ta2_team_name + ' Task1 csv outputs...')
    for file_name in tqdm(os.listdir(extracted_ke_directory), position=0, leave=False):
        # only consider .csv file (ignoring system hidden files like .DS_store)
        fn_surfix = file_name[-4:]
        if fn_surfix != '.csv':
            continue
        # if the output is not for task1, skip this file
        if 'task1' not in file_name:
            continue
        # skip non-target TA1-TA2 pairs for Task1
        if target_pairs:
            fn_hyphen_list = file_name.split('-')
            ta1 = fn_hyphen_list[0].lower()
            ta2 = fn_hyphen_list[1].lower()
            key = ta1 + '-' + ta2
            if key not in target_pairs:
                continue
        # do the assessment and save back to the csv file
        extracted_ke_df = pd.read_csv(extracted_ke_directory + file_name)
        assessed_ev_df = assessed_rel_df = pd.DataFrame()
        if ke == 'arg':
            assessed_ev_fn = assessed_ev_directory + file_name
            if not os.path.isfile(assessed_ev_fn):
                sys.exit('File not found: ' + assessed_ev_fn)
            assessed_ev_df = pd.read_csv(assessed_ev_directory + file_name)
        if ke == 'ent':
            assessed_rel_fn = assessed_rel_directory + file_name
            if not os.path.isfile(assessed_rel_fn):
                sys.exit('File not found: ' + assessed_rel_fn)
            assessed_rel_df = pd.read_csv(assessed_rel_directory + file_name)

        assessed_ke_df = assign_ke_assessment_to_single_file(extracted_ke_df, ke_alignment_df, correctness_df,
                                                             ke_mapping_df, assessed_ev_df, assessed_rel_df, ke)

        assessed_ke_df.to_csv(assessed_ke_directory + file_name, index=False)


def get_ke_seqnum(ke_row: pd.Series, assessed_ke_df: pd.DataFrame, ke: str) -> int:
    ke_id = ke_row.get(ke + '_id')
    schema_id = ke_row.get('schema_id')
    ke_mapping_df = assessed_ke_df.loc[
        (assessed_ke_df['schema_id'] == schema_id) &
        (assessed_ke_df[ke + '_id'] == ke_id)
        ]

    ke_mapping_id = ''
    if len(ke_mapping_df) == 0:
        sys.exit('No mapping system_ep_id for SDF ev_id: ' + schema_id + '/' + ke_id)
    elif len(ke_mapping_df) > 1:
        sys.exit('Multiple mapping system_ep_id for SDF ev_id: ' + schema_id + '/' + ke_id)
    elif len(ke_mapping_df) == 1:
        ke_mapping_id = ke_mapping_df.iloc[0][ke + '_mapping_id']

    ke_underbar_list = ke_mapping_id.split('_')
    last_element = ke_underbar_list[len(ke_underbar_list) - 1]
    last_element = last_element[3:]
    ke_seqnum = int(last_element)
    if ke == 'rel':
        ke_seqnum = ke_seqnum % 10000

    return ke_seqnum


def add_reference_columns(input_df: pd.DataFrame, ke: str) -> pd.DataFrame:
    input_df.insert(len(input_df.columns), ke + '_reference_id', 'empty')
    input_df.insert(len(input_df.columns), ke + '_match_status', 'empty')
    input_df.insert(len(input_df.columns), ke + '_assessor_id', 0)

    return input_df


def compute_true_ke_count(assessed_ke_count: int, assessed_ke_df: pd.DataFrame, ke: str) -> list:
    if assessed_ke_count > 0:
        true_ke_reference_list = []
        distinct_true_ke_reference_set = set()
        extra_relevant_ke_list = []
        true_assessed_ke_df = assessed_ke_df.loc[~assessed_ke_df[ke + '_reference_id'].isin(
            ['not_matched', 'empty', 'no'])]
        for i, row in true_assessed_ke_df.iterrows():
            # get ke_reference_id
            ke_reference_id: str = row.get(ke + '_reference_id')
            # compute true_ke_reference_list and distinct_true_ke_reference_set
            if (ke == 'ev' and ke_reference_id.startswith('VP')) or \
                    (ke in ['arg', 'ent'] and (ke_reference_id.startswith('AR') or ke_reference_id.startswith('VP'))) or \
                    (ke == 'rel' and ke_reference_id.startswith('RR')):
                true_ke_reference_list.append(ke_reference_id)
                distinct_true_ke_reference_set.add(ke_reference_id)
            else:
                extra_relevant_ke_list.append(ke_reference_id)

        true_ke_count = len(true_ke_reference_list)
        distinct_true_ke_count = len(distinct_true_ke_reference_set)
        extra_relevant_ke_count = len(extra_relevant_ke_list)
    else:
        true_ke_count = 0
        distinct_true_ke_count = 0
        extra_relevant_ke_count = 0

    return [true_ke_count, distinct_true_ke_count, extra_relevant_ke_count]


def compute_match_ke_count(assessed_ke_count: int, assessed_ke_df: pd.DataFrame, ke: str) \
        -> Tuple[int, int]:
    if assessed_ke_count > 0:
        match_reference_ser = assessed_ke_df[ke + '_match_status'].loc[assessed_ke_df[ke + '_match_status'] == "match"]
        match_count = len(match_reference_ser)
        match_ie_reference_ser = assessed_ke_df[ke + '_match_status'].loc[
            assessed_ke_df[ke + '_match_status'] == "match-inexact"]
        match_ie_count = len(match_ie_reference_ser)
    else:
        match_count = 0
        match_ie_count = 0

    return match_count, match_ie_count


def compute_extra_relevant_ke_count(assessed_ke_count: int, assessed_ke_df: pd.DataFrame, ke: str) \
        -> Tuple[int, int, int]:
    if assessed_ke_count > 0:
        er_y_reference_ser = assessed_ke_df[ke + '_match_status'].loc[
            (assessed_ke_df[ke + '_match_status'] == "extra-relevant") &
            (~assessed_ke_df[ke + '_reference_id'].isin(["empty", "no"]))]
        er_y_count = len(er_y_reference_ser)
        er_n_reference_ser = assessed_ke_df[ke + '_match_status'].loc[
            (assessed_ke_df[ke + '_match_status'] == "extra-relevant") &
            (assessed_ke_df[ke + '_reference_id'].isin(["no"]))]
        er_n_count = len(er_n_reference_ser)
        er_e_reference_ser = assessed_ke_df[ke + '_match_status'].loc[
            (assessed_ke_df[ke + '_match_status'] == "extra-relevant") &
            (assessed_ke_df[ke + '_reference_id'].isin(["empty"]))]
        er_e_count = len(er_e_reference_ser)
    else:
        er_y_count = 0
        er_n_count = 0
        er_e_count = 0

    return er_y_count, er_n_count, er_e_count


def compute_extra_ke_count(assessed_ke_count: int, assessed_ke_df: pd.DataFrame, ke: str) \
        -> Tuple[int, int]:
    if assessed_ke_count > 0:
        nm_reference_ser = assessed_ke_df[ke + '_match_status'].loc[
            assessed_ke_df[ke + '_match_status'] == "not_matched"]
        nm_count = len(nm_reference_ser)
        extra_ir_reference_ser = assessed_ke_df[ke + '_match_status'].loc[
            assessed_ke_df[ke + '_match_status'] == "extra-irrelevant"]
        extra_ir_count = len(extra_ir_reference_ser)
    else:
        nm_count = 0
        extra_ir_count = 0

    return nm_count, extra_ir_count


def compute_true_ke_count_at_top_k(assessed_ke_count: int, assessed_ke_df: pd.DataFrame, ke: str, k: int) -> list:
    if assessed_ke_count > 0:
        true_ke_at_top_k_reference_list = []
        distinct_true_ke_at_top_k_reference_set = set()
        extra_relevant_ke_at_top_k_list = []
        top_k_assessed_ke_df = assessed_ke_df.sort_values(by=ke + '_mapping_id').head(k)

        for i, row in top_k_assessed_ke_df.iterrows():
            # get ke_reference_id
            ke_reference_id = row.get(ke + '_reference_id')
            # compute true_ke_reference_list and distinct_true_ke_reference_set
            if (ke == 'ev' and ke_reference_id.startswith('VP')) or \
                    (ke in ['arg', 'ent'] and (ke_reference_id.startswith('AR') or ke_reference_id.startswith('VP'))) or \
                    (ke == 'rel' and ke_reference_id.startswith('RR')):
                true_ke_at_top_k_reference_list.append(ke_reference_id)
                distinct_true_ke_at_top_k_reference_set.add(ke_reference_id)
            elif ke_reference_id not in ['not_matched', 'no', 'empty']:
                extra_relevant_ke_at_top_k_list.append(ke_reference_id)

        true_ke_count_at_top_k = len(true_ke_at_top_k_reference_list)
        distinct_true_ke_count_at_top_k = len(distinct_true_ke_at_top_k_reference_set)
        extra_relevant_ke_count_at_top_k = len(extra_relevant_ke_at_top_k_list)
    else:
        true_ke_count_at_top_k = 0
        distinct_true_ke_count_at_top_k = 0
        extra_relevant_ke_count_at_top_k = 0

    return [true_ke_count_at_top_k, distinct_true_ke_count_at_top_k, extra_relevant_ke_count_at_top_k]


def compute_target_ann_ke_count(annotation_directory: str, target_ce: str, ke: str) -> int:
    if ke == 'ev':
        ann_ke_fn = annotation_directory + target_ce + '/' + target_ce + '_events.xlsx'
    elif ke in ['arg', 'ent']:
        ann_ke_fn = annotation_directory + target_ce + '/' + target_ce + '_arguments.xlsx'
    elif ke == 'rel':
        ann_ke_fn = annotation_directory + target_ce + '/' + target_ce + '_relations.xlsx'
    else:
        sys.exit('KE error!')

    if not os.path.isfile(ann_ke_fn):
        # print('File not found: ' + ann_ke_fn)
        return -1

    ann_ke_df = pd.read_excel(ann_ke_fn)
    # if ke is argument, select event arguments
    if ke == 'arg':
        ann_ke_df = ann_ke_df.loc[ann_ke_df['eventprimitive_id'] != 'EMPTY_NA']
    if ke == 'ent':
        ann_ke_df = ann_ke_df.loc[ann_ke_df['eventprimitive_id'] == 'EMPTY_NA']

    ann_ke_count = len(ann_ke_df)

    return ann_ke_count


def compute_single_ke_stats(input_ke_df: pd.DataFrame, input_ke_stats_df: pd.DataFrame, input_unit: str,
                            ann_ke_count: int, ke: str, k: int) -> pd.DataFrame:
    file_name = input_ke_df.iloc[0]['file_name']
    # get ta1 and target_ce
    fn_hyphen_list = file_name.split('-')
    ta1 = fn_hyphen_list[0]
    ta2 = fn_hyphen_list[1]
    target_ce = fn_hyphen_list[2]
    # compute all_ke_count
    if ke in ['ev', 'rel', 'ent']:
        all_ke_count = len(input_ke_df)
    # delete the empty arg created for events with no arguments
    elif ke == 'arg':
        all_ke_count = len(input_ke_df.loc[input_ke_df['arg_id'] != 'empty'])
    else:
        sys.exit('KE error!')
    # compute assessed_ke_df
    assessed_ke_df = input_ke_df.loc[input_ke_df[ke + '_match_status'] != 'empty']
    assessed_ke_count = len(assessed_ke_df)
    assessed_ke_woer_df = assessed_ke_df.loc[assessed_ke_df[ke + '_match_status'] != 'extra-relevant']
    assessed_ke_woer_count = len(assessed_ke_woer_df)
    # compute human_assessed_ke_df
    if assessed_ke_count > 0:
        human_assessed_ke_df = assessed_ke_df.loc[assessed_ke_df[ke + '_assessor_id'] != 0]
        human_assessed_ke_count = len(human_assessed_ke_df)
    else:
        human_assessed_ke_count = 0
    # compute true_ke_count, distinct_true_ke_count
    true_count_list = compute_true_ke_count(assessed_ke_count, assessed_ke_df, ke)
    true_ke_count = true_count_list[0]
    distinct_true_ke_count = true_count_list[1]
    extra_relevant_ke_count = true_count_list[2]
    true_count_at_top_k_list = compute_true_ke_count_at_top_k(assessed_ke_count, assessed_ke_df, ke, k)
    true_ke_count_at_top_k = true_count_at_top_k_list[0]
    distinct_true_ke_count_at_top_k = true_count_at_top_k_list[1]
    extra_relevant_ke_count_at_top_k = true_count_at_top_k_list[2]
    true_woer_count_at_top_k_list = compute_true_ke_count_at_top_k(assessed_ke_count, assessed_ke_woer_df, ke, k)
    true_ke_woer_count_at_top_k = true_woer_count_at_top_k_list[0]
    distinct_true_ke_woer_count_at_top_k = true_woer_count_at_top_k_list[1]
    match_ke_count, match_i_ke_count = compute_match_ke_count(assessed_ke_count, assessed_ke_df, ke)
    nm_ke_count, extra_ir_ke_count = compute_extra_ke_count(assessed_ke_count, assessed_ke_df, ke)
    er_y_ke_count, er_n_ke_count, er_e_ke_count = compute_extra_relevant_ke_count(assessed_ke_count, assessed_ke_df, ke)
    empty_ke_count = all_ke_count - assessed_ke_count

    if input_unit == 'file':
        stats_row = {'file_name': file_name, 'TA1': ta1, 'TA2': ta2, 'target_ce': target_ce,
                     'all_{0}_count'.format(ke): all_ke_count, 'assessed_{0}_count'.format(ke): assessed_ke_count,
                     'assessed_{0}_woer_count'.format(ke): assessed_ke_woer_count,
                     'human_assessed_{0}_count'.format(ke): human_assessed_ke_count,
                     'true_{0}_count'.format(ke): true_ke_count,
                     'distinct_true_{0}_count'.format(ke): distinct_true_ke_count,
                     'extra_relevant_{0}_count'.format(ke): extra_relevant_ke_count,
                     'true_{0}_count@{1}'.format(ke, str(k)): true_ke_count_at_top_k,
                     'distinct_true_{0}_count@{1}'.format(ke, str(k)): distinct_true_ke_count_at_top_k,
                     'extra_relevant_{0}_count@{1}'.format(ke, str(k)): extra_relevant_ke_count_at_top_k,
                     'true_{0}_woer_count@{1}'.format(ke, str(k)): true_ke_woer_count_at_top_k,
                     'distinct_true_{0}_woer_count@{1}'.format(ke, str(k)): distinct_true_ke_woer_count_at_top_k,
                     'ann_{0}_count'.format(ke): ann_ke_count,
                     'match_{0}_count'.format(ke): match_ke_count,
                     'match-inexact_{0}_count'.format(ke): match_i_ke_count,
                     'extra_relevant_{0}_yes_count'.format(ke): er_y_ke_count,
                     'extra_relevant_{0}_no_count'.format(ke): er_n_ke_count,
                     'extra_relevant_{0}_empty_count'.format(ke): er_e_ke_count,
                     'extra-irrelevant_{0}_count'.format(ke): extra_ir_ke_count,
                     'unassessed_{0}_count'.format(ke): empty_ke_count, 'non-match_{0}_count'.format(ke): nm_ke_count
                     }
    elif input_unit == 'schema':
        schema_id = input_ke_df.iloc[0]['schema_id']
        schema_super = input_ke_df.iloc[0]['schema_super']
        stats_row = {'file_name': file_name, 'schema_id': schema_id, 'schema_super': schema_super, 'TA1': ta1, 'TA2': ta2, 'target_ce': target_ce,
                     'all_{0}_count'.format(ke): all_ke_count, 'assessed_{0}_count'.format(ke): assessed_ke_count,
                     'assessed_{0}_woer_count'.format(ke): assessed_ke_woer_count,
                     'human_assessed_{0}_count'.format(ke): human_assessed_ke_count,
                     'true_{0}_count'.format(ke): true_ke_count,
                     'distinct_true_{0}_count'.format(ke): distinct_true_ke_count,
                     'extra_relevant_{0}_count'.format(ke): extra_relevant_ke_count,
                     'true_{0}_count@{1}'.format(ke, str(k)): true_ke_count_at_top_k,
                     'distinct_true_{0}_count@{1}'.format(ke, str(k)): distinct_true_ke_count_at_top_k,
                     'extra_relevant_{0}_count@{1}'.format(ke, str(k)): extra_relevant_ke_count_at_top_k,
                     'true_{0}_woer_count@{1}'.format(ke, str(k)): true_ke_woer_count_at_top_k,
                     'distinct_true_{0}_woer_count@{1}'.format(ke, str(k)): distinct_true_ke_woer_count_at_top_k,
                     'ann_{0}_count'.format(ke): ann_ke_count,
                     'match_{0}_count'.format(ke): match_ke_count,
                     'match-inexact_{0}_count'.format(ke): match_i_ke_count,
                     'extra_relevant_{0}_yes_count'.format(ke): er_y_ke_count,
                     'extra_relevant_{0}_no_count'.format(ke): er_n_ke_count,
                     'extra_relevant_{0}_empty_count'.format(ke): er_e_ke_count,
                     'extra-irrelevant_{0}_count'.format(ke): extra_ir_ke_count,
                     'unassessed_{0}_count'.format(ke): empty_ke_count, 'non-match_{0}_count'.format(ke): nm_ke_count
                     }
    else:
        sys.exit('Input unit error!')

    if ke == 'ev':
        empty_ev_count = len(input_ke_df.loc[input_ke_df['ev_without_arg'] == 'yes'])
        stats_row['empty_{0}_count'.format(ke)] = empty_ev_count
    if ke == 'arg':
        mm_ke_count = len(input_ke_df.loc[input_ke_df['arg_remark'] == 'multiple_mappings'])
        stats_row['mm_' + ke + '_count'] = mm_ke_count

    input_ke_stats_df = input_ke_stats_df.append(stats_row, ignore_index=True)

    return input_ke_stats_df


def compute_schema_ke_stats(file_ke_df: pd.DataFrame, schema_ke_stats_df: pd.DataFrame,
                            ann_ke_count: int, ke: str, k: int) -> pd.DataFrame:
    schema_list = file_ke_df.schema_id.unique()
    if len(schema_list) > 0:
        grouped = file_ke_df.groupby(file_ke_df.schema_id)
        for schema_id in schema_list:
            schema_ke_df = grouped.get_group(schema_id)
            schema_ke_stats_df = compute_single_ke_stats(schema_ke_df, schema_ke_stats_df, 'schema',
                                                         ann_ke_count, ke, k)

    return schema_ke_stats_df


def compute_ke_stats(ta2_team_name: str, task1_score_directory: str, annotation_directory: str, ke: str, k: int) \
        -> Tuple[pd.DataFrame, pd.DataFrame]:
    if ke == 'ev':
        input_directory = task1_score_directory + ta2_team_name + '/Assessed_events/'
    elif ke == 'arg':
        input_directory = task1_score_directory + ta2_team_name + '/Assessed_arguments/'
    elif ke == 'rel':
        input_directory = task1_score_directory + ta2_team_name + '/Assessed_relations/'
    elif ke == 'ent':
        input_directory = task1_score_directory + ta2_team_name + '/Assessed_relation_entities/'
    else:
        sys.exit('KE error!')

    if not os.path.isdir(input_directory):
        sys.exit('Directory not found: ' + input_directory)

    file_ke_stats_df = pd.DataFrame(columns=['file_name', 'TA1', 'TA2', 'target_ce',
                                             'all_{0}_count'.format(ke), 'assessed_{0}_count'.format(ke),
                                             'assessed_{0}_woer_count'.format(ke),
                                             'human_assessed_{0}_count'.format(ke),
                                             'true_{0}_count'.format(ke), 'distinct_true_{0}_count'.format(ke),
                                             'extra_relevant_{0}_count'.format(ke),
                                             'true_{0}_count@{1}'.format(ke, str(k)),
                                             'distinct_true_{0}_count@{1}'.format(ke, str(k)),
                                             'extra_relevant_{0}_count@{1}'.format(ke, str(k)),
                                             'true_{0}_woer_count@{1}'.format(ke, str(k)),
                                             'distinct_true_{0}_woer_count@{1}'.format(ke, str(k)),
                                             'ann_{0}_count'.format(ke),
                                             'match_{0}_count'.format(ke), 'match-inexact_{0}_count'.format(ke),
                                             'extra_relevant_{0}_yes_count'.format(ke),
                                             'extra_relevant_{0}_no_count'.format(ke),
                                             'extra_relevant_{0}_empty_count'.format(ke),
                                             'extra-irrelevant_{0}_count'.format(ke),
                                             'unassessed_{0}_count'.format(ke), 'non-match_{0}_count'.format(ke)])
    schema_ke_stats_df = pd.DataFrame(columns=['file_name', 'schema_id', 'schema_super', 'TA1', 'TA2', 'target_ce',
                                               'all_{0}_count'.format(ke), 'assessed_{0}_count'.format(ke),
                                               'assessed_{0}_woer_count'.format(ke),
                                               'human_assessed_{0}_count'.format(ke),
                                               'true_{0}_count'.format(ke), 'distinct_true_{0}_count'.format(ke),
                                               'extra_relevant_{0}_count'.format(ke),
                                               'true_{0}_count@{1}'.format(ke, str(k)),
                                               'distinct_true_{0}_count@{1}'.format(ke, str(k)),
                                               'extra_relevant_{0}_count@{1}'.format(ke, str(k)),
                                               'true_{0}_woer_count@{1}'.format(ke, str(k)),
                                               'distinct_true_{0}_woer_count@{1}'.format(ke, str(k)),
                                               'ann_{0}_count'.format(ke),
                                               'match_{0}_count'.format(ke), 'match-inexact_{0}_count'.format(ke),
                                               'extra_relevant_{0}_yes_count'.format(ke),
                                               'extra_relevant_{0}_no_count'.format(ke),
                                               'extra_relevant_{0}_empty_count'.format(ke),
                                               'extra-irrelevant_{0}_count'.format(ke),
                                               'unassessed_{0}_count'.format(ke), 'non-match_{0}_count'.format(ke)])

    if ke == 'arg':
        file_ke_stats_df.insert(len(file_ke_stats_df.columns), 'mm_{0}_count'.format(ke), 0)
        schema_ke_stats_df.insert(len(schema_ke_stats_df.columns), 'mm_{0}_count'.format(ke), 0)

    for file_name in tqdm(os.listdir(input_directory), position=0, leave=False):
        fn_suffix = file_name[-4:]
        if fn_suffix != '.csv':
            continue

        target_ce = file_name.split('-')[2]
        ann_ke_count = compute_target_ann_ke_count(annotation_directory, target_ce, ke)
        if ann_ke_count != -1:
            # compute file event stats
            file_ke_df = pd.read_csv(input_directory + file_name, low_memory=False)
            file_ke_stats_df = compute_single_ke_stats(file_ke_df, file_ke_stats_df, 'file', ann_ke_count, ke, k)

            # compute schema event stats
            schema_ke_stats_df = compute_schema_ke_stats(file_ke_df, schema_ke_stats_df, ann_ke_count, ke, k)

    return file_ke_stats_df, schema_ke_stats_df


def add_accuracy_columns(input_ke_score_df: pd.DataFrame, base_index: int, k: int) -> pd.DataFrame:
    input_ke_score_df.insert(base_index, 'precision', None)
    base_index += 1
    input_ke_score_df.insert(base_index, 'recall', None)
    base_index += 1
    input_ke_score_df.insert(base_index, 'f_measure', None)
    base_index += 1
    input_ke_score_df.insert(base_index, 'precision_woer', None)
    base_index += 1
    input_ke_score_df.insert(base_index, 'recall_woer', None)
    base_index += 1
    input_ke_score_df.insert(base_index, 'f_measure_woer', None)
    base_index += 1
    input_ke_score_df.insert(base_index, 'precision@{0}'.format(str(k)), None)
    base_index += 1
    input_ke_score_df.insert(base_index, 'recall@{0}'.format(str(k)), None)
    base_index += 1
    input_ke_score_df.insert(base_index, 'f_measure@{0}'.format(str(k)), None)
    base_index += 1
    input_ke_score_df.insert(base_index, 'precision@{0}_woer'.format(str(k)), None)
    base_index += 1
    input_ke_score_df.insert(base_index, 'recall@{0}_woer'.format(str(k)), None)
    base_index += 1
    input_ke_score_df.insert(base_index, 'f_measure@{0}_woer'.format(str(k)), None)

    return input_ke_score_df


def compute_precision_recall_f_measure(input_ke_score_df: pd.DataFrame, ke: str, k: int) -> pd.DataFrame:
    for i, row in input_ke_score_df.iterrows():
        # get file stats
        assessed_ke_count = row.get('assessed_{0}_count'.format(ke))
        assessed_ke_woer_count = row.get('assessed_{0}_woer_count'.format(ke))
        true_ke_count = row.get('true_{0}_count'.format(ke))
        distinct_true_ke_count = row.get('distinct_true_{0}_count'.format(ke))
        extra_relevant_ke_count = row.get('extra_relevant_{0}_count'.format(ke))
        true_ke_count_at_top_k = row.get('true_{0}_count@{1}'.format(ke, str(k)))
        distinct_true_ke_count_at_top_k = row.get('distinct_true_{0}_count@{1}'.format(ke, str(k)))
        extra_relevant_ke_count_at_top_k = row.get('extra_relevant_{0}_count@{1}'.format(ke, str(k)))
        true_ke_woer_count_at_top_k = row.get('true_{0}_woer_count@{1}'.format(ke, str(k)))
        distinct_true_ke_woer_count_at_top_k = row.get('distinct_true_{0}_woer_count@{1}'.format(ke, str(k)))
        ann_ke_count = row.get('ann_{0}_count'.format(ke))
        precision = recall = f_measure = None
        precision_woer = recall_woer = f_measure_woer = None
        precision_at_top_k = recall_at_top_k = f_measure_at_top_k = None
        precision_at_top_k_woer = recall_at_top_k_woer = f_measure_at_top_k_woer = None

        if assessed_ke_count > 0:
            precision = (true_ke_count + extra_relevant_ke_count) / assessed_ke_count
            input_ke_score_df.at[i, 'precision'] = precision
        if assessed_ke_woer_count > 0:
            precision_woer = true_ke_count / assessed_ke_woer_count
            input_ke_score_df.at[i, 'precision_woer'] = precision_woer
        precision_at_top_k = (true_ke_count_at_top_k + extra_relevant_ke_count_at_top_k) / k
        input_ke_score_df.at[i, 'precision@{0}'.format(str(k))] = precision_at_top_k
        precision_at_top_k_woer = true_ke_woer_count_at_top_k / k
        input_ke_score_df.at[i, 'precision@{0}_woer'.format(str(k))] = precision_at_top_k_woer
        if ann_ke_count > 0:
            recall = (distinct_true_ke_count + extra_relevant_ke_count) / ann_ke_count
            input_ke_score_df.at[i, 'recall'] = recall
            recall_woer = distinct_true_ke_count / ann_ke_count
            input_ke_score_df.at[i, 'recall_woer'] = recall_woer
            recall_at_top_k = (distinct_true_ke_count_at_top_k + extra_relevant_ke_count_at_top_k) / ann_ke_count
            input_ke_score_df.at[i, 'recall@{0}'.format(str(k))] = recall_at_top_k
            recall_at_top_k_woer = distinct_true_ke_woer_count_at_top_k / ann_ke_count
            input_ke_score_df.at[i, 'recall@{0}_woer'.format(str(k))] = recall_at_top_k_woer
        if precision and recall:
            if precision + recall > 0:
                f_measure = (2 * precision * recall) / (precision + recall)
                input_ke_score_df.at[i, 'f_measure'] = f_measure
        if precision_woer and recall_woer:
            if precision_woer + recall_woer > 0:
                f_measure_woer = (2 * precision_woer * recall_woer) / (precision_woer + recall_woer)
                input_ke_score_df.at[i, 'f_measure_woer'] = f_measure_woer
        if precision_at_top_k and recall_at_top_k:
            if precision_at_top_k + recall_at_top_k > 0:
                f_measure_at_top_k = (2 * precision_at_top_k * recall_at_top_k) / (precision_at_top_k + recall_at_top_k)
                input_ke_score_df.at[i, 'f_measure@{0}'.format(str(k))] = f_measure_at_top_k
        if precision_at_top_k_woer and recall_at_top_k_woer:
            if precision_at_top_k_woer + recall_at_top_k_woer > 0:
                f_measure_at_top_k_woer = (2 * precision_at_top_k_woer * recall_at_top_k_woer) / \
                                          (precision_at_top_k_woer + recall_at_top_k_woer)
                input_ke_score_df.at[i, 'f_measure@{0}_woer'.format(str(k))] = f_measure_at_top_k_woer

    return input_ke_score_df


def score_ke(input_ke_stats_df: pd.DataFrame, input_unit: str, ke: str, k: int) -> pd.DataFrame:
    input_ke_score_df = input_ke_stats_df.copy()

    if input_unit == 'file':
        base_index = 4
    elif input_unit == 'schema':
        base_index = 5
    else:
        sys.exit('Error input unit, please use file or schema')

    input_ke_score_df = add_accuracy_columns(input_ke_score_df, base_index, k)

    input_ke_score_df = compute_precision_recall_f_measure(input_ke_score_df, ke, k)

    return input_ke_score_df


def score_file_and_schema_ke(file_ke_stats_df: pd.DataFrame, schema_ke_stats_df: pd.DataFrame, ke: str, k: int) \
        -> Tuple[pd.DataFrame, pd.DataFrame]:
    # compute file based event score
    file_ke_score_df = score_ke(file_ke_stats_df, 'file', ke, k)
    # compute schema based event score
    schema_ke_score_df = score_ke(schema_ke_stats_df, 'schema', ke, k)

    return file_ke_score_df, schema_ke_score_df


def score(ta2_team_name: str, mode: str, ta2_ext: bool, ev_arg_assess: bool):
    if mode == 'test':
        target_pairs = []
    else:
        target_pairs = ['cmu-cmu', 'isi-cmu', 'ibm-ibm', 'isi-ibm',
                        'jhu-jhu', 'sbu-jhu', 'resin-resin', 'sbu-resin']

    ta2_team_name = ta2_team_name.upper()

    base_dir = ''
    # get path of directories
    try:
        config = configparser.ConfigParser()
        base_dir = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(base_dir, "../config.ini")) as configfile:
            config.read_file(configfile)
    except:
        sys.exit('CAN NOT OPEN CONFIG FILE: ' + os.path.join(os.path.dirname(base_dir), "../config.ini"))

    if mode == 'evaluation':
        ta2_task1_output_directory = config['Phase1Eval']['ta2_task1_output_directory']
        task1_score_directory = config['Phase1Eval']['task1_score_directory']
        assessment_directory = config['Phase1Eval']['assessment_directory']
        annotation_directory = config['Phase1Eval']['annotation_directory']
        caci_directory = config['Phase1Eval']['caci_directory']
    elif mode == 'test':
        ta2_task1_output_directory = config['Test']['ta2_task1_output_directory']
        task1_score_directory = config['Test']['task1_score_directory']
        assessment_directory = config['Test']['assessment_directory']
        annotation_directory = config['Test']['annotation_directory']
        caci_directory = config['Test']['caci_directory']
    else:
        sys.exit('Please enter the correct mode: evaluation or test')

    if not os.path.isdir(ta2_task1_output_directory):
        sys.exit('Input directory not found: ' + ta2_task1_output_directory)
    if not os.path.exists(task1_score_directory):
        os.makedirs(task1_score_directory)
    if not os.path.isdir(assessment_directory):
        sys.exit('Input directory not found: ' + assessment_directory)
    if not os.path.isdir(annotation_directory):
        sys.exit('Input directory not found: ' + annotation_directory)

    # extract event and argument from json-ld outputs
    print('-------------------------------------------------------------------')
    if ta2_ext:
        print('Start to extract EV and ARG from TA2_{0} Task1 outputs...'.format(ta2_team_name))
        time.sleep(0.1)
        extract_event_argument.extract_ta2_ev_arg_from_json(
            ta2_team_name, ta2_task1_output_directory, task1_score_directory, target_pairs, annotation_directory)
        print('Extraction completed!')
    else:
        print('Skip EV and ARG extraction from TA2_{0} Task1 outputs.'.format(ta2_team_name))

    print('-------------------------------------------------------------------')
    if ev_arg_assess:
        time.sleep(0.1)
        assign_ke_assessment(ta2_team_name, task1_score_directory, assessment_directory,
                             caci_directory, 'ev', target_pairs)
        assign_ke_assessment(ta2_team_name, task1_score_directory, assessment_directory,
                             caci_directory, 'arg', target_pairs)
        print('EV and ARG assignment completed!')
    else:
        print('Skip assigning of EV and ARG assessments for TA2_{0}'.format(ta2_team_name))

    print('----------------------------------------------------------------------------------------------------------')
    print('Start to score TA2_{0} Task1 EV in terms of precision, recall, and F measure...'.format(ta2_team_name))
    time.sleep(0.1)
    k = 20
    file_ev_stats_df, schema_ev_stats_df = compute_ke_stats(ta2_team_name, task1_score_directory,
                                                            annotation_directory, 'ev', k)
    file_ev_score_df, schema_ev_score_df = score_file_and_schema_ke(file_ev_stats_df, schema_ev_stats_df, 'ev', k)

    # write to files
    file_ev_score_df.to_excel(
        '{0}{1}/task1_file_score_event_{2}.xlsx'.format(task1_score_directory, ta2_team_name, ta2_team_name),
        index=False)
    schema_ev_score_df.to_excel(
        '{0}{1}/task1_schema_score_event_{2}.xlsx'.format(task1_score_directory, ta2_team_name, ta2_team_name),
        index=False)
    print('Scoring TA2_{0} Task1 EV finished!'.format(ta2_team_name))

    print('----------------------------------------------------------------------------------------------------------')
    print('Start to score TA2_{0} Task1 ARG in terms of precision, recall, and F measure...'.format(ta2_team_name))
    time.sleep(0.1)
    file_arg_stats_df, schema_arg_stats_df = compute_ke_stats(ta2_team_name, task1_score_directory,
                                                              annotation_directory, 'arg', k)
    file_arg_score_df, schema_arg_score_df = score_file_and_schema_ke(file_arg_stats_df, schema_arg_stats_df,
                                                                      'arg', k)

    # write to files
    file_arg_score_df.to_excel(
        '{0}{1}/task1_file_score_argument_{2}.xlsx'.format(task1_score_directory, ta2_team_name, ta2_team_name),
        index=False)
    schema_arg_score_df.to_excel(
        '{0}{1}/task1_schema_score_argument_{2}.xlsx'.format(task1_score_directory, ta2_team_name, ta2_team_name),
        index=False)
    print('Scoring TA2_{0} Task1 ARG finished!'.format(ta2_team_name))


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def cli_parser():
    parser = argparse.ArgumentParser(
        description='Task1 scorer for event primitives and arguments'
    )

    parser.add_argument('-t', '--ta2-team-name', help='supported ta2 team names are: CMU, IBM, JHU, and RESIN',
                        required=True, type=str)
    parser.add_argument('-t2e', '--ta2_ext', help='whether to conduct ta2 output extraction',
                        required=False, type=str2bool, default=True)
    parser.add_argument('-eaa', '--ev_arg_assess', help='whether to conduct assigning of '
                                                        'event and argument assessment',
                        required=False, type=str2bool, default=True)
    parser.add_argument('-m', '--mode', help='the mode of the scorer: test or evaluation',
                        required=False, type=str, default='evaluation')
    parser.add_argument('-v', '--verbose', help='Switch logger to debug level',
                        required=False, action='store_true', default=False)

    parser.set_defaults(func=score)
    args = parser.parse_args()

    logging.basicConfig()

    if hasattr(args, 'func') and args.func:
        logging.getLogger().setLevel(logging.INFO)
        if args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)

        args_dict = dict(args.__dict__)
        del args_dict['func']
        del args_dict['verbose']

        args.func(**args_dict)
    else:
        parser.print_help()


if __name__ == "__main__":
    mode = 'evaluation'
    ta2_ext = True
    ev_arg_assess = True

    # ta2_team_name = 'cmu'
    # score(ta2_team_name, mode, ta2_ext, ev_arg_assess)
    # ta2_team_name = 'ibm'
    # score(ta2_team_name, mode, ta2_ext, ev_arg_assess)
    # ta2_team_name = 'jhu'
    # score(ta2_team_name, mode, ta2_ext, ev_arg_assess)
    ta2_team_name = 'resin'
    score(ta2_team_name, mode, ta2_ext, ev_arg_assess)
