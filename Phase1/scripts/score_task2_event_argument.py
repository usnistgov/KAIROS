#!/usr/bin/env python3

__author__ = "Xiongnan Jin (xiongnan.jin@nist.gov)"
__version__ = "Development: 0.5.7"
__date__ = "03/09/2021"

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

##########################################################################################
# extract event (ev) and argument (arg) from TA2 output JSON files and save as csv files
# read extracted ev and arg csv files and compute recall score
##########################################################################################
import argparse
import collections
import configparser
import logging
import pickle
import sys
import pandas as pd
import os
import copy
import time

from typing import Tuple, Mapping

from tqdm import tqdm
from scripts import extract_event_argument

scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../scripts")
sys.path.append(scripts_path)


def initiate_file_schema_score_df() -> Tuple[pd.DataFrame, pd.DataFrame]:
    # initiate schema based stats
    file_score_df = pd.DataFrame(columns=['file_name', 'CE', 'TA2', 'TA1', 'file_ev_all_count',
                                          'file_ta2_gev_all_count',
                                          'file_ta2_gev_crt_count', 'file_ta2_gev_opt_count',
                                          'file_ta1_gev_sst_all_count', 'file_ta1_gev_sst_crt_count',
                                          'file_ta1_gev_st_all_count', 'file_ta1_gev_st_crt_count',
                                          'file_ta1_gev_t_all_count', 'file_ta1_gev_t_crt_count',
                                          'file_ta1_aev_sst_all_count', 'file_ta1_aev_sst_crt_count',
                                          'file_ta1_aev_st_all_count', 'file_ta1_aev_st_crt_count',
                                          'file_ta1_aev_t_all_count', 'file_ta1_aev_t_crt_count',
                                          'gev_all_count', 'gev_crt_count', 'gev_opt_count',
                                          'aev_all_count', 'aev_crt_count',
                                          'file_arg_all_count', 'file_ta2_gev_ta2_garg_count',
                                          'file_ta1_gev_sst_ta1_garg_count', 'file_ta1_gev_st_ta1_garg_count',
                                          'file_ta1_gev_t_ta1_garg_count',
                                          'file_ta1_aev_sst_ta1_aarg_count', 'file_ta1_aev_st_ta1_aarg_count',
                                          'file_ta1_aev_t_ta1_aarg_count',
                                          'garg_count', 'aarg_count'])
    # initiate schema based stats
    schema_score_df = pd.DataFrame(columns=['file_name', 'schema_id', 'schema_super', 'CE', 'TA2', 'TA1',
                                            'schema_ev_all_count',
                                            'schema_ta2_gev_all_count',
                                            'schema_ta2_gev_crt_count', 'schema_ta2_gev_opt_count',
                                            'schema_ta1_gev_sst_all_count', 'schema_ta1_gev_sst_crt_count',
                                            'schema_ta1_gev_st_all_count', 'schema_ta1_gev_st_crt_count',
                                            'schema_ta1_gev_t_all_count', 'schema_ta1_gev_t_crt_count',
                                            'schema_ta1_aev_sst_all_count', 'schema_ta1_aev_sst_crt_count',
                                            'schema_ta1_aev_st_all_count', 'schema_ta1_aev_st_crt_count',
                                            'schema_ta1_aev_t_all_count', 'schema_ta1_aev_t_crt_count',
                                            'gev_all_count', 'gev_crt_count', 'gev_opt_count',
                                            'aev_all_count', 'aev_crt_count',
                                            'schema_arg_all_count', 'schema_ta2_gev_ta2_garg_count',
                                            'schema_ta1_gev_sst_ta1_garg_count', 'schema_ta1_gev_st_ta1_garg_count',
                                            'schema_ta1_gev_t_ta1_garg_count',
                                            'schema_ta1_aev_sst_ta1_aarg_count', 'schema_ta1_aev_st_ta1_aarg_count',
                                            'schema_ta1_aev_t_ta1_aarg_count',
                                            'garg_count', 'aarg_count'])

    return file_score_df, schema_score_df


def is_ta1ref_valid(ta1_ref) -> bool:
    if (not isinstance(ta1_ref, str)) or (isinstance(ta1_ref, str) and
                                          ta1_ref in ['kairos:null', 'KAIROS:NULL', 'n/a', 'nan', '', 'kairos:NULL']):
        return False

    return True


def is_provenance_valid(provenance: str, ke_type: str) -> bool:
    if isinstance(provenance, str) and len(provenance) > 2:
        if ke_type == 'event' and provenance.startswith('VP'):
            return True
        if ke_type == 'argument' and (provenance.startswith('AR') or provenance.startswith('VP')):
            return True

    return False


def is_type_matching_between_sys_ev_and_gev(sys_ev_arg_row: pd.Series, matching_gev_df: pd.DataFrame):
    ev_full_type = sys_ev_arg_row.ev_type
    ev_type_slash_list = ev_full_type.split('/')
    ev_type_dot_list = ev_type_slash_list[len(ev_type_slash_list) - 1].split('.')
    ev_type = ev_type_dot_list[0].lower()
    ev_subtype = ev_type_dot_list[1].lower()
    ev_subsubtype = ev_type_dot_list[2].lower()
    gev_type = matching_gev_df.iloc[0]['type']
    gev_subtype = matching_gev_df.iloc[0]['subtype']
    gev_subsubtype = matching_gev_df.iloc[0]['subsubtype']
    if ev_type == gev_type and ev_subtype == gev_subtype and ev_subsubtype == gev_subsubtype:
        return True

    return False


def get_matching_ta2_gev(sys_ta2_valid_ev_df: pd.DataFrame, gev_df: pd.DataFrame) -> pd.DataFrame:
    sys_ta2_gev_df = extract_event_argument.init_ta2_ev_arg_dataframe(True)
    if 'significance' not in sys_ta2_gev_df.columns:
        sys_ta2_gev_df.insert(len(sys_ta2_gev_df.columns), 'significance', '')
    if 'ref_ev_id' not in sys_ta2_gev_df.columns:
        sys_ta2_gev_df.insert(len(sys_ta2_gev_df.columns), 'ref_ev_id', '')

    if len(sys_ta2_valid_ev_df) == 0:
        return sys_ta2_gev_df

    for sys_ev_arg_row in sys_ta2_valid_ev_df.itertuples(index=False):
        # check the system event has proper ta1ref
        ev_ta1ref = sys_ev_arg_row.ev_ta1ref
        file_name = sys_ev_arg_row.file_name
        ta2_team_name = file_name.split('-')[1].upper()
        if is_ta1ref_valid(ev_ta1ref):
            ev_provenance = sys_ev_arg_row.ev_provenance
            # check the system event provenance points to graph G event id
            if is_provenance_valid(ev_provenance, 'event'):
                matching_gev_df = gev_df.loc[gev_df['eventprimitive_id'] == ev_provenance]
                # check the matching between ta2 event types and gev types
                if len(matching_gev_df) > 0 and \
                        is_type_matching_between_sys_ev_and_gev(sys_ev_arg_row, matching_gev_df):
                    significance = matching_gev_df.iloc[0]['significance']
                    gev_dict = sys_ev_arg_row._asdict()
                    gev_dict['significance'] = significance
                    gev_dict['ref_ev_id'] = ev_provenance
                    sys_ta2_gev_df = sys_ta2_gev_df.append(gev_dict, ignore_index=True)

                    example_directory = '../../../../Phase_1_evaluation/Examples/' + ta2_team_name + '/'
                    if not os.path.isdir(example_directory):
                        os.makedirs(example_directory)
                    example_fp = example_directory + ta2_team_name + '_ta2_gev_all.xlsx'
                    if not os.path.isfile(example_fp):
                        example = pd.Series(gev_dict)
                        example = example.take([0, 2, 3, 4, 5, 7, 10], axis=1)
                        example.to_excel(example_fp, header=False)
                else:
                    example_directory = '../../../../Phase_1_evaluation/Examples/' + ta2_team_name + '/'
                    if not os.path.isdir(example_directory):
                        os.makedirs(example_directory)
                    example_fp = example_directory + ta2_team_name + '_ta2_gev_all_type_not_match_ta2ev_gev.xlsx'
                    if not os.path.isfile(example_fp):
                        example = pd.Series(sys_ev_arg_row._asdict())
                        example = example.take([0, 2, 3, 4, 5, 7, 10], axis=1)
                        example.to_excel(example_fp, header=False)
            else:
                example_directory = '../../../../Phase_1_evaluation/Examples/' + ta2_team_name + '/'
                if not os.path.isdir(example_directory):
                    os.makedirs(example_directory)
                example_fp = example_directory + ta2_team_name + '_ta2_gev_all_provenance_not_valid.xlsx'
                if not os.path.isfile(example_fp):
                    example = pd.Series(sys_ev_arg_row._asdict())
                    example = example.take([0, 2, 3, 4, 5, 7, 10], axis=1)
                    example.to_excel(example_fp, header=False)
        else:
            example_directory = '../../../../Phase_1_evaluation/Examples/' + ta2_team_name + '/'
            if not os.path.isdir(example_directory):
                os.makedirs(example_directory)
            example_fp = example_directory + ta2_team_name + '_ta2_gev_all_ta1ref_not_valid.xlsx'
            if not os.path.isfile(example_fp):
                example = pd.Series(sys_ev_arg_row._asdict())
                example = example.take([0, 2, 3, 4, 5, 7, 10], axis=1)
                example.to_excel(example_fp, header=False)

    return sys_ta2_gev_df


def compute_ta1_schema_indices(score_directory: str) -> dict:
    ta1_schema_indices = {}
    ta1_team_name_list = ['CMU', 'IBM', 'ISI', 'JHU', 'RESIN', 'SBU']

    for ta1_team_name in ta1_team_name_list:
        ta1_schema_index = {}
        ta1_ev_arg_directory = score_directory + 'TA1_library/' + ta1_team_name + '/Event_arguments/'
        for file_name in os.listdir(ta1_ev_arg_directory):
            # only consider .csv file (ignoring system hidden files like .DS_store)
            fn_surfix = file_name[-4:]
            if fn_surfix != '.csv':
                continue
            # compute file score
            file_ev_arg_df = pd.read_csv(ta1_ev_arg_directory + file_name, low_memory=False)
            print('Indexing TA1_{0} schema library...'.format(ta1_team_name))
            time.sleep(0.1)
            for file_ev_arg_row in tqdm(file_ev_arg_df.itertuples(index=False), total=file_ev_arg_df.shape[0],
                                        position=0, leave=False):
                schema_id = file_ev_arg_row.schema_id
                ev_id = file_ev_arg_row.ev_id
                key = schema_id + ';' + ev_id
                if key not in ta1_schema_index.keys():
                    ta1_ev_arg_df = extract_event_argument.init_ta1_ev_arg_dataframe()
                    ta1_schema_index[key] = ta1_ev_arg_df.append(file_ev_arg_row._asdict(), ignore_index=True)
                else:
                    ta1_schema_index[key] = ta1_schema_index[key].append(
                        file_ev_arg_row._asdict(), ignore_index=True)

        ta1_schema_indices[ta1_team_name] = ta1_schema_index

    return ta1_schema_indices


def get_valid_ev(sys_ev_arg_df: pd.DataFrame, ta1_schema_indices: dict) -> pd.DataFrame:
    valid_df = extract_event_argument.init_ta2_ev_arg_dataframe(True)

    if len(sys_ev_arg_df) == 0:
        return valid_df

    ta1_team_name = sys_ev_arg_df.iloc[0]['file_name'].split('-')[0].upper()
    ta1_schema_index: dict = ta1_schema_indices[ta1_team_name]

    for sys_ta2_gev_row in sys_ev_arg_df.itertuples(index=False):
        file_name = sys_ta2_gev_row.file_name
        ta2_team_name = file_name.split('-')[1].upper()
        ta2_ev_type = sys_ta2_gev_row.ev_type
        ta2_schema_super = sys_ta2_gev_row.schema_super
        ta2_ev_ta1ref = sys_ta2_gev_row.ev_ta1ref

        ev_key = ta2_schema_super + ';' + ta2_ev_ta1ref
        if ev_key in ta1_schema_index.keys():
            matching_ta1_ev_df = ta1_schema_index[ev_key]
            ta1_ev_type = matching_ta1_ev_df.iloc[0]['ev_type']
            if ta2_ev_type == ta1_ev_type:
                valid_df = valid_df.append(sys_ta2_gev_row._asdict(), ignore_index=True)
                example_directory = '../../../../Phase_1_evaluation/Examples/' + ta2_team_name + '/'
                if not os.path.isdir(example_directory):
                    os.makedirs(example_directory)
                example_fp = example_directory + ta2_team_name + '_ta2_valid_all.xlsx'
                if not os.path.isfile(example_fp):
                    example = pd.Series(sys_ta2_gev_row._asdict())
                    example = example.take([0, 2, 3, 4, 5, 7, 10], axis=1)
                    example.to_excel(example_fp, header=False)
            else:
                example_directory = '../../../../Phase_1_evaluation/Examples/' + ta2_team_name + '/'
                if not os.path.isdir(example_directory):
                    os.makedirs(example_directory)
                example_fp = example_directory + ta2_team_name + '_ta2_gev_all_type_not_match_ta2ev_ta1ev.xlsx'
                if not os.path.isfile(example_fp):
                    example = pd.Series(sys_ta2_gev_row._asdict())
                    example = example.take([0, 2, 3, 4, 5, 7, 10], axis=1)
                    example.to_excel(example_fp, header=False)
        else:
            example_directory = '../../../../Phase_1_evaluation/Examples/' + ta2_team_name + '/'
            if not os.path.isdir(example_directory):
                os.makedirs(example_directory)
            example_fp = example_directory + ta2_team_name + '_ta2_gev_all_ta1ev_not_linked.xlsx'
            if not os.path.isfile(example_fp):
                example = pd.Series(sys_ta2_gev_row._asdict())
                example = example.take([0, 2, 3, 4, 5, 7, 10], axis=1)
                example.to_excel(example_fp, header=False)

    return valid_df


def get_matching_ta1_ev(sys_valid_ev_df: pd.DataFrame, ref_ev_df: pd.DataFrame,
                        type_match_depth: str) -> pd.DataFrame:
    sys_ta1_ev_df = extract_event_argument.init_ta2_ev_arg_dataframe(True)
    if 'significance' not in sys_ta1_ev_df.columns:
        sys_ta1_ev_df.insert(len(sys_ta1_ev_df.columns), 'significance', '')
    if 'ref_ev_id' not in sys_ta1_ev_df.columns:
        sys_ta1_ev_df.insert(len(sys_ta1_ev_df.columns), 'ref_ev_id', '')

    if len(sys_valid_ev_df) == 0:
        return sys_ta1_ev_df

    for sys_input_row in sys_valid_ev_df.itertuples(index=False):
        # check whether type, subtype, and subsubtype match exactly with reference
        ev_type = sys_input_row.ev_type
        slash_list = ev_type.split('/')
        full_type = slash_list[len(slash_list) - 1]
        type_list = full_type.split('.')
        type = type_list[0].lower()
        subtype = type_list[1].lower()
        subsubtype = type_list[2].lower()

        # get matching event from reference according to type match depth
        mev_df = pd.DataFrame()
        if type_match_depth == 'subsubtype':
            mev_df = ref_ev_df.loc[(ref_ev_df['type'] == type) & (ref_ev_df['subtype'] == subtype)
                                   & (ref_ev_df['subsubtype'] == subsubtype)]
        elif type_match_depth == 'subtype':
            mev_df = ref_ev_df.loc[(ref_ev_df['type'] == type) & (ref_ev_df['subtype'] == subtype)]
        elif type_match_depth == 'type':
            mev_df = ref_ev_df.loc[ref_ev_df['type'] == type]

        if len(mev_df) > 0:
            ref_id_list = []
            for mev_row in mev_df.itertuples():
                mev_ref_id = mev_row.eventprimitive_id
                if mev_ref_id not in ref_id_list:
                    ref_id_list.append(mev_ref_id)

            significance = mev_df.iloc[0]['significance']
            mev_dict = sys_input_row._asdict()
            mev_dict['significance'] = significance
            mev_dict['ref_ev_id'] = ref_id_list
            sys_ta1_ev_df = sys_ta1_ev_df.append(mev_dict, ignore_index=True)

    return sys_ta1_ev_df


def is_role_and_type_matching(sys_ta2_gev_arg_row, matching_garg_df: pd.DataFrame) -> bool:
    ## check the matching between ta2 arg role and type with garg
    # role check
    arg_role_slash_list = sys_ta2_gev_arg_row.arg_role.split('/')
    arg_role = arg_role_slash_list[len(arg_role_slash_list) - 1].lower()
    garg_role = matching_garg_df.iloc[0]['slot_type'][11:]
    if arg_role == garg_role:
        # type check
        row_arg_type = sys_ta2_gev_arg_row.arg_type.replace('\'', '')
        row_arg_type = row_arg_type.replace('[', '')
        row_arg_type = row_arg_type.replace(']', '')
        row_arg_type = row_arg_type.replace(' ', '')
        arg_type_comma_list = row_arg_type.split(',')
        arg_full_type_list = []
        for i in range(len(arg_type_comma_list)):
            cur_val = arg_type_comma_list[i]
            slash_list = cur_val.split('/')
            arg_full_type = slash_list[len(slash_list) - 1]
            dot_list = arg_full_type.split('.')
            if len(dot_list) == 1:
                arg_type = dot_list[0].lower()
                arg_subtype = ''
                arg_subsubtype = ''
            else:
                arg_type = dot_list[0].lower()
                arg_subtype = dot_list[1].lower()
                arg_subsubtype = dot_list[2].lower()
            arg_full_type_list.append([arg_type, arg_subtype, arg_subsubtype])

        garg_type = matching_garg_df.iloc[0]['type']
        garg_subtype = matching_garg_df.iloc[0]['subtype']
        if not isinstance(garg_subtype, str):
            garg_subtype = ''
        garg_subsubtype = matching_garg_df.iloc[0]['subsubtype']
        if not isinstance(garg_subsubtype, str):
            garg_subsubtype = ''

        for i in range(len(arg_full_type_list)):
            arg_type = arg_full_type_list[i][0]
            arg_subtype = arg_full_type_list[i][1]
            arg_subsubtype = arg_full_type_list[i][2]
            if garg_type == arg_type and garg_subtype == arg_subtype and \
                    garg_subsubtype == arg_subsubtype:
                return True

    return False


def get_matching_ta2_garg(ta2_gev_arg_df: pd.DataFrame, garg_df: pd.DataFrame) -> pd.DataFrame:
    # initiate matching graph g arg dataframe
    sys_ta2_gev_garg_df = extract_event_argument.init_ta2_ev_arg_dataframe(True)

    if len(ta2_gev_arg_df) == 0:
        return sys_ta2_gev_garg_df

    cur_garg_df = copy.deepcopy(garg_df)
    for sys_ta2_gev_row in ta2_gev_arg_df.itertuples():
        # check a system value provenance points to a graph G argument id,
        val_provenance = sys_ta2_gev_row.val_provenance
        if is_provenance_valid(val_provenance, 'argument'):
            ev_provenance = sys_ta2_gev_row.ev_provenance
            matching_garg_df = cur_garg_df.loc[(cur_garg_df['arg_id'] == val_provenance) &
                                               (cur_garg_df['eventprimitive_id'] == ev_provenance)]
            if len(matching_garg_df) > 0:
                ## check the matching between ta2 arg role and type with garg
                if is_role_and_type_matching(sys_ta2_gev_row, matching_garg_df):
                    # then add it to sys_gev_garg_df and delete it from rarg_df
                    sys_ta2_gev_garg_df = sys_ta2_gev_garg_df.append(sys_ta2_gev_row._asdict(), ignore_index=True)
                    cur_garg_df = cur_garg_df.loc[cur_garg_df['arg_id'] != val_provenance]

    return sys_ta2_gev_garg_df


def is_arg_type_matching(ta1_arg_type: str, ta2_arg_type: str) -> bool:
    ta1_arg_type = ta1_arg_type.replace('\'', '')
    ta1_arg_type = ta1_arg_type.replace('[', '')
    ta1_arg_type = ta1_arg_type.replace(']', '')
    ta1_arg_type = ta1_arg_type.replace(' ', '')

    ta2_arg_type = ta2_arg_type.replace('\'', '')
    ta2_arg_type = ta2_arg_type.replace('[', '')
    ta2_arg_type = ta2_arg_type.replace(']', '')
    ta2_arg_type = ta2_arg_type.replace(' ', '')
    # if both ta1 arg type and ta2 arg type are single value
    if ',' not in ta1_arg_type and ',' not in ta2_arg_type:
        if ta1_arg_type == ta2_arg_type:
            return True
    # if ta1 arg type is a single value and ta2 arg type is a list
    elif ',' not in ta1_arg_type and ',' in ta2_arg_type:
        return False
    # if ta1 arg type is a list and ta2 arg type is a single value
    elif ',' in ta1_arg_type and ',' not in ta2_arg_type:
        ta1_arg_type_list = ta1_arg_type.split(',')
        if ta2_arg_type in ta1_arg_type_list:
            return True
    # if both ta1 arg type and ta2 arg type are list
    elif ',' in ta1_arg_type and ',' in ta2_arg_type:
        ta1_arg_type_list = ta1_arg_type.split(',')
        ta2_arg_type_list = ta2_arg_type.split(',')
        ta1_arg_type_count = collections.Counter(ta1_arg_type_list)
        ta2_arg_type_count = collections.Counter(ta2_arg_type_list)
        if ta1_arg_type_count == ta2_arg_type_count:
            return True

    return False


def get_valid_arg(sys_ev_arg_df: pd.DataFrame, ta1_schema_indices: dict) -> pd.DataFrame:
    valid_df = extract_event_argument.init_ta2_ev_arg_dataframe(True)

    if len(sys_ev_arg_df) == 0:
        return valid_df

    ta1_team_name = sys_ev_arg_df.iloc[0]['file_name'].split('-')[0].upper()
    ta1_schema_index: dict = ta1_schema_indices[ta1_team_name]

    for ta2_row in sys_ev_arg_df.itertuples(index=False):
        ta2_arg_type = ta2_row.arg_type
        ta2_arg_role = ta2_row.arg_role
        ta2_ev_ta1ref = ta2_row.ev_ta1ref
        ta2_schema_super = ta2_row.schema_super

        ev_key = ta2_schema_super + ';' + ta2_ev_ta1ref
        if ev_key in ta1_schema_index.keys():
            ta1_ev_row = ta1_schema_index[ev_key]
            ta1_arg_row = ta1_ev_row.loc[ta1_ev_row['arg_role'] == ta2_arg_role]
            if len(ta1_arg_row) > 0:
                ta1_arg_type = ta1_arg_row.iloc[0]['arg_type']
                if is_arg_type_matching(ta1_arg_type, ta2_arg_type):
                    valid_df = valid_df.append(ta2_row._asdict(), ignore_index=True)

    return valid_df


def get_matching_ta1_arg(sys_ev_arg_df: pd.DataFrame, ref_arg_df: pd.DataFrame, hot_fix_set: set) -> pd.DataFrame:
    output_df = extract_event_argument.init_ta2_ev_arg_dataframe(True)
    if 'ref_arg_id' not in output_df.columns:
        output_df.insert(len(output_df.columns), 'ref_arg_id', '')

    if len(sys_ev_arg_df) == 0:
        return output_df

    for sys_input_row in sys_ev_arg_df.itertuples():
        # check whether role and type match exactly with reference
        # skip empty argument
        ref_ev_id_list = sys_input_row.ref_ev_id
        arg_id = sys_input_row.arg_id
        if arg_id == 'empty':
            continue
        arg_role = sys_input_row.arg_role
        # convert argument role from 'kairos:Primitives/Events/Cognitive.TeachingTrainingLearning.Unspecified/
        # Slots/TeacherTrainer' to 'teachertrainer'
        arg_role_slash_list = arg_role.split('/')
        arg_role = arg_role_slash_list[len(arg_role_slash_list) - 1].lower()
        arg_type = sys_input_row.arg_type
        arg_type = arg_type.replace('[', '')
        arg_type = arg_type.replace(']', '')
        arg_type = arg_type.replace('\'', '')
        # convert argument type from 'kairos:Primitives/Entities/PER' to 'per'
        marg_df = pd.DataFrame()
        if ',' not in arg_type:
            arg_type_slash_list = arg_type.split('/')
            arg_type = arg_type_slash_list[len(arg_type_slash_list) - 1].lower()
            # suppose it is an entity argument
            if '.' not in arg_type:
                for ref_row in ref_arg_df.itertuples(index=False):
                    ref_ev_id = ref_row.eventprimitive_id
                    ref_arg_type = ref_row.type
                    ref_arg_subtype = ref_row.subtype
                    ref_arg_slot_type = ref_row.slot_type[11:]
                    if arg_type == 'event' and (not isinstance(ref_arg_subtype, str) or ref_arg_subtype != 'EMPTY_NA'):
                        if ref_arg_slot_type == arg_role and ref_ev_id in ref_ev_id_list:
                            marg_df = marg_df.append(ref_row._asdict(), ignore_index=True)
                    else:
                        if ref_ev_id in ref_ev_id_list:
                            hot_fix_key = '{0};{1}'.format(ref_ev_id, ref_arg_slot_type)
                            if (hot_fix_key in hot_fix_set and ref_arg_slot_type == arg_role) or \
                                    (ref_arg_type == arg_type and ref_arg_slot_type == arg_role):
                                marg_df = marg_df.append(ref_row._asdict(), ignore_index=True)
            # suppose it is an event argument
            elif '.' in arg_type:
                type_list = arg_type.split('.')
                ev_arg_type = type_list[0]
                ev_arg_subtype = type_list[1]
                ev_arg_subsubtype = type_list[2]
                for ref_row in ref_arg_df.itertuples(index=False):
                    ref_ev_id = ref_row.eventprimitive_id
                    ref_arg_type = ref_row.type
                    ref_arg_subtype = ref_row.subtype
                    ref_arg_subsubtype = ref_row.subsubtype
                    ref_arg_slot_type = ref_row.slot_type[11:]
                    if ref_arg_type == ev_arg_type and ref_arg_subtype == ev_arg_subtype \
                            and ref_arg_subsubtype == ev_arg_subsubtype and ref_arg_slot_type == arg_role \
                            and ref_ev_id in ref_ev_id_list:
                        marg_df = marg_df.append(ref_row._asdict(), ignore_index=True)
        elif ',' in arg_type:
            arg_type_list = arg_type.split(',')
            for j in range(len(arg_type_list)):
                cur_arg_type = arg_type_list[j]
                arg_type_slash_list = cur_arg_type.split('/')
                arg_type_list[j] = arg_type_slash_list[len(arg_type_slash_list) - 1].lower()
            for ref_row in ref_arg_df.itertuples(index=False):
                ref_ev_id = ref_row.eventprimitive_id
                ref_arg_type = ref_row.type
                ref_arg_slot_type = ref_row.slot_type[11:]
                if ref_arg_type in arg_type_list and ref_arg_slot_type == arg_role and ref_ev_id in ref_ev_id_list:
                    marg_df = marg_df.append(ref_row._asdict(), ignore_index=True)

        if len(marg_df) > 0:
            ref_id_list = []
            for marg_row in marg_df.itertuples(index=False):
                marg_ref_id = marg_row.arg_id
                if marg_ref_id not in ref_id_list:
                    ref_id_list.append(marg_ref_id)

            marg_dict = sys_input_row._asdict()
            marg_dict['ref_arg_id'] = ref_id_list
            output_df = output_df.append(marg_dict, ignore_index=True)

    return output_df


def count_distinct_ke(input_df: pd.DataFrame, ke: str) -> int:
    if ke == 'ev':
        input_df = input_df.drop_duplicates(subset=['schema_id', 'ev_id'])

    visited_ref_set = set()
    count = 0

    for input_row in input_df.itertuples(index=False):
        if ke == 'ev':
            ref_id_list = input_row.ref_ev_id
        elif ke == 'arg':
            ref_id_list = input_row.ref_arg_id
        else:
            sys.exit('KE error: ' + ke)

        for ref_id in ref_id_list:
            if ref_id not in visited_ref_set:
                count += 1
                visited_ref_set.add(ref_id)
                break

    return count


def add_significance_to_graph_g_ev(gev_df: pd.DataFrame, aev_df: pd.DataFrame) -> pd.DataFrame:
    gev_df.insert(len(gev_df.columns), 'significance', '')

    for gev_row in gev_df.itertuples():
        gev_id = gev_row.eventprimitive_id
        matching_aev_df = aev_df.loc[aev_df['eventprimitive_id'] == gev_id]
        significance = matching_aev_df.iloc[0]['significance']
        gev_df.at[gev_row[0], 'significance'] = significance

    return gev_df


def read_ke_reference(graph_g_directory: str, annotation_directory: str, target_ce: str, ke: str) \
        -> Tuple[pd.DataFrame, pd.DataFrame]:
    if ke == 'ev':
        graph_g_ke_df = pd.read_csv(graph_g_directory + target_ce + '/' + target_ce + '_GraphG_ev.csv',
                                    low_memory=False)
        ann_ke_df = pd.read_excel(annotation_directory + target_ce + '/' + target_ce + '_events.xlsx')
    elif ke == 'arg':
        graph_g_ke_df = pd.read_csv(graph_g_directory + target_ce + '/' + target_ce + '_GraphG_arg.csv',
                                    low_memory=False)
        ann_ke_df = pd.read_excel(annotation_directory + target_ce + '/' + target_ce + '_arguments.xlsx')
    else:
        sys.exit('KE error: ' + ke)

    return graph_g_ke_df, ann_ke_df


def compute_ev_unit_stats(graph_g_directory: str, target_ce: str, annotation_directory: str,
                          sys_ev_arg_df: pd.DataFrame, ta1_schema_indices: dict) -> Tuple[dict, dict]:
    ev_df_dict = {}
    ev_stats_dict = {}

    # read graph G and annotation ev files
    gev_df, aev_df = read_ke_reference(graph_g_directory, annotation_directory, target_ce, 'ev')
    gev_df = add_significance_to_graph_g_ev(gev_df, aev_df)
    gev_crt_df = gev_df.loc[gev_df['significance'] == 'critical']
    aev_crt_df = aev_df.loc[aev_df['significance'] == 'critical']
    # get ta2 graph G ev, ta1 graph G ev, and ta1 annotation ev
    sys_valid_ev_df = get_valid_ev(sys_ev_arg_df, ta1_schema_indices)
    ta2_gev_df = get_matching_ta2_gev(sys_valid_ev_df, gev_df)
    ev_df_dict['ta2_gev_df'] = ta2_gev_df
    ta1_gev_sst_df = get_matching_ta1_ev(sys_valid_ev_df, gev_df, 'subsubtype')
    ev_df_dict['ta1_gev_sst_df'] = ta1_gev_sst_df
    ta1_gev_sst_crt_df = get_matching_ta1_ev(sys_valid_ev_df, gev_crt_df, 'subsubtype')
    ta1_gev_st_df = get_matching_ta1_ev(sys_valid_ev_df, gev_df, 'subtype')
    ev_df_dict['ta1_gev_st_df'] = ta1_gev_st_df
    ta1_gev_st_crt_df = get_matching_ta1_ev(sys_valid_ev_df, gev_crt_df, 'subtype')
    ta1_gev_t_df = get_matching_ta1_ev(sys_valid_ev_df, gev_df, 'type')
    ev_df_dict['ta1_gev_t_df'] = ta1_gev_t_df
    ta1_gev_t_crt_df = get_matching_ta1_ev(sys_valid_ev_df, gev_crt_df, 'type')
    ta1_aev_sst_df = get_matching_ta1_ev(sys_valid_ev_df, aev_df, 'subsubtype')
    ev_df_dict['ta1_aev_sst_df'] = ta1_aev_sst_df
    ta1_aev_sst_crt_df = get_matching_ta1_ev(sys_valid_ev_df, aev_crt_df, 'subsubtype')
    ta1_aev_st_df = get_matching_ta1_ev(sys_valid_ev_df, aev_df, 'subtype')
    ev_df_dict['ta1_aev_st_df'] = ta1_aev_st_df
    ta1_aev_st_crt_df = get_matching_ta1_ev(sys_valid_ev_df, aev_crt_df, 'subtype')
    ta1_aev_t_df = get_matching_ta1_ev(sys_valid_ev_df, aev_df, 'type')
    ev_df_dict['ta1_aev_t_df'] = ta1_aev_t_df
    ta1_aev_t_crt_df = get_matching_ta1_ev(sys_valid_ev_df, aev_crt_df, 'type')

    # count ev stats
    gev_all_count = len(gev_df)
    ev_stats_dict['gev_all_count'] = gev_all_count
    gev_crt_count = len(gev_df.loc[gev_df['significance'] == 'critical'])
    ev_stats_dict['gev_crt_count'] = gev_crt_count
    gev_opt_count = len(gev_df.loc[gev_df['significance'] == 'optional'])
    ev_stats_dict['gev_opt_count'] = gev_opt_count
    aev_all_count = len(aev_df)
    ev_stats_dict['aev_all_count'] = aev_all_count
    aev_crt_count = len(aev_crt_df)
    ev_stats_dict['aev_crt_count'] = aev_crt_count

    sys_ev_df = copy.deepcopy(sys_ev_arg_df)
    sys_ev_df = sys_ev_df.drop_duplicates(subset=['schema_id', 'ev_id'])
    sys_ev_all_count = len(sys_ev_df)
    ev_stats_dict['sys_ev_all_count'] = sys_ev_all_count
    # count ta2 ev stats
    ta2_gev_all_count = len(ta2_gev_df.ref_ev_id.unique())
    ev_stats_dict['ta2_gev_all_count'] = ta2_gev_all_count

    ta2_gev_crt_df = ta2_gev_df.loc[
        ta2_gev_df['significance'] == 'critical']
    ta2_gev_crt_count = len(ta2_gev_crt_df.ref_ev_id.unique())
    ev_stats_dict['ta2_gev_crt_count'] = ta2_gev_crt_count

    ta2_gev_opt_df = ta2_gev_df.loc[
        ta2_gev_df['significance'] == 'optional']
    ta2_gev_opt_count = len(ta2_gev_opt_df.ref_ev_id.unique())
    ev_stats_dict['ta2_gev_opt_count'] = ta2_gev_opt_count
    # count ta1 gev stats
    ta1_gev_sst_all_count = count_distinct_ke(ta1_gev_sst_df, 'ev')
    ev_stats_dict['ta1_gev_sst_all_count'] = ta1_gev_sst_all_count
    if ta1_gev_sst_all_count < ta2_gev_all_count:
        print('ta1_gev_sst_all_count < ta2_gev_all_count')

    ta1_gev_sst_crt_count = count_distinct_ke(ta1_gev_sst_crt_df, 'ev')
    ev_stats_dict['ta1_gev_sst_crt_count'] = ta1_gev_sst_crt_count
    if ta1_gev_sst_crt_count < ta2_gev_crt_count:
        print('ta1_gev_sst_crt_count < ta2_gev_crt_count')

    ta1_gev_st_all_count = count_distinct_ke(ta1_gev_st_df, 'ev')
    ev_stats_dict['ta1_gev_st_all_count'] = ta1_gev_st_all_count
    if ta1_gev_st_all_count < ta1_gev_sst_all_count:
        print('ta1_gev_st_all_count < ta1_gev_sst_all_count')

    ta1_gev_st_crt_count = count_distinct_ke(ta1_gev_st_crt_df, 'ev')
    ev_stats_dict['ta1_gev_st_crt_count'] = ta1_gev_st_crt_count
    if ta1_gev_st_crt_count < ta1_gev_sst_crt_count:
        print('ta1_gev_st_crt_count < ta1_gev_sst_crt_count')

    ta1_gev_t_all_count = count_distinct_ke(ta1_gev_t_df, 'ev')
    ev_stats_dict['ta1_gev_t_all_count'] = ta1_gev_t_all_count
    if ta1_gev_t_all_count < ta1_gev_st_all_count:
        print('ta1_gev_t_all_count < ta1_gev_st_all_count')

    ta1_gev_t_crt_count = count_distinct_ke(ta1_gev_t_crt_df, 'ev')
    ev_stats_dict['ta1_gev_t_crt_count'] = ta1_gev_t_crt_count
    if ta1_gev_t_crt_count < ta1_gev_st_crt_count:
        print('ta1_gev_t_crt_count < ta1_gev_st_crt_count')

    # count ta1 aev stats
    ta1_aev_sst_all_count = count_distinct_ke(ta1_aev_sst_df, 'ev')
    ev_stats_dict['ta1_aev_sst_all_count'] = ta1_aev_sst_all_count
    if ta1_aev_sst_all_count < ta1_gev_sst_all_count:
        print('ta1_aev_sst_all_count < ta1_gev_sst_all_count')

    ta1_aev_sst_crt_count = count_distinct_ke(ta1_aev_sst_crt_df, 'ev')
    ev_stats_dict['ta1_aev_sst_crt_count'] = ta1_aev_sst_crt_count
    if ta1_aev_sst_crt_count < ta1_gev_sst_crt_count:
        print('ta1_aev_sst_crt_count < ta1_gev_sst_crt_count')

    ta1_aev_st_all_count = count_distinct_ke(ta1_aev_st_df, 'ev')
    ev_stats_dict['ta1_aev_st_all_count'] = ta1_aev_st_all_count
    if ta1_aev_st_all_count < ta1_gev_st_all_count:
        print('ta1_aev_st_all_count < ta1_gev_st_all_count')
    if ta1_aev_st_all_count < ta1_aev_sst_all_count:
        print('ta1_aev_st_all_count < ta1_aev_sst_all_count')

    ta1_aev_st_crt_count = count_distinct_ke(ta1_aev_st_crt_df, 'ev')
    ev_stats_dict['ta1_aev_st_crt_count'] = ta1_aev_st_crt_count
    if ta1_aev_st_crt_count < ta1_gev_st_crt_count:
        print('ta1_aev_st_crt_count < ta1_gev_st_crt_count')
    if ta1_aev_st_all_count < ta1_aev_sst_all_count:
        print('ta1_aev_st_crt_count < ta1_aev_sst_crt_count')

    ta1_aev_t_all_count = count_distinct_ke(ta1_aev_t_df, 'ev')
    ev_stats_dict['ta1_aev_t_all_count'] = ta1_aev_t_all_count
    if ta1_aev_t_all_count < ta1_gev_t_all_count:
        print('ta1_aev_t_all_count < ta1_gev_t_all_count')
    if ta1_aev_t_all_count < ta1_aev_st_all_count:
        print('ta1_aev_t_all_count < ta1_aev_st_all_count')

    ta1_aev_t_crt_count = count_distinct_ke(ta1_aev_t_crt_df, 'ev')
    ev_stats_dict['ta1_aev_t_crt_count'] = ta1_aev_t_crt_count
    if ta1_aev_t_crt_count < ta1_gev_t_crt_count:
        print('ta1_aev_t_crt_count < ta1_gev_t_crt_count')
    if ta1_aev_t_crt_count < ta1_aev_st_crt_count:
        print('ta1_aev_t_crt_count < ta1_aev_st_crt_count')

    return ev_df_dict, ev_stats_dict


def compute_arg_unit_stats(graph_g_directory: str, target_ce: str, annotation_directory: str,
                           hot_fix_set: set, ta1_schema_indices: dict, sys_ev_arg_df: pd.DataFrame,
                           ev_df_dict: dict) -> dict:
    arg_stats_dict = {}

    # read graph G and annotation arg files
    garg_df, aarg_df = read_ke_reference(graph_g_directory, annotation_directory, target_ce, 'arg')
    aarg_df = aarg_df.loc[aarg_df['eventprimitive_id'] != 'EMPTY_NA']
    garg_count = len(garg_df)
    arg_stats_dict['garg_count'] = garg_count
    aarg_count = len(aarg_df)
    arg_stats_dict['aarg_count'] = aarg_count
    # get ta2 graph G arg, ta1 graph G arg, and ta1 annotation arg
    ta2_gev_ta2_garg_df = get_matching_ta2_garg(ev_df_dict['ta2_gev_df'], garg_df)
    ta2_gev_ta2_garg_df = get_valid_arg(ta2_gev_ta2_garg_df, ta1_schema_indices)
    # get ta1 garg and aarg under type match depth subsubtype
    ta1_gev_sst_ta1_garg_df = get_matching_ta1_arg(ev_df_dict['ta1_gev_sst_df'], garg_df, hot_fix_set)
    ta1_gev_sst_ta1_garg_df = get_valid_arg(ta1_gev_sst_ta1_garg_df, ta1_schema_indices)
    ta1_aev_sst_ta1_aarg_df = get_matching_ta1_arg(ev_df_dict['ta1_aev_sst_df'], aarg_df, hot_fix_set)
    ta1_aev_sst_ta1_aarg_df = get_valid_arg(ta1_aev_sst_ta1_aarg_df, ta1_schema_indices)
    # get ta1 garg and aarg under type match depth subtype
    ta1_gev_st_ta1_garg_df = get_matching_ta1_arg(ev_df_dict['ta1_gev_st_df'], garg_df, hot_fix_set)
    ta1_gev_st_ta1_garg_df = get_valid_arg(ta1_gev_st_ta1_garg_df, ta1_schema_indices)
    ta1_aev_st_ta1_aarg_df = get_matching_ta1_arg(ev_df_dict['ta1_aev_st_df'], aarg_df, hot_fix_set)
    ta1_aev_st_ta1_aarg_df = get_valid_arg(ta1_aev_st_ta1_aarg_df, ta1_schema_indices)
    # get ta1 garg and aarg under type match depth type
    ta1_gev_t_ta1_garg_df = get_matching_ta1_arg(ev_df_dict['ta1_gev_t_df'], garg_df, hot_fix_set)
    ta1_gev_t_ta1_garg_df = get_valid_arg(ta1_gev_t_ta1_garg_df, ta1_schema_indices)
    ta1_aev_t_ta1_aarg_df = get_matching_ta1_arg(ev_df_dict['ta1_aev_t_df'], aarg_df, hot_fix_set)
    ta1_aev_t_ta1_aarg_df = get_valid_arg(ta1_aev_t_ta1_aarg_df, ta1_schema_indices)

    # count arg elements
    sys_arg_all_df = sys_ev_arg_df.loc[sys_ev_arg_df['arg_id'] != 'empty']
    sys_arg_all_count = len(sys_arg_all_df)
    arg_stats_dict['sys_arg_all_count'] = sys_arg_all_count
    ta2_gev_ta2_garg_count = len(ta2_gev_ta2_garg_df.val_provenance.unique())
    arg_stats_dict['ta2_gev_ta2_garg_count'] = ta2_gev_ta2_garg_count
    # count ta1 garg stats
    ta1_gev_sst_ta1_garg_count = count_distinct_ke(ta1_gev_sst_ta1_garg_df, 'arg')
    if ta1_gev_sst_ta1_garg_count < ta2_gev_ta2_garg_count:
        ta1_gev_sst_ta1_garg_count = ta2_gev_ta2_garg_count
    arg_stats_dict['ta1_gev_sst_ta1_garg_count'] = ta1_gev_sst_ta1_garg_count

    ta1_gev_st_ta1_garg_count = count_distinct_ke(ta1_gev_st_ta1_garg_df, 'arg')
    if ta1_gev_st_ta1_garg_count < ta1_gev_sst_ta1_garg_count:
        ta1_gev_st_ta1_garg_count = ta1_gev_sst_ta1_garg_count
    arg_stats_dict['ta1_gev_st_ta1_garg_count'] = ta1_gev_st_ta1_garg_count

    ta1_gev_t_ta1_garg_count = count_distinct_ke(ta1_gev_t_ta1_garg_df, 'arg')
    if ta1_gev_t_ta1_garg_count < ta1_gev_st_ta1_garg_count:
        ta1_gev_t_ta1_garg_count = ta1_gev_st_ta1_garg_count
    arg_stats_dict['ta1_gev_t_ta1_garg_count'] = ta1_gev_t_ta1_garg_count
    # count ta1 aarg stats
    ta1_aev_sst_ta1_aarg_count = count_distinct_ke(ta1_aev_sst_ta1_aarg_df, 'arg')
    if ta1_aev_sst_ta1_aarg_count < ta1_gev_sst_ta1_garg_count:
        ta1_aev_sst_ta1_aarg_count = ta1_gev_sst_ta1_garg_count
    arg_stats_dict['ta1_aev_sst_ta1_aarg_count'] = ta1_aev_sst_ta1_aarg_count

    ta1_aev_st_ta1_aarg_count = count_distinct_ke(ta1_aev_st_ta1_aarg_df, 'arg')
    if ta1_aev_st_ta1_aarg_count < ta1_gev_st_ta1_garg_count:
        ta1_aev_st_ta1_aarg_count = ta1_gev_st_ta1_garg_count
    if ta1_aev_st_ta1_aarg_count < ta1_aev_sst_ta1_aarg_count:
        ta1_aev_st_ta1_aarg_count = ta1_aev_sst_ta1_aarg_count
    arg_stats_dict['ta1_aev_st_ta1_aarg_count'] = ta1_aev_st_ta1_aarg_count

    ta1_aev_t_ta1_aarg_count = count_distinct_ke(ta1_aev_t_ta1_aarg_df, 'arg')
    if ta1_aev_t_ta1_aarg_count < ta1_gev_t_ta1_garg_count:
        ta1_aev_t_ta1_aarg_count = ta1_gev_t_ta1_garg_count
    if ta1_aev_t_ta1_aarg_count < ta1_aev_st_ta1_aarg_count:
        ta1_aev_t_ta1_aarg_count = ta1_aev_st_ta1_aarg_count
    arg_stats_dict['ta1_aev_t_ta1_aarg_count'] = ta1_aev_t_ta1_aarg_count

    return arg_stats_dict


def compute_ev_arg_unit_stats(base: str, ta1: str, ta2: str, annotation_directory: str, graph_g_directory: str,
                              target_ce: str, sys_ev_arg_df: pd.DataFrame, stats_df: pd.DataFrame,
                              hot_fix_set: set, ta1_schema_indices: dict) -> pd.DataFrame:
    # compute file ev score
    ev_df_dict, ev_stats_dict = compute_ev_unit_stats(graph_g_directory, target_ce, annotation_directory,
                                                      sys_ev_arg_df, ta1_schema_indices)

    # compute file arg score
    arg_stats_dict = compute_arg_unit_stats(graph_g_directory, target_ce, annotation_directory, hot_fix_set,
                                            ta1_schema_indices, sys_ev_arg_df, ev_df_dict)

    if base == 'file':
        # add a row to file score
        file_name = sys_ev_arg_df.iloc[0]['file_name']
        score_row = {'file_name': file_name, 'CE': target_ce, 'TA2': ta2, 'TA1': ta1,
                     'file_ev_all_count': ev_stats_dict['sys_ev_all_count'],
                     'file_ta2_gev_all_count': ev_stats_dict['ta2_gev_all_count'],
                     'file_ta2_gev_crt_count': ev_stats_dict['ta2_gev_crt_count'],
                     'file_ta2_gev_opt_count': ev_stats_dict['ta2_gev_opt_count'],
                     'file_ta1_gev_sst_all_count': ev_stats_dict['ta1_gev_sst_all_count'],
                     'file_ta1_gev_sst_crt_count': ev_stats_dict['ta1_gev_sst_crt_count'],
                     'file_ta1_gev_st_all_count': ev_stats_dict['ta1_gev_st_all_count'],
                     'file_ta1_gev_st_crt_count': ev_stats_dict['ta1_gev_st_crt_count'],
                     'file_ta1_gev_t_all_count': ev_stats_dict['ta1_gev_t_all_count'],
                     'file_ta1_gev_t_crt_count': ev_stats_dict['ta1_gev_t_crt_count'],
                     'file_ta1_aev_sst_all_count': ev_stats_dict['ta1_aev_sst_all_count'],
                     'file_ta1_aev_sst_crt_count': ev_stats_dict['ta1_aev_sst_crt_count'],
                     'file_ta1_aev_st_all_count': ev_stats_dict['ta1_aev_st_all_count'],
                     'file_ta1_aev_st_crt_count': ev_stats_dict['ta1_aev_st_crt_count'],
                     'file_ta1_aev_t_all_count': ev_stats_dict['ta1_aev_t_all_count'],
                     'file_ta1_aev_t_crt_count': ev_stats_dict['ta1_aev_t_crt_count'],
                     'gev_all_count': ev_stats_dict['gev_all_count'], 'gev_crt_count': ev_stats_dict['gev_crt_count'],
                     'gev_opt_count': ev_stats_dict['gev_opt_count'],
                     'aev_all_count': ev_stats_dict['aev_all_count'], 'aev_crt_count': ev_stats_dict['aev_crt_count'],
                     'file_arg_all_count': arg_stats_dict['sys_arg_all_count'],
                     'file_ta2_gev_ta2_garg_count': arg_stats_dict['ta2_gev_ta2_garg_count'],
                     'file_ta1_gev_sst_ta1_garg_count': arg_stats_dict['ta1_gev_sst_ta1_garg_count'],
                     'file_ta1_gev_st_ta1_garg_count': arg_stats_dict['ta1_gev_st_ta1_garg_count'],
                     'file_ta1_gev_t_ta1_garg_count': arg_stats_dict['ta1_gev_t_ta1_garg_count'],
                     'file_ta1_aev_sst_ta1_aarg_count': arg_stats_dict['ta1_aev_sst_ta1_aarg_count'],
                     'file_ta1_aev_st_ta1_aarg_count': arg_stats_dict['ta1_aev_st_ta1_aarg_count'],
                     'file_ta1_aev_t_ta1_aarg_count': arg_stats_dict['ta1_aev_t_ta1_aarg_count'],
                     'garg_count': arg_stats_dict['garg_count'], 'aarg_count': arg_stats_dict['aarg_count']}
        stats_df = stats_df.append(score_row, ignore_index=True)
    elif base == 'schema':
        # add a row to schema score
        file_name = sys_ev_arg_df.iloc[0]['file_name']
        schema_id = sys_ev_arg_df.iloc[0]['schema_id']
        schema_super = sys_ev_arg_df.iloc[0]['schema_super']
        score_row = {'file_name': file_name, 'schema_id': schema_id, 'schema_super': schema_super,
                     'CE': target_ce, 'TA2': ta2, 'TA1': ta1,
                     'schema_ev_all_count': ev_stats_dict['sys_ev_all_count'],
                     'schema_ta2_gev_all_count': ev_stats_dict['ta2_gev_all_count'],
                     'schema_ta2_gev_crt_count': ev_stats_dict['ta2_gev_crt_count'],
                     'schema_ta2_gev_opt_count': ev_stats_dict['ta2_gev_opt_count'],
                     'schema_ta1_gev_sst_all_count': ev_stats_dict['ta1_gev_sst_all_count'],
                     'schema_ta1_gev_sst_crt_count': ev_stats_dict['ta1_gev_sst_crt_count'],
                     'schema_ta1_gev_st_all_count': ev_stats_dict['ta1_gev_st_all_count'],
                     'schema_ta1_gev_st_crt_count': ev_stats_dict['ta1_gev_st_crt_count'],
                     'schema_ta1_gev_t_all_count': ev_stats_dict['ta1_gev_t_all_count'],
                     'schema_ta1_gev_t_crt_count': ev_stats_dict['ta1_gev_t_crt_count'],
                     'schema_ta1_aev_sst_all_count': ev_stats_dict['ta1_aev_sst_all_count'],
                     'schema_ta1_aev_sst_crt_count': ev_stats_dict['ta1_aev_sst_crt_count'],
                     'schema_ta1_aev_st_all_count': ev_stats_dict['ta1_aev_st_all_count'],
                     'schema_ta1_aev_st_crt_count': ev_stats_dict['ta1_aev_st_crt_count'],
                     'schema_ta1_aev_t_all_count': ev_stats_dict['ta1_aev_t_all_count'],
                     'schema_ta1_aev_t_crt_count': ev_stats_dict['ta1_aev_t_crt_count'],
                     'gev_all_count': ev_stats_dict['gev_all_count'], 'gev_crt_count': ev_stats_dict['gev_crt_count'],
                     'gev_opt_count': ev_stats_dict['gev_opt_count'],
                     'aev_all_count': ev_stats_dict['aev_all_count'], 'aev_crt_count': ev_stats_dict['aev_crt_count'],
                     'schema_arg_all_count': arg_stats_dict['sys_arg_all_count'],
                     'schema_ta2_gev_ta2_garg_count': arg_stats_dict['ta2_gev_ta2_garg_count'],
                     'schema_ta1_gev_sst_ta1_garg_count': arg_stats_dict['ta1_gev_sst_ta1_garg_count'],
                     'schema_ta1_gev_st_ta1_garg_count': arg_stats_dict['ta1_gev_st_ta1_garg_count'],
                     'schema_ta1_gev_t_ta1_garg_count': arg_stats_dict['ta1_gev_t_ta1_garg_count'],
                     'schema_ta1_aev_sst_ta1_aarg_count': arg_stats_dict['ta1_aev_sst_ta1_aarg_count'],
                     'schema_ta1_aev_st_ta1_aarg_count': arg_stats_dict['ta1_aev_st_ta1_aarg_count'],
                     'schema_ta1_aev_t_ta1_aarg_count': arg_stats_dict['ta1_aev_t_ta1_aarg_count'],
                     'garg_count': arg_stats_dict['garg_count'], 'aarg_count': arg_stats_dict['aarg_count']}
        stats_df = stats_df.append(score_row, ignore_index=True)

    return stats_df


def compute_schema_ev_arg_stats(file_ev_arg_df: pd.DataFrame, ta1: str, ta2: str,
                                annotation_directory: str, graph_g_directory: str,
                                target_ce: str, schema_stats_df: pd.DataFrame, hot_fix_set: set,
                                ta1_schema_indices: dict) -> pd.DataFrame:
    schema_list = file_ev_arg_df.schema_id.unique()
    if len(schema_list) > 0:
        grouped = file_ev_arg_df.groupby(file_ev_arg_df.schema_id)
        for schema in schema_list:
            schema_ev_arg_df = grouped.get_group(schema)
            schema_stats_df = compute_ev_arg_unit_stats('schema', ta1, ta2, annotation_directory,
                                                        graph_g_directory, target_ce, schema_ev_arg_df,
                                                        schema_stats_df, hot_fix_set, ta1_schema_indices)

    return schema_stats_df

    # generate hot-fix for graph G argument type error


def create_hot_fix() -> set:
    hot_fix_set = set()
    hot_fix_set.add('VP1005.1006;participant')
    hot_fix_set.add('VP1006.1035;explosivedevice')
    hot_fix_set.add('VP1006.1035;target')
    hot_fix_set.add('VP1007.1007;participant')
    hot_fix_set.add('VP1008.1037;target')
    hot_fix_set.add('VP1008.1043;target')
    hot_fix_set.add('VP1009.1014;passengerartifact')
    hot_fix_set.add('VP1009.1020;observedentity')
    hot_fix_set.add('VP1009.1045;participant')
    hot_fix_set.add('VP1010.1007;attacker')
    hot_fix_set.add('VP1011.1002;target')
    hot_fix_set.add('VP1013.1008;passengerartifact')
    hot_fix_set.add('VP1013.1040;participant')
    hot_fix_set.add('VP1013.1051;observedentity')
    hot_fix_set.add('VP1020.1017;target')

    return hot_fix_set


def get_ev_arg_stats(ta2_team_name: str, score_directory: str, annotation_directory: str,
                     graph_g_directory: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # initiate file and schema based stats
    file_stats_df, schema_stats_df = initiate_file_schema_score_df()

    ta1_schema_indices = compute_ta1_schema_indices(score_directory)

    ev_arg_directory = score_directory + ta2_team_name + '/Event_arguments/'
    for file_name in tqdm(os.listdir(ev_arg_directory), leave=False):
        # only consider .csv file (ignoring system hidden files like .DS_store)
        fn_surfix = file_name[-4:]
        if fn_surfix != '.csv':
            continue
        # get CE and TA2 info
        fn_list = file_name.split('-')
        # if target ce is not specified, skip this file
        ta1 = fn_list[0]
        ta2 = fn_list[1]
        target_ce = fn_list[2]
        task2 = False
        if 'task2' in file_name:
            task2 = True
        if not task2:
            continue

        # compute file score
        file_ev_arg_df = pd.read_csv(ev_arg_directory + file_name, low_memory=False)
        hot_fix_set = create_hot_fix()
        # compute file ev and arg score
        file_stats_df = compute_ev_arg_unit_stats('file', ta1, ta2, annotation_directory, graph_g_directory,
                                                  target_ce, file_ev_arg_df, file_stats_df,
                                                  hot_fix_set, ta1_schema_indices)

        # compute schema ev and arg score
        schema_stats_df = compute_schema_ev_arg_stats(file_ev_arg_df, ta1, ta2, annotation_directory,
                                                      graph_g_directory, target_ce, schema_stats_df,
                                                      hot_fix_set, ta1_schema_indices)

    return file_stats_df, schema_stats_df


def add_recall_columns(input_score_df: pd.DataFrame, base_index: int) -> pd.DataFrame:
    # add ta1 gev recall
    input_score_df.insert(base_index, 'recall_ta1_gev_all', None)
    base_index += 1
    input_score_df.insert(base_index, 'recall_ta1_gev_crt', None)
    base_index += 1
    input_score_df.insert(base_index, 'recall_ta1_gev_opt', None)
    base_index += 1
    # add ta1 aev sst recall
    input_score_df.insert(base_index, 'recall_ta1_aev_sst_all', None)
    base_index += 1
    input_score_df.insert(base_index, 'recall_ta1_aev_sst_crt', None)
    base_index += 1
    # add ta1 aev st recall
    input_score_df.insert(base_index, 'recall_ta1_aev_st_all', None)
    base_index += 1
    input_score_df.insert(base_index, 'recall_ta1_aev_st_crt', None)
    base_index += 1
    # add ta1 aev t recall
    input_score_df.insert(base_index, 'recall_ta1_aev_t_all', None)
    base_index += 1
    input_score_df.insert(base_index, 'recall_ta1_aev_t_crt', None)
    base_index += 1

    # add ta1 gev ta1 garg recall
    input_score_df.insert(base_index, 'recall_ta1_gev_ta1_garg', None)
    base_index += 1
    # add ta1 aarg reacll
    input_score_df.insert(base_index, 'recall_ta1_aev_sst_ta1_aarg', None)
    base_index += 1
    input_score_df.insert(base_index, 'recall_ta1_aev_st_ta1_aarg', None)
    base_index += 1
    input_score_df.insert(base_index, 'recall_ta1_aev_t_ta1_aarg', None)

    return input_score_df


def compute_recall_score(input_score_df: pd.DataFrame, base_unit: str) -> pd.DataFrame:
    for row in input_score_df.itertuples():
        if base_unit == 'file':
            input_ta2_gev_all_count = row.file_ta2_gev_all_count
            input_ta2_gev_crt_count = row.file_ta2_gev_crt_count
            input_ta2_gev_opt_count = row.file_ta2_gev_opt_count
            input_ta1_aev_sst_all_count = row.file_ta1_aev_sst_all_count
            input_ta1_aev_sst_crt_count = row.file_ta1_aev_sst_crt_count
            input_ta1_aev_st_all_count = row.file_ta1_aev_st_all_count
            input_ta1_aev_st_crt_count = row.file_ta1_aev_st_crt_count
            input_ta1_aev_t_all_count = row.file_ta1_aev_t_all_count
            input_ta1_aev_t_crt_count = row.file_ta1_aev_t_crt_count
            input_ta2_gev_ta2_garg_count = row.file_ta2_gev_ta2_garg_count
            input_ta1_aev_sst_ta1_aarg_count = row.file_ta1_aev_sst_ta1_aarg_count
            input_ta1_aev_st_ta1_aarg_count = row.file_ta1_aev_st_ta1_aarg_count
            input_ta1_aev_t_ta1_aarg_count = row.file_ta1_aev_t_ta1_aarg_count
        else:
            input_ta2_gev_all_count = row.schema_ta2_gev_all_count
            input_ta2_gev_crt_count = row.schema_ta2_gev_crt_count
            input_ta2_gev_opt_count = row.schema_ta2_gev_opt_count
            input_ta1_aev_sst_all_count = row.schema_ta1_aev_sst_all_count
            input_ta1_aev_sst_crt_count = row.schema_ta1_aev_sst_crt_count
            input_ta1_aev_st_all_count = row.schema_ta1_aev_st_all_count
            input_ta1_aev_st_crt_count = row.schema_ta1_aev_st_crt_count
            input_ta1_aev_t_all_count = row.schema_ta1_aev_t_all_count
            input_ta1_aev_t_crt_count = row.schema_ta1_aev_t_crt_count
            input_ta2_gev_ta2_garg_count = row.schema_ta2_gev_ta2_garg_count
            input_ta1_aev_sst_ta1_aarg_count = row.schema_ta1_aev_sst_ta1_aarg_count
            input_ta1_aev_st_ta1_aarg_count = row.schema_ta1_aev_st_ta1_aarg_count
            input_ta1_aev_t_ta1_aarg_count = row.schema_ta1_aev_t_ta1_aarg_count

        gev_all_count = row.gev_all_count
        gev_crt_count = row.gev_crt_count
        gev_opt_count = row.gev_opt_count
        aev_all_count = row.aev_all_count
        aev_crt_count = row.aev_crt_count
        garg_count = row.garg_count
        aarg_count = row.aarg_count

        ## compute event recall
        # compute ta1 gev recall
        if gev_all_count > 0:
            recall_ta1_gev_all = input_ta2_gev_all_count / gev_all_count
            input_score_df.at[row[0], 'recall_ta1_gev_all'] = recall_ta1_gev_all
            if recall_ta1_gev_all > 1:
                print('recall_ta1_gev_all > 1')
        if gev_crt_count > 0:
            recall_ta1_gev_crt = input_ta2_gev_crt_count / gev_crt_count
            input_score_df.at[row[0], 'recall_ta1_gev_crt'] = recall_ta1_gev_crt
            if recall_ta1_gev_crt > 1:
                print('recall_ta1_gev_crt > 1')
        if gev_opt_count > 0:
            recall_ta1_gev_opt = input_ta2_gev_opt_count / gev_opt_count
            input_score_df.at[row[0], 'recall_ta1_gev_opt'] = recall_ta1_gev_opt
            if recall_ta1_gev_opt > 1:
                print('recall_ta1_gev_opt > 1')
        # compute ta1 aev sst recall
        if aev_all_count > 0:
            recall_ta1_aev_sst_all = input_ta1_aev_sst_all_count / aev_all_count
            input_score_df.at[row[0], 'recall_ta1_aev_sst_all'] = recall_ta1_aev_sst_all
            if recall_ta1_aev_sst_all > 1:
                print('recall_ta1_aev_sst_all > 1')
            recall_ta1_aev_sst_crt = input_ta1_aev_sst_crt_count / aev_crt_count
            input_score_df.at[row[0], 'recall_ta1_aev_sst_crt'] = recall_ta1_aev_sst_crt
            if recall_ta1_aev_sst_crt > 1:
                print('recall_ta1_aev_sst_crt > 1')
            # compute ta1 aev st recall
            recall_ta1_aev_st_all = input_ta1_aev_st_all_count / aev_all_count
            input_score_df.at[row[0], 'recall_ta1_aev_st_all'] = recall_ta1_aev_st_all
            if recall_ta1_aev_st_all > 1:
                print('recall_ta1_aev_st_all > 1')
            recall_ta1_aev_st_crt = input_ta1_aev_st_crt_count / aev_crt_count
            input_score_df.at[row[0], 'recall_ta1_aev_st_crt'] = recall_ta1_aev_st_crt
            if recall_ta1_aev_st_crt > 1:
                print('recall_ta1_aev_st_crt > 1')
            # compute ta1 aev t recall
            recall_ta1_aev_t_all = input_ta1_aev_t_all_count / aev_all_count
            input_score_df.at[row[0], 'recall_ta1_aev_t_all'] = recall_ta1_aev_t_all
            if recall_ta1_aev_t_all > 1:
                print('recall_ta1_aev_t_all > 1')
            recall_ta1_aev_t_crt = input_ta1_aev_t_crt_count / aev_crt_count
            input_score_df.at[row[0], 'recall_ta1_aev_t_crt'] = recall_ta1_aev_t_crt
            if recall_ta1_aev_t_crt > 1:
                print('recall_ta1_aev_t_crt > 1')

        ## compute argument recall
        # compute ta1 garg recall
        if garg_count > 0:
            recall_ta1_gev_ta1_garg = input_ta2_gev_ta2_garg_count / garg_count
            input_score_df.at[row[0], 'recall_ta1_gev_ta1_garg'] = recall_ta1_gev_ta1_garg
        # compute ta1 aarg recall
        if aarg_count > 0:
            recall_ta1_aev_sst_ta1_aarg = input_ta1_aev_sst_ta1_aarg_count / aarg_count
            input_score_df.at[row[0], 'recall_ta1_aev_sst_ta1_aarg'] = recall_ta1_aev_sst_ta1_aarg
            recall_ta1_aev_st_ta1_aarg = input_ta1_aev_st_ta1_aarg_count / aarg_count
            input_score_df.at[row[0], 'recall_ta1_aev_st_ta1_aarg'] = recall_ta1_aev_st_ta1_aarg
            recall_ta1_aev_t_ta1_aarg = input_ta1_aev_t_ta1_aarg_count / aarg_count
            input_score_df.at[row[0], 'recall_ta1_aev_t_ta1_aarg'] = recall_ta1_aev_t_ta1_aarg

    return input_score_df


def score_ev_arg_recall(file_stats_df: pd.DataFrame, schema_stats_df: pd.DataFrame) \
        -> Tuple[pd.DataFrame, pd.DataFrame]:
    file_score_df = copy.deepcopy(file_stats_df)
    schema_score_df = copy.deepcopy(schema_stats_df)
    # compute file based recall
    file_score_df = add_recall_columns(file_score_df, 4)
    file_score_df = compute_recall_score(file_score_df, 'file')

    # compute schema based recall
    schema_score_df = add_recall_columns(schema_score_df, 5)
    schema_score_df = compute_recall_score(schema_score_df, 'schema')

    return file_score_df, schema_score_df


def add_diagnosis_columns(input_diagnosis_df: pd.DataFrame, base_index: int) -> pd.DataFrame:
    # add ta2 gev sst recall
    input_diagnosis_df.insert(base_index, 'recall_ta2_gev_sst_all', None)
    base_index += 1
    input_diagnosis_df.insert(base_index, 'recall_ta2_gev_sst_crt', None)
    base_index += 1
    # add ta2 gev st recall
    input_diagnosis_df.insert(base_index, 'recall_ta2_gev_st_all', None)
    base_index += 1
    input_diagnosis_df.insert(base_index, 'recall_ta2_gev_st_crt', None)
    base_index += 1
    # add ta2 gev t recall
    input_diagnosis_df.insert(base_index, 'recall_ta2_gev_t_all', None)
    base_index += 1
    input_diagnosis_df.insert(base_index, 'recall_ta2_gev_t_crt', None)
    base_index += 1
    # add ta1 gev sst recall
    input_diagnosis_df.insert(base_index, 'recall_ta1_gev_sst_all', None)
    base_index += 1
    input_diagnosis_df.insert(base_index, 'recall_ta1_gev_sst_crt', None)
    base_index += 1
    # add ta1 gev st recall
    input_diagnosis_df.insert(base_index, 'recall_ta1_gev_st_all', None)
    base_index += 1
    input_diagnosis_df.insert(base_index, 'recall_ta1_gev_st_crt', None)
    base_index += 1
    # add ta1 gev t recall
    input_diagnosis_df.insert(base_index, 'recall_ta1_gev_t_all', None)
    base_index += 1
    input_diagnosis_df.insert(base_index, 'recall_ta1_gev_t_crt', None)
    base_index += 1
    # add ta2 garg recall
    input_diagnosis_df.insert(base_index, 'recall_ta2_gev_ta2_garg', None)
    base_index += 1
    input_diagnosis_df.insert(base_index, 'recall_ta1_gev_sst_ta2_garg', None)
    base_index += 1
    input_diagnosis_df.insert(base_index, 'recall_ta1_gev_st_ta2_garg', None)
    base_index += 1
    input_diagnosis_df.insert(base_index, 'recall_ta1_gev_t_ta2_garg', None)
    base_index += 1
    # add ta1 ev ta1 garg recall
    input_diagnosis_df.insert(base_index, 'recall_ta1_gev_sst_ta1_garg', None)
    base_index += 1
    input_diagnosis_df.insert(base_index, 'recall_ta1_gev_st_ta1_garg', None)
    base_index += 1
    input_diagnosis_df.insert(base_index, 'recall_ta1_gev_t_ta1_garg', None)
    base_index += 1

    return input_diagnosis_df


def compute_recall_diagnosis(input_diagnosis_df: pd.DataFrame, base_unit: str) -> pd.DataFrame:
    for row in input_diagnosis_df.itertuples():
        if base_unit == 'file':
            input_ta2_gev_all_count = row.file_ta2_gev_all_count
            input_ta2_gev_crt_count = row.file_ta2_gev_crt_count
            input_ta2_gev_opt_count = row.file_ta2_gev_opt_count
            input_ta1_gev_sst_all_count = row.file_ta1_gev_sst_all_count
            input_ta1_gev_sst_crt_count = row.file_ta1_gev_sst_crt_count
            input_ta1_gev_st_all_count = row.file_ta1_gev_st_all_count
            input_ta1_gev_st_crt_count = row.file_ta1_gev_st_crt_count
            input_ta1_gev_t_all_count = row.file_ta1_gev_t_all_count
            input_ta1_gev_t_crt_count = row.file_ta1_gev_t_crt_count
            input_ta2_gev_ta2_garg_count = row.file_ta2_gev_ta2_garg_count
            input_ta1_gev_sst_ta1_garg_count = row.file_ta1_gev_sst_ta1_garg_count
            input_ta1_gev_st_ta1_garg_count = row.file_ta1_gev_st_ta1_garg_count
            input_ta1_gev_t_ta1_garg_count = row.file_ta1_gev_t_ta1_garg_count
        else:
            input_ta2_gev_all_count = row.schema_ta2_gev_all_count
            input_ta2_gev_crt_count = row.schema_ta2_gev_crt_count
            input_ta2_gev_opt_count = row.schema_ta2_gev_opt_count
            input_ta1_gev_sst_all_count = row.schema_ta1_gev_sst_all_count
            input_ta1_gev_sst_crt_count = row.schema_ta1_gev_sst_crt_count
            input_ta1_gev_st_all_count = row.schema_ta1_gev_st_all_count
            input_ta1_gev_st_crt_count = row.schema_ta1_gev_st_crt_count
            input_ta1_gev_t_all_count = row.schema_ta1_gev_t_all_count
            input_ta1_gev_t_crt_count = row.schema_ta1_gev_t_crt_count
            input_ta2_gev_ta2_garg_count = row.schema_ta2_gev_ta2_garg_count
            input_ta1_gev_sst_ta1_garg_count = row.schema_ta1_gev_sst_ta1_garg_count
            input_ta1_gev_st_ta1_garg_count = row.schema_ta1_gev_st_ta1_garg_count
            input_ta1_gev_t_ta1_garg_count = row.schema_ta1_gev_t_ta1_garg_count

        gev_all_count = row.gev_all_count
        gev_crt_count = row.gev_crt_count
        gev_opt_count = row.gev_opt_count
        garg_count = row.garg_count

        #### compute recall
        ## compute event recall
        # compute ta2 event sst recall
        if input_ta1_gev_sst_all_count > 0:
            recall_ta2_gev_sst_all = input_ta2_gev_all_count / input_ta1_gev_sst_all_count
            input_diagnosis_df.at[row[0], 'recall_ta2_gev_sst_all'] = recall_ta2_gev_sst_all
            if recall_ta2_gev_sst_all > 1:
                print('recall_ta2_gev_sst_all > 1')
            recall_ta2_gev_sst_crt = input_ta2_gev_crt_count / input_ta1_gev_sst_crt_count
            input_diagnosis_df.at[row[0], 'recall_ta2_gev_sst_crt'] = recall_ta2_gev_sst_crt
            if recall_ta2_gev_sst_crt > 1:
                print('recall_ta2_gev_sst_crt > 1')
        # compute ta2 event st recall
        if input_ta1_gev_st_all_count > 0:
            recall_ta2_gev_st_all = input_ta2_gev_all_count / input_ta1_gev_st_all_count
            input_diagnosis_df.at[row[0], 'recall_ta2_gev_st_all'] = recall_ta2_gev_st_all
            if recall_ta2_gev_st_all > 1:
                print('recall_ta2_gev_st_all > 1')
            recall_ta2_gev_st_crt = input_ta2_gev_crt_count / input_ta1_gev_st_crt_count
            input_diagnosis_df.at[row[0], 'recall_ta2_gev_st_crt'] = recall_ta2_gev_st_crt
            if recall_ta2_gev_st_crt > 1:
                print('recall_ta2_gev_st_crt > 1')
        # compute ta2 event t recall
        if input_ta1_gev_t_all_count > 0:
            recall_ta2_gev_t_all = input_ta2_gev_all_count / input_ta1_gev_t_all_count
            input_diagnosis_df.at[row[0], 'recall_ta2_gev_t_all'] = recall_ta2_gev_t_all
            if recall_ta2_gev_t_all > 1:
                print('recall_ta2_gev_t_all > 1')
            recall_ta2_gev_t_crt = input_ta2_gev_crt_count / input_ta1_gev_t_crt_count
            input_diagnosis_df.at[row[0], 'recall_ta2_gev_t_crt'] = recall_ta2_gev_t_crt
            if recall_ta2_gev_t_crt > 1:
                print('recall_ta2_gev_t_crt > 1')
        # compute ta1 event sst recall
        if gev_all_count > 0:
            recall_ta1_gev_sst_all = input_ta1_gev_sst_all_count / gev_all_count
            input_diagnosis_df.at[row[0], 'recall_ta1_gev_sst_all'] = recall_ta1_gev_sst_all
            if recall_ta1_gev_sst_all > 1:
                print('recall_ta1_gev_sst_all > 1')
            recall_ta1_gev_sst_crt = input_ta1_gev_sst_crt_count / gev_crt_count
            input_diagnosis_df.at[row[0], 'recall_ta1_gev_sst_crt'] = recall_ta1_gev_sst_crt
            if recall_ta1_gev_sst_crt > 1:
                print('recall_ta1_gev_sst_crt > 1')
        # compute ta1 event st recall
        if gev_all_count > 0:
            recall_ta1_gev_st_all = input_ta1_gev_st_all_count / gev_all_count
            input_diagnosis_df.at[row[0], 'recall_ta1_gev_st_all'] = recall_ta1_gev_st_all
            if recall_ta1_gev_st_all > 1:
                print('recall_ta1_gev_st_all > 1')
            recall_ta1_gev_st_crt = input_ta1_gev_st_crt_count / gev_crt_count
            input_diagnosis_df.at[row[0], 'recall_ta1_gev_st_crt'] = recall_ta1_gev_st_crt
            if recall_ta1_gev_st_crt > 1:
                print('recall_ta1_gev_st_crt > 1')
        # compute ta1 event t recall
        if gev_all_count > 0:
            recall_ta1_gev_t_all = input_ta1_gev_t_all_count / gev_all_count
            input_diagnosis_df.at[row[0], 'recall_ta1_gev_t_all'] = recall_ta1_gev_t_all
            if recall_ta1_gev_t_all > 1:
                print('recall_ta1_gev_t_all > 1')
            recall_ta1_gev_t_crt = input_ta1_gev_t_crt_count / gev_crt_count
            input_diagnosis_df.at[row[0], 'recall_ta1_gev_t_crt'] = recall_ta1_gev_t_crt
            if recall_ta1_gev_t_crt > 1:
                print('recall_ta1_gev_t_crt > 1')
        ## compute argument recall
        if input_ta1_gev_sst_ta1_garg_count > 0:
            recall_ta1_gev_sst_ta2_garg = input_ta2_gev_ta2_garg_count / input_ta1_gev_sst_ta1_garg_count
            input_diagnosis_df.at[row[0], 'recall_ta1_gev_sst_ta2_garg'] = recall_ta1_gev_sst_ta2_garg
            if recall_ta1_gev_sst_ta2_garg > 1:
                print('recall_ta1_gev_sst_ta2_garg > 1')
        if input_ta1_gev_st_ta1_garg_count > 0:
            recall_ta1_gev_st_ta2_garg = input_ta2_gev_ta2_garg_count / input_ta1_gev_st_ta1_garg_count
            input_diagnosis_df.at[row[0], 'recall_ta1_gev_st_ta2_garg'] = recall_ta1_gev_st_ta2_garg
            if recall_ta1_gev_st_ta2_garg > 1:
                print('recall_ta1_gev_st_ta2_garg > 1')
        if input_ta1_gev_t_ta1_garg_count > 0:
            recall_ta1_gev_t_ta2_garg = input_ta2_gev_ta2_garg_count / input_ta1_gev_t_ta1_garg_count
            input_diagnosis_df.at[row[0], 'recall_ta1_gev_t_ta2_garg'] = recall_ta1_gev_t_ta2_garg
            if recall_ta1_gev_t_ta2_garg > 1:
                print('recall_ta1_gev_t_ta2_garg > 1')
        if garg_count > 0:
            recall_ta1_gev_sst_ta1_garg = input_ta1_gev_sst_ta1_garg_count / garg_count
            input_diagnosis_df.at[row[0], 'recall_ta1_gev_sst_ta1_garg'] = recall_ta1_gev_sst_ta1_garg
            if recall_ta1_gev_sst_ta1_garg > 1:
                print('recall_ta1_gev_sst_ta1_garg > 1')
        if garg_count > 0:
            recall_ta1_gev_st_ta1_garg = input_ta1_gev_st_ta1_garg_count / garg_count
            input_diagnosis_df.at[row[0], 'recall_ta1_gev_st_ta1_garg'] = recall_ta1_gev_st_ta1_garg
            if recall_ta1_gev_st_ta1_garg > 1:
                print('recall_ta1_gev_st_ta1_garg > 1')
        if garg_count > 0:
            recall_ta1_gev_t_ta1_garg = input_ta1_gev_t_ta1_garg_count / garg_count
            input_diagnosis_df.at[row[0], 'recall_ta1_gev_t_ta1_garg'] = recall_ta1_gev_t_ta1_garg
            if recall_ta1_gev_t_ta1_garg > 1:
                print('recall_ta1_gev_t_ta1_garg > 1')

    return input_diagnosis_df


def write_to_files(task2_score_directory: str, ta2_team_name: str, file_score_df: pd.DataFrame,
                   schema_score_df: pd.DataFrame, file_diagnosis_df: pd.DataFrame,
                   schema_diagnosis_df: pd.DataFrame) -> None:
    ta2_score_directory = task2_score_directory + ta2_team_name + '/'
    if not os.path.isdir(ta2_score_directory):
        os.makedirs(ta2_score_directory)

    if len(file_score_df) == 0:
        print('No TA2 task2 score of event and argument from ' + ta2_team_name)
    else:
        file_score_df.to_excel(ta2_score_directory +
                               'task2_file_score_event_argument_' + ta2_team_name + '.xlsx', index=False)
        schema_score_df.to_excel(ta2_score_directory +
                                 'task2_schema_score_event_argument_' + ta2_team_name + '.xlsx', index=False)

    if len(file_diagnosis_df) == 0:
        print('No TA2 task2 diagnosis of event and argument from ' + ta2_team_name)
    else:
        ta2_diagnosis_directory = ta2_score_directory + 'Diagnosis/'
        if not os.path.isdir(ta2_diagnosis_directory):
            os.makedirs(ta2_diagnosis_directory)
        file_diagnosis_df.to_excel(ta2_diagnosis_directory + 'task2_file_diagnosis_event_argument_' +
                                   ta2_team_name + '.xlsx', index=False)
        schema_diagnosis_df.to_excel(ta2_diagnosis_directory + 'task2_schema_diagnosis_event_argument_' +
                                     ta2_team_name + '.xlsx', index=False)


def diagnose_ev_arg_recall(file_stats_df: pd.DataFrame, schema_stats_df: pd.DataFrame) \
        -> Tuple[pd.DataFrame, pd.DataFrame]:
    file_diagnosis_df = copy.deepcopy(file_stats_df)
    schema_diagnosis_df = copy.deepcopy(schema_stats_df)
    # compute file based recall
    file_diagnosis_df = add_diagnosis_columns(file_diagnosis_df, 4)
    file_diagnosis_df = compute_recall_diagnosis(file_diagnosis_df, 'file')

    # compute schema based recall
    schema_diagnosis_df = add_diagnosis_columns(schema_diagnosis_df, 5)
    schema_diagnosis_df = compute_recall_diagnosis(schema_diagnosis_df, 'schema')

    return file_diagnosis_df, schema_diagnosis_df


def score(ta2_team_name: str, graph_g_ext: bool, ta1_ext: bool, ta2_ext: bool, mode: str) -> None:
    # if there is no change in graph g/ta1/ta2 outputs and its extraction is already conducted,
    # set graph_g_ext/ta1_ext/ta2_ext as False to reduce the run time
    print('Graph G extraction is set as ' + str(graph_g_ext))
    print('TA1 output extraction is set as ' + str(ta1_ext))
    print('TA2 output extraction is set as ' + str(ta2_ext))

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
        ta2_output_directory = config['Phase1Eval']['ta2_task2_output_directory']
        ta1_output_directory = config['Phase1Eval']['ta1_output_directory']
        task2_score_directory = config['Phase1Eval']['task2_score_directory']
        annotation_directory = config['Phase1Eval']['annotation_directory']
        graph_g_directory = config['Phase1Eval']['graph_g_directory']
    elif mode == 'test':
        ta2_output_directory = config['Test']['ta2_task2_output_directory']
        ta1_output_directory = config['Test']['ta1_output_directory']
        task2_score_directory = config['Test']['task2_score_directory']
        annotation_directory = config['Test']['annotation_directory']
        graph_g_directory = config['Test']['graph_g_directory']
    else:
        sys.exit('Please enter the correct mode: evaluation or test')

    if not os.path.isdir(ta2_output_directory):
        sys.exit('Directory not found: ' + ta2_output_directory)
    if not os.path.isdir(ta1_output_directory):
        sys.exit('Directory not found: ' + ta1_output_directory)
    if not os.path.isdir(annotation_directory):
        sys.exit('Directory not found: ' + annotation_directory)
    if not os.path.isdir(graph_g_directory):
        sys.exit('Directory not found ' + graph_g_directory)
    if not os.path.exists(task2_score_directory):
        os.makedirs(task2_score_directory)

    # extract graph g ev and arg from json outputs
    if graph_g_ext:
        print('---------------------------------------------------------------------')
        print('Start to extract ev and arg from graph G json outputs...')
        time.sleep(0.1)
        extract_event_argument.extract_graph_g_ev_arg_from_json(graph_g_directory)

        print('Extraction completed!')
    else:
        print('---------------------------------------------------------------------')
        print('Skip ev and arg extraction from graph G.')

    # extract ta1 ev and arg from json-ld outputs
    if ta1_ext:
        print('---------------------------------------------------------------------')
        print('Start to extract ev and arg from TA1 json outputs...')
        for ta1 in os.listdir(ta1_output_directory):
            if not os.path.isdir(ta1_output_directory + ta1 + '/'):
                continue
            print('TA1_' + ta1 + ' ...')
            time.sleep(0.1)
            extract_event_argument.extract_ta1_ev_arg_from_json(ta1, ta1_output_directory, task2_score_directory)
        print('Extraction completed!')
    else:
        print('---------------------------------------------------------------------')
        print('Skip ev and arg extraction from TA1 schema libraries.')

    # extract ta2 ev and arg from json-ld outputs
    if ta2_ext:
        print('---------------------------------------------------------------------')
        print('Start to extract ev and arg from TA2_' + ta2_team_name + ' json outputs...')
        time.sleep(0.1)
        extract_event_argument.extract_ta2_ev_arg_from_json(ta2_team_name, ta2_output_directory,
                                                            task2_score_directory, [], annotation_directory)
        print('Extraction completed!')
    else:
        print('---------------------------------------------------------------------')
        print('Skip ev and arg extraction from TA2 outputs.')

    # compute ta2 event and argument score
    print('---------------------------------------------------------------------')
    print('Start to score ' + ta2_team_name + ' task 2 ev and arg outputs...')
    time.sleep(0.1)
    file_stats_df, schema_stats_df = get_ev_arg_stats(ta2_team_name, task2_score_directory, annotation_directory,
                                                      graph_g_directory)
    file_score_df, schema_score_df = score_ev_arg_recall(file_stats_df, schema_stats_df)
    file_diagnosis_df, schema_diagnosis_df = diagnose_ev_arg_recall(file_stats_df, schema_stats_df)

    print('Scoring finished!')

    ## write to excel files
    write_to_files(task2_score_directory, ta2_team_name, file_score_df, schema_score_df,
                   file_diagnosis_df, schema_diagnosis_df)


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


# Command Line Interface
def cli_parser():
    parser = argparse.ArgumentParser(
        description='Task2 scorer for events and arguments'
    )

    parser.add_argument('-t', '--ta2_team_name', help='supported ta2 team names are: CMU, IBM, JHU,'
                                                      'RESIN_PRIMARY, RESIN_RELAXED_ALL, and RESIN_RELAXED_ATTACK',
                        required=True, type=str)
    parser.add_argument('-gge', '--graph_g_ext', help='whether to conduct graph g extraction',
                        required=False, type=str2bool, default=True)
    parser.add_argument('-t1e', '--ta1_ext', help='whether to conduct ta1 output extraction',
                        required=False, type=str2bool, default=True)
    parser.add_argument('-t2e', '--ta2_ext', help='whether to conduct ta2 output extraction',
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


if __name__ == '__main__':
    # cli_parser()

    graph_g_ext = False
    ta1_ext = False
    ta2_ext = False
    mode = 'evaluation'

    ta2_team_name = 'jhu'
    score(ta2_team_name, graph_g_ext, ta1_ext, ta2_ext, mode)
