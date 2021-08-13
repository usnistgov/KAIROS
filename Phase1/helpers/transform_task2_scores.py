#!/usr/bin/env python3

__author__ = "Xiongnan Jin (xiongnan.jin@nist.gov)"
__version__ = "Development: 0.5.1"
__date__ = "02/25/2021"

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
# transform the task 2 scores xlsx files to fit for visualization
######################################################################################
import configparser
import glob
import os
import shutil
import sys
import pandas as pd


def split_into_ta1_based_scores(ke_score_df: pd.DataFrame, ta1_based_analysis_directory: str, ta1: str, ke: str):
    if len(ke_score_df) > 0:
        ta1_based_ke_score_df = ke_score_df.loc[ke_score_df['TA1'] == ta1.lower()]
        if len(ta1_based_ke_score_df) > 0:
            ta1_based_ke_score_df = ta1_based_ke_score_df.drop(columns='TA1')
            file_path = os.path.join(ta1_based_analysis_directory, 'TA1_' + ta1 + '_' + ke + '.xlsx')
            ta1_based_ke_score_df.to_excel(file_path, index=False)


def split_into_ta2_based_scores(ke_score_df: pd.DataFrame, ta2_based_analysis_directory: str, ta2: str, ke: str):
    if len(ke_score_df) > 0:
        ta2_based_ke_score_df = ke_score_df.loc[ke_score_df['TA2'] == ta2.lower()]
        if len(ta2_based_ke_score_df) > 0:
            ta2_based_ke_score_df = ta2_based_ke_score_df.drop(columns='TA2')
            file_path = os.path.join(ta2_based_analysis_directory, 'TA2_' + ta2 + '_' + ke + '.xlsx')
            ta2_based_ke_score_df.to_excel(file_path, index=False)


def add_task1_schema_ke_score(task1_score_directory: str, ta2: str, ke_score_df: pd.DataFrame, ke: str) -> pd.DataFrame:
    fnf_message = 'File not found: '
    excel_suffix = '.xlsx'
    ta2_schema_score_fn = ''
    if ke == 'ev':
        ta2_schema_score_fn = task1_score_directory + ta2 + '/task1_schema_score_event_' + ta2 + excel_suffix
    elif ke == 'arg':
        ta2_schema_score_fn = task1_score_directory + ta2 + '/task1_schema_score_argument_' + ta2 + excel_suffix
    elif ke == 'order':
        ta2_schema_score_fn = task1_score_directory + ta2 + '/task1_schema_score_order_' + ta2 + excel_suffix
    elif ke == 'rel':
        ta2_schema_score_fn = task1_score_directory + ta2 + '/task1_schema_score_relation_' + ta2 + excel_suffix
    elif ke == 'ent':
        ta2_schema_score_fn = task1_score_directory + ta2 + '/task1_schema_score_relation_entity_' + ta2 + excel_suffix
    else:
        sys.exit('KE error: ' + ke)

    if not os.path.isfile(ta2_schema_score_fn):
        print(fnf_message + ta2_schema_score_fn)
    else:
        ta2_schema_score_df = pd.read_excel(ta2_schema_score_fn)
        ke_score_df = ke_score_df.append(ta2_schema_score_df)

    return ke_score_df


def add_task2_schema_ke_score(task2_score_directory: str, ta2: str, ke_score_df: pd.DataFrame, ke: str) -> pd.DataFrame:
    fnf_message = 'File not found: '
    excel_suffix = '.xlsx'
    ta2_schema_score_fn = ''
    if ke == 'ev_arg':
        ta2_schema_score_fn = task2_score_directory + ta2 + '/task2_schema_score_event_argument_' + ta2 + excel_suffix
    elif ke == 'order':
        ta2_schema_score_fn = task2_score_directory + ta2 + '/task2_schema_score_order_' + ta2 + excel_suffix
    elif ke == 'rel':
        ta2_schema_score_fn = task2_score_directory + ta2 + '/task2_schema_score_relation_' + ta2 + excel_suffix
    else:
        sys.exit('KE error: ' + ke)

    if not os.path.isfile(ta2_schema_score_fn):
        print(fnf_message + ta2_schema_score_fn)
    else:
        ta2_schema_score_df = pd.read_excel(ta2_schema_score_fn)
        ke_score_df = ke_score_df.append(ta2_schema_score_df)

    return ke_score_df


def check_input_directory(score_directory: str) -> None:
    if not os.path.isdir(score_directory):
        sys.exit('Directory not found: ' + score_directory)


def check_output_directory(score_analysis_directory: str) -> None:
    if not os.path.isdir(score_analysis_directory):
        os.makedirs(score_analysis_directory)
    else:
        objects = glob.glob(score_analysis_directory + '*')
        for obj in objects:
            if os.path.isdir(obj):
                shutil.rmtree(obj)
            elif os.path.isfile(obj):
                os.remove(obj)


def compute_task1_schema_score(ta2_task1_team_names: list, task1_score_directory: str):
    task1_schema_score_dict = {}
    # read each ta2 team's scores and merge
    ev_score_df = arg_score_df = order_score_df = rel_score_df = ent_score_df = pd.DataFrame()
    for ta2 in ta2_task1_team_names:
        # add ta2_ev_score if exists
        ev_score_df = add_task1_schema_ke_score(task1_score_directory, ta2, ev_score_df, 'ev')
        # add ta2_arg_score if exists
        arg_score_df = add_task1_schema_ke_score(task1_score_directory, ta2, arg_score_df, 'arg')
        # add ta2_order_score if exists
        order_score_df = add_task1_schema_ke_score(task1_score_directory, ta2, order_score_df, 'order')
        # add ta2_rel_score if exists
        rel_score_df = add_task1_schema_ke_score(task1_score_directory, ta2, rel_score_df, 'rel')
        # add ta2_ent_score if exists
        ent_score_df = add_task1_schema_ke_score(task1_score_directory, ta2, ent_score_df, 'ent')

    task1_schema_score_dict['ev_score_df'] = ev_score_df
    task1_schema_score_dict['arg_score_df'] = arg_score_df
    task1_schema_score_dict['order_score_df'] = order_score_df
    task1_schema_score_dict['rel_score_df'] = rel_score_df
    task1_schema_score_dict['ent_score_df'] = ent_score_df

    return task1_schema_score_dict


def compute_task2_schema_score(ta2_task2_team_names: list, task2_score_directory: str):
    task2_schema_score_dict = {}
    # read each ta2 team's scores and merge
    ev_arg_score_df = order_score_df = rel_score_df = pd.DataFrame()
    for ta2 in ta2_task2_team_names:
        # add ta2_ev_arg_score if exists
        ev_arg_score_df = add_task2_schema_ke_score(task2_score_directory, ta2, ev_arg_score_df, 'ev_arg')
        # add ta2_order_score if exists
        order_score_df = add_task2_schema_ke_score(task2_score_directory, ta2, order_score_df, 'order')
        # add ta2_rel_score if exists
        rel_score_df = add_task2_schema_ke_score(task2_score_directory, ta2, rel_score_df, 'rel')

    task2_schema_score_dict['ev_arg_score_df'] = ev_arg_score_df
    task2_schema_score_dict['order_score_df'] = order_score_df
    task2_schema_score_dict['rel_score_df'] = rel_score_df

    return task2_schema_score_dict


def split_task1_results_into_ta2_based_scores(task1_schema_score_dict: dict, ta2_task1_team_names: list,
                                              task1_score_analysis_directory: str) -> None:
    # split into 6 ta2-based scores
    for ta2 in ta2_task1_team_names:
        ta2_based_analysis_directory = os.path.join(task1_score_analysis_directory[:-1], 'TA2_' + ta2)
        if not os.path.isdir(ta2_based_analysis_directory):
            os.makedirs(ta2_based_analysis_directory)

        split_into_ta2_based_scores(task1_schema_score_dict['ev_score_df'],
                                    ta2_based_analysis_directory, ta2, 'ev')
        split_into_ta2_based_scores(task1_schema_score_dict['arg_score_df'],
                                    ta2_based_analysis_directory, ta2, 'arg')
        split_into_ta2_based_scores(task1_schema_score_dict['order_score_df'],
                                    ta2_based_analysis_directory, ta2, 'order')
        split_into_ta2_based_scores(task1_schema_score_dict['rel_score_df'],
                                    ta2_based_analysis_directory, ta2, 'rel')
        split_into_ta2_based_scores(task1_schema_score_dict['ent_score_df'],
                                    ta2_based_analysis_directory, ta2, 'ent')


def split_task2_results_into_ta1_based_scores(task2_schema_score_dict: dict, ta1_team_names: list,
                                              task2_score_analysis_directory: str) -> None:
    # split into 6 ta1-based scores
    for ta1 in ta1_team_names:
        ta1_based_analysis_directory = os.path.join(task2_score_analysis_directory[:-1], 'TA1_' + ta1)
        if not os.path.isdir(ta1_based_analysis_directory):
            os.makedirs(ta1_based_analysis_directory)

        split_into_ta1_based_scores(task2_schema_score_dict['ev_arg_score_df'],
                                    ta1_based_analysis_directory, ta1, 'ev_arg')
        split_into_ta1_based_scores(task2_schema_score_dict['order_score_df'],
                                    ta1_based_analysis_directory, ta1, 'order')
        split_into_ta1_based_scores(task2_schema_score_dict['rel_score_df'],
                                    ta1_based_analysis_directory, ta1, 'rel')


def compute_task1_schema_ce_max_score(task1_schema_score_dict: dict, task1_score_analysis_directory: str) -> dict:
    task1_schema_ce_max_score_dict = {}

    ## group by TA1 and TA2, take maximum value
    max_ev_score_precision_df = task1_schema_score_dict['ev_score_df'].groupby(
        ['TA1', 'TA2', 'target_ce'], as_index=False)['precision'].max()
    task1_schema_ce_max_score_dict['max_ev_score_precision_df'] = max_ev_score_precision_df

    max_ev_score_precision_woer_df = task1_schema_score_dict['ev_score_df'].groupby(
        ['TA1', 'TA2', 'target_ce'], as_index=False)['precision_woer'].max()
    task1_schema_ce_max_score_dict['max_ev_score_precision_woer_df'] = max_ev_score_precision_woer_df

    max_ev_score_precision_at_top_20_df = task1_schema_score_dict['ev_score_df'].groupby(
        ['TA1', 'TA2', 'target_ce'], as_index=False)['precision@20'].max()
    task1_schema_ce_max_score_dict['max_ev_score_precision_at_top_20_df'] = max_ev_score_precision_at_top_20_df

    max_ev_score_precision_at_top_20_woer_df = task1_schema_score_dict['ev_score_df'].groupby(
        ['TA1', 'TA2', 'target_ce'], as_index=False)['precision@20_woer'].max()
    task1_schema_ce_max_score_dict[
        'max_ev_score_precision_at_top_20_woer_df'] = max_ev_score_precision_at_top_20_woer_df

    # save to xlsx files
    task1_score_analysis_schema_max_directory = task1_score_analysis_directory + 'Schema_max/'
    if not os.path.isdir(task1_score_analysis_schema_max_directory):
        os.makedirs(task1_score_analysis_schema_max_directory)

    max_ev_score_precision_df.to_excel(
        task1_score_analysis_schema_max_directory + 'schema_max_ev_precision.xlsx', index=False)
    max_ev_score_precision_woer_df.to_excel(
        task1_score_analysis_schema_max_directory + 'schema_max_ev_precision_woer.xlsx', index=False)
    max_ev_score_precision_at_top_20_df.to_excel(
        task1_score_analysis_schema_max_directory + 'schema_max_ev_precision_at_top_20.xlsx', index=False)
    max_ev_score_precision_at_top_20_woer_df.to_excel(
        task1_score_analysis_schema_max_directory + 'schema_max_ev_precision_at_top_20_woer.xlsx', index=False)

    return task1_schema_ce_max_score_dict


def compute_task2_schema_ce_max_score(task2_schema_score_dict: dict, task2_score_analysis_directory: str) -> dict:
    task2_schema_ce_max_score_dict = {}
    ## group by TA1 and TA2, take maximum value
    max_ev_score_ta1_gev_all_df = task2_schema_score_dict['ev_arg_score_df'].groupby(
        ['TA1', 'TA2', 'CE'], as_index=False)['recall_ta1_gev_all'].max()
    task2_schema_ce_max_score_dict['max_ev_score_ta1_gev_all_df'] = max_ev_score_ta1_gev_all_df

    max_ev_score_ta1_gev_crt_df = task2_schema_score_dict['ev_arg_score_df'].groupby(
        ['TA1', 'TA2', 'CE'], as_index=False)['recall_ta1_gev_crt'].max()
    task2_schema_ce_max_score_dict['max_ev_score_ta1_gev_crt_df'] = max_ev_score_ta1_gev_crt_df

    max_ev_score_ta1_aev_sst_all_df = task2_schema_score_dict['ev_arg_score_df'].groupby(
        ['TA1', 'TA2', 'CE'], as_index=False)['recall_ta1_aev_sst_all'].max()
    task2_schema_ce_max_score_dict['max_ev_score_ta1_aev_sst_all_df'] = max_ev_score_ta1_aev_sst_all_df

    max_ev_score_ta1_aev_sst_crt_df = task2_schema_score_dict['ev_arg_score_df'].groupby(
        ['TA1', 'TA2', 'CE'], as_index=False)['recall_ta1_aev_sst_crt'].max()
    task2_schema_ce_max_score_dict['max_ev_score_ta1_aev_sst_crt_df'] = max_ev_score_ta1_aev_sst_crt_df

    max_ev_score_ta1_aev_st_all_df = task2_schema_score_dict['ev_arg_score_df'].groupby(
        ['TA1', 'TA2', 'CE'], as_index=False)['recall_ta1_aev_st_all'].max()
    task2_schema_ce_max_score_dict['max_ev_score_ta1_aev_st_all_df'] = max_ev_score_ta1_aev_st_all_df

    max_ev_score_ta1_aev_st_crt_df = task2_schema_score_dict['ev_arg_score_df'].groupby(
        ['TA1', 'TA2', 'CE'], as_index=False)['recall_ta1_aev_st_crt'].max()
    task2_schema_ce_max_score_dict['max_ev_score_ta1_aev_st_crt_df'] = max_ev_score_ta1_aev_st_crt_df
    # save to xlsx files
    task2_score_analysis_schema_max_directory = task2_score_analysis_directory + 'Schema_max/'
    if not os.path.isdir(task2_score_analysis_schema_max_directory):
        os.makedirs(task2_score_analysis_schema_max_directory)

    max_ev_score_ta1_gev_all_df.to_excel(
        task2_score_analysis_schema_max_directory + 'schema_max_ev_ta1_gev_all.xlsx', index=False)
    max_ev_score_ta1_gev_crt_df.to_excel(
        task2_score_analysis_schema_max_directory + 'schema_max_ev_ta1_gev_crt.xlsx', index=False)
    max_ev_score_ta1_aev_sst_all_df.to_excel(
        task2_score_analysis_schema_max_directory + 'schema_max_ev_ta1_aev_sst_all.xlsx', index=False)
    max_ev_score_ta1_aev_sst_crt_df.to_excel(
        task2_score_analysis_schema_max_directory + 'schema_max_ev_ta1_aev_sst_crt.xlsx', index=False)
    max_ev_score_ta1_aev_st_all_df.to_excel(
        task2_score_analysis_schema_max_directory + 'schema_max_ev_ta1_aev_st_all.xlsx', index=False)
    max_ev_score_ta1_aev_st_crt_df.to_excel(
        task2_score_analysis_schema_max_directory + 'schema_max_ev_ta1_aev_st_crt.xlsx', index=False)

    return task2_schema_ce_max_score_dict


def compute_task1_schema_average_score(task1_schema_ce_max_score_dict: dict,
                                       task1_score_analysis_directory: str) -> None:
    ## group by TA1 and TA2, take mean value
    avg_ev_score_precision_df = task1_schema_ce_max_score_dict['max_ev_score_precision_df'].groupby(
        ['TA1', 'TA2'], as_index=False)['precision'].mean()
    std_ev_score_precision_df = task1_schema_ce_max_score_dict['max_ev_score_precision_df'].groupby(
        ['TA1', 'TA2'], as_index=False)['precision'].std()

    avg_ev_score_precision_woer_df = task1_schema_ce_max_score_dict['max_ev_score_precision_woer_df'].groupby(
        ['TA1', 'TA2'], as_index=False)['precision_woer'].mean()
    std_ev_score_precision_woer_df = task1_schema_ce_max_score_dict['max_ev_score_precision_woer_df'].groupby(
        ['TA1', 'TA2'], as_index=False)['precision_woer'].std()

    avg_ev_score_precision_at_top_20_df = task1_schema_ce_max_score_dict[
        'max_ev_score_precision_at_top_20_df'].groupby(
        ['TA1', 'TA2'], as_index=False)['precision@20'].mean()
    std_ev_score_precision_at_top_20_df = task1_schema_ce_max_score_dict[
        'max_ev_score_precision_at_top_20_df'].groupby(
        ['TA1', 'TA2'], as_index=False)['precision@20'].std()

    avg_ev_score_precision_at_top_20_woer_df = task1_schema_ce_max_score_dict[
        'max_ev_score_precision_at_top_20_woer_df'].groupby(
        ['TA1', 'TA2'], as_index=False)['precision@20_woer'].mean()
    std_ev_score_precision_at_top_20_woer_df = task1_schema_ce_max_score_dict[
        'max_ev_score_precision_at_top_20_woer_df'].groupby(
        ['TA1', 'TA2'], as_index=False)['precision@20_woer'].std()
    # save to xlsx files
    task1_score_analysis_schema_avg_directory = task1_score_analysis_directory + 'Schema_avg/'
    if not os.path.isdir(task1_score_analysis_schema_avg_directory):
        os.makedirs(task1_score_analysis_schema_avg_directory)

    avg_ev_score_precision_df.to_excel(
        task1_score_analysis_schema_avg_directory + 'schema_avg_ev_score_precision.xlsx', index=False)
    std_ev_score_precision_df.to_excel(
        task1_score_analysis_schema_avg_directory + 'schema_std_ev_score_precision.xlsx', index=False)

    avg_ev_score_precision_woer_df.to_excel(
        task1_score_analysis_schema_avg_directory + 'schema_avg_ev_score_precision_woer.xlsx', index=False)
    std_ev_score_precision_woer_df.to_excel(
        task1_score_analysis_schema_avg_directory + 'schema_std_ev_score_precision_woer.xlsx', index=False)

    avg_ev_score_precision_at_top_20_df = \
        avg_ev_score_precision_at_top_20_df.rename(columns={"precision@20": "precision_at_top_20"}, errors="raise")
    avg_ev_score_precision_at_top_20_df.to_excel(
        task1_score_analysis_schema_avg_directory + 'schema_avg_ev_score_precision_at_top_20.xlsx', index=False)
    std_ev_score_precision_at_top_20_df = \
        std_ev_score_precision_at_top_20_df.rename(columns={"precision@20": "precision_at_top_20"}, errors="raise")
    std_ev_score_precision_at_top_20_df.to_excel(
        task1_score_analysis_schema_avg_directory + 'schema_std_ev_score_precision_at_top_20.xlsx', index=False)

    avg_ev_score_precision_at_top_20_woer_df = \
        avg_ev_score_precision_at_top_20_woer_df.rename(columns={"precision@20_woer": "precision_at_top_20_woer"},
                                                        errors="raise")
    avg_ev_score_precision_at_top_20_woer_df.to_excel(
        task1_score_analysis_schema_avg_directory + 'schema_avg_ev_score_precision_at_top_20_woer.xlsx', index=False)
    std_ev_score_precision_at_top_20_woer_df = \
        std_ev_score_precision_at_top_20_woer_df.rename(columns={"precision@20_woer": "precision_at_top_20_woer"},
                                                        errors="raise")
    std_ev_score_precision_at_top_20_woer_df.to_excel(
        task1_score_analysis_schema_avg_directory + 'schema_std_ev_score_precision_at_top_20_woer.xlsx', index=False)


def compute_task2_schema_average_score(task2_schema_ce_max_score_dict: dict,
                                       task2_score_analysis_directory: str) -> None:
    ## group by TA1 and TA2, take mean value
    avg_ev_score_ta1_gev_all_df = task2_schema_ce_max_score_dict['max_ev_score_ta1_gev_all_df'].groupby(
        ['TA1', 'TA2'], as_index=False)['recall_ta1_gev_all'].mean()
    std_ev_score_ta1_gev_all_df = task2_schema_ce_max_score_dict['max_ev_score_ta1_gev_all_df'].groupby(
        ['TA1', 'TA2'], as_index=False)['recall_ta1_gev_all'].std()

    avg_ev_score_ta1_gev_crt_df = task2_schema_ce_max_score_dict['max_ev_score_ta1_gev_crt_df'].groupby(
        ['TA1', 'TA2'], as_index=False)['recall_ta1_gev_crt'].mean()
    std_ev_score_ta1_gev_crt_df = task2_schema_ce_max_score_dict['max_ev_score_ta1_gev_crt_df'].groupby(
        ['TA1', 'TA2'], as_index=False)['recall_ta1_gev_crt'].std()

    avg_ev_score_ta1_aev_sst_all_df = task2_schema_ce_max_score_dict['max_ev_score_ta1_aev_sst_all_df'].groupby(
        ['TA1', 'TA2'], as_index=False)['recall_ta1_aev_sst_all'].mean()
    std_ev_score_ta1_aev_sst_all_df = task2_schema_ce_max_score_dict['max_ev_score_ta1_aev_sst_all_df'].groupby(
        ['TA1', 'TA2'], as_index=False)['recall_ta1_aev_sst_all'].std()

    avg_ev_score_ta1_aev_sst_crt_df = task2_schema_ce_max_score_dict['max_ev_score_ta1_aev_sst_crt_df'].groupby(
        ['TA1', 'TA2'], as_index=False)['recall_ta1_aev_sst_crt'].mean()
    std_ev_score_ta1_aev_sst_crt_df = task2_schema_ce_max_score_dict['max_ev_score_ta1_aev_sst_crt_df'].groupby(
        ['TA1', 'TA2'], as_index=False)['recall_ta1_aev_sst_crt'].std()

    avg_ev_score_ta1_aev_st_all_df = task2_schema_ce_max_score_dict['max_ev_score_ta1_aev_st_all_df'].groupby(
        ['TA1', 'TA2'], as_index=False)['recall_ta1_aev_st_all'].mean()
    std_ev_score_ta1_aev_st_all_df = task2_schema_ce_max_score_dict['max_ev_score_ta1_aev_st_all_df'].groupby(
        ['TA1', 'TA2'], as_index=False)['recall_ta1_aev_st_all'].std()

    avg_ev_score_ta1_aev_st_crt_df = task2_schema_ce_max_score_dict['max_ev_score_ta1_aev_st_crt_df'].groupby(
        ['TA1', 'TA2'], as_index=False)['recall_ta1_aev_st_crt'].mean()
    std_ev_score_ta1_aev_st_crt_df = task2_schema_ce_max_score_dict['max_ev_score_ta1_aev_st_crt_df'].groupby(
        ['TA1', 'TA2'], as_index=False)['recall_ta1_aev_st_crt'].std()

    # save to xlsx files
    task2_score_analysis_schema_avg_directory = task2_score_analysis_directory + 'Schema_avg/'
    if not os.path.isdir(task2_score_analysis_schema_avg_directory):
        os.makedirs(task2_score_analysis_schema_avg_directory)

    avg_ev_score_ta1_gev_all_df.to_excel(
        task2_score_analysis_schema_avg_directory + 'schema_avg_ev_ta1_gev_all.xlsx', index=False)
    std_ev_score_ta1_gev_all_df.to_excel(
        task2_score_analysis_schema_avg_directory + 'schema_std_ev_ta1_gev_all.xlsx', index=False)

    avg_ev_score_ta1_gev_crt_df.to_excel(
        task2_score_analysis_schema_avg_directory + 'schema_avg_ev_ta1_gev_crt.xlsx', index=False)
    std_ev_score_ta1_gev_crt_df.to_excel(
        task2_score_analysis_schema_avg_directory + 'schema_std_ev_ta1_gev_crt.xlsx', index=False)

    avg_ev_score_ta1_aev_sst_all_df.to_excel(
        task2_score_analysis_schema_avg_directory + 'schema_avg_ev_ta1_aev_sst_all.xlsx', index=False)
    std_ev_score_ta1_aev_sst_all_df.to_excel(
        task2_score_analysis_schema_avg_directory + 'schema_std_ev_ta1_aev_sst_all.xlsx', index=False)

    avg_ev_score_ta1_aev_sst_crt_df.to_excel(
        task2_score_analysis_schema_avg_directory + 'schema_avg_ev_ta1_aev_sst_crt.xlsx', index=False)
    std_ev_score_ta1_aev_sst_crt_df.to_excel(
        task2_score_analysis_schema_avg_directory + 'schema_std_ev_ta1_aev_sst_crt.xlsx', index=False)

    avg_ev_score_ta1_aev_st_all_df.to_excel(
        task2_score_analysis_schema_avg_directory + 'schema_avg_ev_ta1_aev_st_all.xlsx', index=False)
    std_ev_score_ta1_aev_st_all_df.to_excel(
        task2_score_analysis_schema_avg_directory + 'schema_std_ev_ta1_aev_st_all.xlsx', index=False)

    avg_ev_score_ta1_aev_st_crt_df.to_excel(
        task2_score_analysis_schema_avg_directory + 'schema_avg_ev_ta1_aev_st_crt.xlsx', index=False)
    std_ev_score_ta1_aev_st_crt_df.to_excel(
        task2_score_analysis_schema_avg_directory + 'schema_std_ev_ta1_aev_st_crt.xlsx', index=False)


def transform() -> None:
    base_dir = ''
    # get path of directories
    try:
        config = configparser.ConfigParser()
        base_dir = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(base_dir, "../config.ini")) as configfile:
            config.read_file(configfile)
    except:
        sys.exit('CAN NOT OPEN CONFIG FILE: ' + os.path.join(os.path.dirname(base_dir), "../config.ini"))

    task1_score_directory = config['Phase1Eval']['task1_score_directory']
    task1_score_analysis_directory = config['Phase1Eval']['task1_score_analysis_directory']
    task2_score_directory = config['Phase1Eval']['task2_score_directory']
    task2_score_analysis_directory = config['Phase1Eval']['task2_score_analysis_directory']

    check_input_directory(task1_score_directory)
    check_input_directory(task2_score_directory)

    check_output_directory(task1_score_analysis_directory)
    check_output_directory(task2_score_analysis_directory)

    ta2_task1_team_names = ['CMU', 'IBM', 'JHU', 'RESIN']
    ta2_task2_team_names = ['CMU', 'IBM', 'JHU', 'RESIN_PRIMARY']
    ta1_team_names = ['CMU', 'IBM', 'JHU', 'RESIN', 'ISI', 'SBU']

    task1_schema_score_dict = compute_task1_schema_score(ta2_task1_team_names, task1_score_directory)
    task2_schema_score_dict = compute_task2_schema_score(ta2_task2_team_names, task2_score_directory)

    split_task1_results_into_ta2_based_scores(task1_schema_score_dict, ta1_team_names, task1_score_analysis_directory)
    split_task2_results_into_ta1_based_scores(task2_schema_score_dict, ta1_team_names, task2_score_analysis_directory)

    task1_schema_ce_max_score_dict = compute_task1_schema_ce_max_score(
        task1_schema_score_dict, task1_score_analysis_directory)
    task2_schema_ce_max_score_dict = compute_task2_schema_ce_max_score(
        task2_schema_score_dict, task2_score_analysis_directory)

    compute_task1_schema_average_score(task1_schema_ce_max_score_dict, task1_score_analysis_directory)
    compute_task2_schema_average_score(task2_schema_ce_max_score_dict, task2_score_analysis_directory)


if __name__ == '__main__':
    transform()
