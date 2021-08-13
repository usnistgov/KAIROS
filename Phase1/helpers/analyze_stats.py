#!/usr/bin/env python3

__author__ = "Xiongnan Jin (xiongnan.jin@nist.gov)"
__version__ = "Development: 1.0.1"
__date__ = "03/26/2021"

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
# analyze the stats of TA2 outputs, e.g., number of events, arguments,
# relations, those with or without provenance, and number of orders...
######################################################################################
import configparser
import json
import os
import sys
import time

import pandas as pd

from scripts import load_data as load
from tqdm.auto import tqdm

scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../scripts")
sys.path.append(scripts_path)


def get_extracted_ke_directory(score_directory: str, ta2_team_name: str, ke: str) -> str:
    if ke in ['ev', 'arg']:
        extracted_ke_directory = score_directory + ta2_team_name + '/Event_arguments/'
    elif ke == 'order':
        extracted_ke_directory = score_directory + ta2_team_name + '/Orders/'
    elif ke == 'rel':
        extracted_ke_directory = score_directory + ta2_team_name + '/Relations/'
    else:
        sys.exit('KE error: ' + ke)

    if not os.path.isdir(extracted_ke_directory):
        sys.exit('Directory not found: ' + extracted_ke_directory)

    return extracted_ke_directory


def get_ta1_schema_ev_count(ta1: str, schema_super: str, submission_stats_analysis_directory: str) -> int:
    ta1_stats_analysis_directory = submission_stats_analysis_directory[:-10] + 'TA1_schema_libraries/'
    ta1_stats_fp = ta1_stats_analysis_directory + ta1 + '/' + ta1 + '_schema_library_stats.xlsx'
    if not os.path.isfile(ta1_stats_fp):
        sys.exit('File not found: ' + ta1_stats_fp)

    ta1_stats_df = pd.read_excel(ta1_stats_fp)
    ta1_stats_row = ta1_stats_df.loc[ta1_stats_df['schema_id'] == schema_super]
    if len(ta1_stats_row) == 1:
        ta1_schema_ev_count = ta1_stats_row.iloc[0]['ev_count']
    else:
        sys.exit('TA1 schema targeting failure!')

    return ta1_schema_ev_count


def get_single_ta2_schema_ev_stats(schema_df: pd.DataFrame, file_name: str, ta1: str, ta2: str, ce: str,
                                   task2: bool, schema_id: str, schema_super: str, schema_count: int,
                                   submission_stats_analysis_directory: str) -> dict:
    ev_schema_df = schema_df.drop_duplicates(subset=['schema_id', 'ev_id'])
    ev_all_count = len(ev_schema_df)
    ev_wi_arg_df = ev_schema_df.loc[ev_schema_df['arg_id'] != 'empty']
    ev_wo_arg_df = ev_schema_df.loc[ev_schema_df['arg_id'] == 'empty']
    ev_wi_pr_wi_arg_df = ev_wi_arg_df.loc[ev_wi_arg_df['ev_provenance'].notnull()]
    ev_wi_pr_wi_arg_count = len(ev_wi_pr_wi_arg_df)
    ev_wi_pr_wi_arg_wi_ta1ref_df = ev_wi_pr_wi_arg_df.loc[ev_wi_pr_wi_arg_df['ev_ta1ref'].notnull() &
                                                          (ev_wi_pr_wi_arg_df['ev_ta1ref'] != 'kairos:NULL')]
    ev_wi_pr_wi_arg_wi_ta1ref_count = len(ev_wi_pr_wi_arg_wi_ta1ref_df)
    ev_wi_pr_wo_arg_df = ev_wo_arg_df.loc[ev_wo_arg_df['ev_provenance'].notnull()]
    ev_wi_pr_wo_arg_wi_ta1ref_df = ev_wi_pr_wo_arg_df.loc[ev_wi_pr_wo_arg_df['ev_ta1ref'].notnull() &
                                                          (ev_wi_pr_wo_arg_df['ev_ta1ref'] != 'kairos:NULL')]
    ev_wi_pr_wo_arg_wi_ta1ref_count = len(ev_wi_pr_wo_arg_wi_ta1ref_df)
    ev_wi_pr_wo_arg_count = len(ev_wi_pr_wo_arg_df)
    ev_wo_pr_wi_arg_df = ev_wi_arg_df.loc[ev_wi_arg_df['ev_provenance'].isnull()]
    ev_wo_pr_wi_arg_count = len(ev_wo_pr_wi_arg_df)
    ev_wo_pr_wi_arg_wi_ta1ref_df = ev_wo_pr_wi_arg_df.loc[ev_wo_pr_wi_arg_df['ev_ta1ref'].notnull() &
                                                          (ev_wo_pr_wi_arg_df['ev_ta1ref'] != 'kairos:NULL')]
    ev_wo_pr_wi_arg_wi_ta1ref_count = len(ev_wo_pr_wi_arg_wi_ta1ref_df)
    ev_wo_pr_wi_arg_wo_ta1ref_df = ev_wo_pr_wi_arg_df.loc[ev_wo_pr_wi_arg_df['ev_ta1ref'] == 'kairos:NULL']
    ev_wo_pr_wi_arg_wo_ta1ref_count = len(ev_wo_pr_wi_arg_wo_ta1ref_df)
    if ev_wo_pr_wi_arg_count != ev_wo_pr_wi_arg_wi_ta1ref_count + ev_wo_pr_wi_arg_wo_ta1ref_count:
        print('ev_wo_pr_wi_arg_count error!')
    ev_wo_pr_wo_arg_df = ev_wo_arg_df.loc[ev_wo_arg_df['ev_provenance'].isnull()]
    ev_wo_pr_wo_arg_count = len(ev_wo_pr_wo_arg_df)
    ev_wo_pr_wo_arg_wi_ta1ref_df = ev_wo_pr_wo_arg_df.loc[ev_wo_pr_wo_arg_df['ev_ta1ref'].notnull() &
                                                          (ev_wo_pr_wo_arg_df['ev_ta1ref'] != 'kairos:NULL')]
    ev_wo_pr_wo_arg_wi_ta1ref_count = len(ev_wo_pr_wo_arg_wi_ta1ref_df)
    ev_wo_pr_wo_arg_wo_ta1ref_df = ev_wo_pr_wo_arg_df.loc[ev_wo_pr_wo_arg_df['ev_ta1ref'] == 'kairos:NULL']
    ev_wo_pr_wo_arg_wo_ta1ref_count = len(ev_wo_pr_wo_arg_wo_ta1ref_df)
    if ev_wo_pr_wo_arg_count != ev_wo_pr_wo_arg_wi_ta1ref_count + ev_wo_pr_wo_arg_wo_ta1ref_count:
        print('ev_wo_pr_wo_arg_count error!')
    ev_wi_ta1ref_df = ev_schema_df.loc[ev_schema_df['ev_ta1ref'] != 'kairos:NULL']
    ev_wi_ta1ref_count = len(ev_wi_ta1ref_df)
    ta1_schema_ev_count = get_ta1_schema_ev_count(ta1, schema_super, submission_stats_analysis_directory)
    if ev_all_count != (ev_wi_pr_wi_arg_count + ev_wi_pr_wo_arg_count + ev_wo_pr_wi_arg_count + ev_wo_pr_wo_arg_count):
        print('Event count error!')
    stats_row = {'file_name': file_name, 'CE': ce, 'TA2': ta2, 'TA1': ta1, 'task2': task2, 'schema_id': schema_id,
                 'schema_super': schema_super, 'schema_count': schema_count,
                 'ev_all_count': ev_all_count, 'ev_wi_pr_wi_arg_count': ev_wi_pr_wi_arg_count,
                 'ev_wi_pr_wi_arg_wi_ta1ref_count': ev_wi_pr_wi_arg_wi_ta1ref_count,
                 'ev_wi_pr_wo_arg_count': ev_wi_pr_wo_arg_count,
                 'ev_wi_pr_wo_arg_wi_ta1ref_count': ev_wi_pr_wo_arg_wi_ta1ref_count,
                 'ev_wo_pr_wi_arg_count': ev_wo_pr_wi_arg_count,
                 'ev_wo_pr_wi_arg_wi_ta1ref_count': ev_wo_pr_wi_arg_wi_ta1ref_count,
                 'ev_wo_pr_wi_arg_wo_ta1ref_count': ev_wo_pr_wi_arg_wo_ta1ref_count,
                 'ev_wo_pr_wo_arg_count': ev_wo_pr_wo_arg_count,
                 'ev_wo_pr_wo_arg_wi_ta1ref_count': ev_wo_pr_wo_arg_wi_ta1ref_count,
                 'ev_wo_pr_wo_arg_wo_ta1ref_count': ev_wo_pr_wo_arg_wo_ta1ref_count,
                 'ev_wi_ta1ref_count': ev_wi_ta1ref_count,
                 'ta1_schema_ev_count': ta1_schema_ev_count}
    if not task2:
        # count ev provenance type
        ev_wi_pr_df = ev_schema_df.loc[ev_schema_df['ev_provenance'].notnull()]
        ev_pr_text_df = ev_wi_pr_df.loc[ev_wi_pr_df['ev_prov_mediaType'].str.startswith('text', na=False)]
        ev_pr_text_count = len(ev_pr_text_df)
        ev_pr_image_df = ev_wi_pr_df.loc[ev_wi_pr_df['ev_prov_mediaType'].str.startswith('image', na=False)]
        ev_pr_image_count = len(ev_pr_image_df)
        ev_pr_video_df = ev_wi_pr_df.loc[ev_wi_pr_df['ev_prov_mediaType'].str.startswith('video', na=False)]
        ev_pr_video_count = len(ev_pr_video_df)
        ev_pr_eng_df = ev_wi_pr_df.loc[ev_wi_pr_df['ev_prov_language'] == 'English']
        ev_pr_eng_count = len(ev_pr_eng_df)
        ev_pr_spa_df = ev_wi_pr_df.loc[ev_wi_pr_df['ev_prov_language'] == 'Spanish']
        ev_pr_spa_count = len(ev_pr_spa_df)
        stats_row['ev_pr_text_count'] = ev_pr_text_count
        stats_row['ev_pr_image_count'] = ev_pr_image_count
        stats_row['ev_pr_video_count'] = ev_pr_video_count
        stats_row['ev_pr_eng_count'] = ev_pr_eng_count
        stats_row['ev_pr_spa_count'] = ev_pr_spa_count

    return stats_row


def get_single_ta2_schema_arg_stats(schema_df: pd.DataFrame, file_name: str, ta1: str, ta2: str, ce: str,
                                    task2: bool, schema_id: str, schema_super: str, schema_count: int) -> dict:
    arg_schema_df = schema_df.loc[schema_df['arg_id'] != 'empty']
    arg_all_count = len(arg_schema_df)
    arg_wi_pr_df = arg_schema_df.loc[arg_schema_df['arg_provenance'].notnull() &
                                     (arg_schema_df['arg_provenance'] != 'n/a')]
    arg_wi_pr_count = len(arg_wi_pr_df)
    arg_wo_pr_df = arg_schema_df.loc[arg_schema_df['arg_provenance'].isnull() |
                                     (arg_schema_df['arg_provenance'] == 'n/a')]
    arg_wo_pr_count = len(arg_wo_pr_df)
    if arg_all_count != (arg_wi_pr_count + arg_wo_pr_count):
        print('Argument count error!')
    stats_row = {'file_name': file_name, 'CE': ce, 'TA2': ta2, 'TA1': ta1, 'task2': task2, 'schema_id': schema_id,
                 'schema_super': schema_super, 'schema_count': schema_count,
                 'arg_all_count': arg_all_count, 'arg_wi_pr_count': arg_wi_pr_count,
                 'arg_wo_pr_count': arg_wo_pr_count}
    if not task2:
        # count val provenance type
        arg_pr_text_df = arg_wi_pr_df.loc[arg_wi_pr_df['val_prov_mediaType'].str.startswith('text', na=False)]
        arg_pr_text_count = len(arg_pr_text_df)
        arg_pr_image_df = arg_wi_pr_df.loc[arg_wi_pr_df['val_prov_mediaType'].str.startswith('image', na=False)]
        arg_pr_image_count = len(arg_pr_image_df)
        arg_pr_video_df = arg_wi_pr_df.loc[arg_wi_pr_df['val_prov_mediaType'].str.startswith('video', na=False)]
        arg_pr_video_count = len(arg_pr_video_df)
        arg_pr_eng_df = arg_wi_pr_df.loc[arg_wi_pr_df['val_prov_language'] == 'English']
        arg_pr_eng_count = len(arg_pr_eng_df)
        arg_pr_spa_df = arg_wi_pr_df.loc[arg_wi_pr_df['val_prov_language'] == 'Spanish']
        arg_pr_spa_count = len(arg_pr_spa_df)
        stats_row['arg_pr_text_count'] = arg_pr_text_count
        stats_row['arg_pr_image_count'] = arg_pr_image_count
        stats_row['arg_pr_video_count'] = arg_pr_video_count
        stats_row['arg_pr_eng_count'] = arg_pr_eng_count
        stats_row['arg_pr_spa_count'] = arg_pr_spa_count

    return stats_row


def get_single_ta2_schema_rel_stats(schema_df: pd.DataFrame, file_name: str, ta1: str, ta2: str, ce: str,
                                    task2: bool, schema_id: str, schema_super: str, schema_count: int) -> dict:
    rel_all_count = len(schema_df)
    rel_wi_pr_df = schema_df.loc[schema_df['rel_provenance'].notnull() & (schema_df['rel_provenance'] != 'n/a')]
    rel_wi_pr_count = len(rel_wi_pr_df)
    rel_wi_pr_wi_ta1ref_df = rel_wi_pr_df.loc[rel_wi_pr_df['rel_ta1ref'].notnull() &
                                              (rel_wi_pr_df['rel_ta1ref'] != 'kairos:NULL')]
    rel_wi_pr_wi_ta1ref_count = len(rel_wi_pr_wi_ta1ref_df)
    rel_wo_pr_df = schema_df.loc[schema_df['rel_provenance'].isnull() |
                                 (schema_df['rel_provenance'] == 'n/a')]
    rel_wo_pr_count = len(rel_wo_pr_df)
    if rel_all_count != (rel_wi_pr_count + rel_wo_pr_count):
        print('Relation count error!')
    stats_row = {'file_name': file_name, 'CE': ce, 'TA2': ta2, 'TA1': ta1, 'task2': task2, 'schema_id': schema_id,
                 'schema_super': schema_super, 'schema_count': schema_count,
                 'rel_all_count': rel_all_count, 'rel_wi_pr_count': rel_wi_pr_count,
                 'rel_wi_pr_wi_ta1ref_count': rel_wi_pr_wi_ta1ref_count,
                 'rel_wo_pr_count': rel_wo_pr_count}
    if not task2:
        # count ev provenance type
        rel_pr_text_df = rel_wi_pr_df.loc[rel_wi_pr_df['rel_prov_mediaType'].str.startswith('text', na=False)]
        rel_pr_text_count = len(rel_pr_text_df)
        rel_pr_image_df = rel_wi_pr_df.loc[rel_wi_pr_df['rel_prov_mediaType'].str.startswith('image', na=False)]
        rel_pr_image_count = len(rel_pr_image_df)
        rel_pr_video_df = rel_wi_pr_df.loc[rel_wi_pr_df['rel_prov_mediaType'].str.startswith('video', na=False)]
        rel_pr_video_count = len(rel_pr_video_df)
        rel_pr_eng_df = rel_wi_pr_df.loc[rel_wi_pr_df['rel_prov_language'] == 'English']
        rel_pr_eng_count = len(rel_pr_eng_df)
        rel_pr_spa_df = rel_wi_pr_df.loc[rel_wi_pr_df['rel_prov_language'] == 'Spanish']
        rel_pr_spa_count = len(rel_pr_spa_df)
        stats_row['rel_pr_text_count'] = rel_pr_text_count
        stats_row['rel_pr_image_count'] = rel_pr_image_count
        stats_row['rel_pr_video_count'] = rel_pr_video_count
        stats_row['rel_pr_eng_count'] = rel_pr_eng_count
        stats_row['rel_pr_spa_count'] = rel_pr_spa_count

    return stats_row


def add_single_ta2_schema_ke_stats(file_name: str, ta1: str, ta2: str, ce: str, target_task: str, schema_id: str,
                                   schema_super: str, schema_count: int, stats_df: pd.DataFrame,
                                   schema_df: pd.DataFrame, submission_stats_analysis_directory: str,
                                   ke: str) -> pd.DataFrame:
    if target_task == 'task2':
        task2 = True
    else:
        task2 = False

    if ke == 'ev':
        stats_row = get_single_ta2_schema_ev_stats(schema_df, file_name, ta1, ta2, ce, task2, schema_id, schema_super,
                                                   schema_count, submission_stats_analysis_directory)
    elif ke == 'arg':
        stats_row = get_single_ta2_schema_arg_stats(schema_df, file_name, ta1, ta2, ce, task2, schema_id, schema_super,
                                                    schema_count)
    elif ke == 'order':
        order_count = len(schema_df)
        stats_row = {'file_name': file_name, 'CE': ce, 'TA2': ta2, 'TA1': ta1, 'task2': task2, 'schema_id': schema_id,
                     'schema_super': schema_super, 'schema_count': schema_count,
                     'order_count': order_count}
    elif ke == 'rel':
        stats_row = get_single_ta2_schema_rel_stats(schema_df, file_name, ta1, ta2, ce, task2, schema_id, schema_super,
                                                    schema_count)
    else:
        sys.exit('KE error: ' + ke)

    stats_df = stats_df.append(stats_row, ignore_index=True)

    return stats_df


def add_ta2_team_ke_stats(score_directory: str, ta2_team_name: str, stats_df: pd.DataFrame, target_task: str,
                          submission_stats_analysis_directory: str, ke: str) -> pd.DataFrame:
    extracted_ke_directory = get_extracted_ke_directory(score_directory, ta2_team_name, ke)

    if not os.path.isdir(extracted_ke_directory):
        sys.exit('Directory not found: ' + extracted_ke_directory)

    for file_name in tqdm(os.listdir(extracted_ke_directory), position=0, leave=True):
        if not file_name.endswith('.csv'):
            continue

        file_df = pd.read_csv(extracted_ke_directory + file_name, low_memory=False)
        schema_list = file_df.schema_id.unique()
        if len(schema_list) > 0:
            grouped = file_df.groupby(file_df.schema_id)
            for schema in schema_list:
                schema_df = grouped.get_group(schema)
                file_name = schema_df.iloc[0]['file_name']
                fn_hyphen_list = file_name.split('-')
                ta1 = fn_hyphen_list[0]
                ta2 = fn_hyphen_list[1]
                ce = fn_hyphen_list[2]
                schema_id = schema_df.iloc[0]['schema_id']
                schema_super = schema_df.iloc[0]['schema_super']
                schema_count = len(schema_list)
                stats_df = add_single_ta2_schema_ke_stats(file_name, ta1, ta2, ce, target_task, schema_id, schema_super,
                                                          schema_count, stats_df, schema_df,
                                                          submission_stats_analysis_directory, ke)
    # read ta1 stats
    ta1_stats_fn = os.path.join(submission_stats_analysis_directory[:-1], '../TA1_schema_libraries',
                                'TA1_schema_library_stats.xlsx')
    if not os.path.isfile(ta1_stats_fn):
        sys.exit('File not found: ' + ta1_stats_fn)
    ta1_stats_df = pd.read_excel(ta1_stats_fn)
    # add ta1_ke_count column
    stats_df.insert(8, 'ta1_{0}_count'.format(ke), -1)
    if ke == 'ev':
        stats_df.insert(9, 'ta1_{0}_util_non_empty'.format(ke), -1.0)
        stats_df.insert(10, 'ta1_{0}_util_full'.format(ke), -1.0)
        stats_df.insert(11, 'non_ta1_{0}_util_wi_pr'.format(ke), -1.0)

    for i, stats_row in stats_df.iterrows():
        ta1 = stats_row.TA1.upper()
        schema_super = stats_row.schema_super
        matching_df = ta1_stats_df.loc[(ta1_stats_df['TA1'] == ta1) & (ta1_stats_df['schema_id'] == schema_super)]
        if len(matching_df) > 0:
            ta1_ke_count = matching_df.iloc[0]['{0}_count'.format(ke)]
            stats_df.at[i, 'ta1_{0}_count'.format(ke)] = ta1_ke_count
            if ke == 'ev':
                ev_wi_pr_wi_arg_count = stats_row.ev_wi_pr_wi_arg_count
                ev_wi_pr_wi_arg_wi_ta1ref_count = stats_row.ev_wi_pr_wi_arg_wi_ta1ref_count
                ev_wi_pr_wo_arg_count = stats_row.ev_wi_pr_wo_arg_count
                ev_wi_pr_wo_arg_wi_ta1ref_count = stats_row.ev_wi_pr_wo_arg_wi_ta1ref_count
                ta1_ke_util_non_empty = ev_wi_pr_wi_arg_wi_ta1ref_count / ta1_ke_count
                ta1_ke_util_full = (ev_wi_pr_wi_arg_wi_ta1ref_count + ev_wi_pr_wo_arg_wi_ta1ref_count) / ta1_ke_count
                non_ta1_ke_util_wi_pr = (ev_wi_pr_wi_arg_count - ev_wi_pr_wi_arg_wi_ta1ref_count +
                                         ev_wi_pr_wo_arg_count - ev_wi_pr_wo_arg_wi_ta1ref_count) / ta1_ke_count
                stats_df.at[i, 'ta1_{0}_util_non_empty'.format(ke)] = ta1_ke_util_non_empty
                stats_df.at[i, 'ta1_{0}_util_full'.format(ke)] = ta1_ke_util_full
                stats_df.at[i, 'non_ta1_{0}_util_wi_pr'.format(ke)] = non_ta1_ke_util_wi_pr

    return stats_df


def compute_ta1_ev_util(ta2_ev_stats_df: pd.DataFrame, submission_stats_analysis_directory: str) -> None:
    ta1_ev_util_mean_df = ta2_ev_stats_df.groupby(by=['TA2', 'TA1'], as_index=False)[
        'ta1_ev_util_non_empty', 'ta1_ev_util_full', 'non_ta1_ev_util_wi_pr'].mean()
    ta1_ev_util_max_df = ta2_ev_stats_df.groupby(by=['TA2', 'TA1'], as_index=False)[
        'ta1_ev_util_non_empty', 'ta1_ev_util_full', 'non_ta1_ev_util_wi_pr'].max()

    ta1_ev_util_df = ta1_ev_util_mean_df.set_index(['TA2', 'TA1']).join(
        ta1_ev_util_max_df.set_index(['TA2', 'TA1']), lsuffix='_mean', rsuffix='_max').reset_index()
    ta1_ev_util_df.to_excel(submission_stats_analysis_directory + 'ta1_ev_util.xlsx')


def compute_ta2_submission_stats_from_extraction(score_directory: str, submission_stats_analysis_directory: str,
                                                 target_task: str) -> None:
    if target_task == 'task1':
        ta2_team_name_list = ['CMU', 'IBM', 'JHU', 'RESIN']
        # ta2_team_name_list = ['RESIN']
    else:
        ta2_team_name_list = ['CMU', 'IBM', 'JHU', 'RESIN_PRIMARY']

    ta2_ev_stats_df = ta2_arg_stats_df = ta2_order_stats_df = ta2_rel_stats_df = pd.DataFrame()

    for ta2_team_name in ta2_team_name_list:
        ev_stats_df = pd.DataFrame(columns=['file_name', 'CE', 'TA2', 'TA1', 'task2', 'schema_id', 'schema_super',
                                            'schema_count', 'ev_all_count',
                                            'ev_wi_pr_wi_arg_count',
                                            'ev_wi_pr_wi_arg_wi_ta1ref_count',
                                            'ev_wi_pr_wo_arg_count',
                                            'ev_wi_pr_wo_arg_wi_ta1ref_count',
                                            'ev_wo_pr_wi_arg_count',
                                            'ev_wo_pr_wi_arg_wi_ta1ref_count',
                                            'ev_wo_pr_wi_arg_wo_ta1ref_count',
                                            'ev_wo_pr_wo_arg_count',
                                            'ev_wo_pr_wo_arg_wi_ta1ref_count',
                                            'ev_wo_pr_wo_arg_wo_ta1ref_count',
                                            'ev_wi_ta1ref_count', 'ta1_schema_ev_count'])
        if target_task == 'task1':
            ev_stats_df.insert(len(ev_stats_df.columns), 'ev_pr_text_count', -1)
            ev_stats_df.insert(len(ev_stats_df.columns), 'ev_pr_image_count', -1)
            ev_stats_df.insert(len(ev_stats_df.columns), 'ev_pr_video_count', -1)
            ev_stats_df.insert(len(ev_stats_df.columns), 'ev_pr_eng_count', -1)
            ev_stats_df.insert(len(ev_stats_df.columns), 'ev_pr_spa_count', -1)

        arg_stats_df = pd.DataFrame(columns=['file_name', 'CE', 'TA2', 'TA1', 'task2', 'schema_id', 'schema_super',
                                             'schema_count', 'arg_all_count', 'arg_wi_pr_count', 'arg_wo_pr_count'])
        if target_task == 'task1':
            arg_stats_df.insert(len(arg_stats_df.columns), 'arg_pr_text_count', -1)
            arg_stats_df.insert(len(arg_stats_df.columns), 'arg_pr_image_count', -1)
            arg_stats_df.insert(len(arg_stats_df.columns), 'arg_pr_video_count', -1)
            arg_stats_df.insert(len(arg_stats_df.columns), 'arg_pr_eng_count', -1)
            arg_stats_df.insert(len(arg_stats_df.columns), 'arg_pr_spa_count', -1)

        order_stats_df = pd.DataFrame(columns=['file_name', 'CE', 'TA2', 'TA1', 'task2', 'schema_id', 'schema_super',
                                               'schema_count', 'order_count'])

        rel_stats_df = pd.DataFrame(columns=['file_name', 'CE', 'TA2', 'TA1', 'task2', 'schema_id', 'schema_super',
                                             'schema_count', 'rel_all_count', 'rel_wi_pr_count',
                                             'rel_wi_pr_wi_ta1ref_count', 'rel_wo_pr_count'])
        if target_task == 'task1':
            rel_stats_df.insert(len(rel_stats_df.columns), 'rel_pr_text_count', -1)
            rel_stats_df.insert(len(rel_stats_df.columns), 'rel_pr_image_count', -1)
            rel_stats_df.insert(len(rel_stats_df.columns), 'rel_pr_video_count', -1)
            rel_stats_df.insert(len(rel_stats_df.columns), 'rel_pr_eng_count', -1)
            rel_stats_df.insert(len(rel_stats_df.columns), 'rel_pr_spa_count', -1)

        print('computing Event stats for TA2_' + ta2_team_name + '_' + target_task + ' ...')
        time.sleep(0.1)
        ev_stats_df = add_ta2_team_ke_stats(score_directory, ta2_team_name, ev_stats_df, target_task,
                                            submission_stats_analysis_directory, 'ev')
        print('computing Argument stats for TA2_' + ta2_team_name + '_' + target_task + ' ...')
        time.sleep(0.1)
        arg_stats_df = add_ta2_team_ke_stats(score_directory, ta2_team_name, arg_stats_df, target_task,
                                             submission_stats_analysis_directory, 'arg')
        print('computing Order stats for TA2_' + ta2_team_name + '_' + target_task + ' ...')
        time.sleep(0.1)
        order_stats_df = add_ta2_team_ke_stats(score_directory, ta2_team_name, order_stats_df, target_task,
                                               submission_stats_analysis_directory, 'order')
        print('computing Relation stats for TA2_' + ta2_team_name + '_' + target_task + ' ...')
        time.sleep(0.1)
        rel_stats_df = add_ta2_team_ke_stats(score_directory, ta2_team_name, rel_stats_df, target_task,
                                             submission_stats_analysis_directory, 'rel')

        # write to excel files
        output_directory = os.path.join(submission_stats_analysis_directory[:-1], ta2_team_name)
        if not os.path.isdir(output_directory):
            os.makedirs(output_directory)
        ev_stats_fn = os.path.join(submission_stats_analysis_directory[:-1], ta2_team_name,
                                   ta2_team_name + '_ev_stats.xlsx')
        ev_stats_df.to_excel(ev_stats_fn, index=False)
        arg_stats_fn = os.path.join(submission_stats_analysis_directory[:-1], ta2_team_name,
                                    ta2_team_name + '_arg_stats.xlsx')
        arg_stats_df.to_excel(arg_stats_fn, index=False)
        order_stats_fn = os.path.join(submission_stats_analysis_directory[:-1], ta2_team_name,
                                      ta2_team_name + '_order_stats.xlsx')
        order_stats_df.to_excel(order_stats_fn, index=False)
        rel_stats_fn = os.path.join(submission_stats_analysis_directory[:-1], ta2_team_name,
                                    ta2_team_name + '_rel_stats.xlsx')
        rel_stats_df.to_excel(rel_stats_fn, index=False)

        # merge to ta2_ke_stats
        ta2_ev_stats_df = ta2_ev_stats_df.append(ev_stats_df)
        ta2_arg_stats_df = ta2_arg_stats_df.append(arg_stats_df)
        ta2_order_stats_df = ta2_order_stats_df.append(order_stats_df)
        ta2_rel_stats_df = ta2_rel_stats_df.append(rel_stats_df)

    # write merged ta2 stats to excel files
    ta2_ev_stats_df.to_excel(submission_stats_analysis_directory + 'ta2_ev_stats.xlsx')
    ta2_arg_stats_df.to_excel(submission_stats_analysis_directory + 'ta2_arg_stats.xlsx')
    ta2_order_stats_df.to_excel(submission_stats_analysis_directory + 'ta2_order_stats.xlsx')
    ta2_rel_stats_df.to_excel(submission_stats_analysis_directory + 'ta2_rel_stats.xlsx')

    compute_ta1_ev_util(ta2_ev_stats_df, submission_stats_analysis_directory)


def add_single_ta1_file_stats(file_name: str, ta1_team_name: str, ta1_team_stats_df: pd.DataFrame,
                              ta1_json: dict) -> pd.DataFrame:
    schema_list = ta1_json['schemas']
    if schema_list:
        for schema in tqdm(schema_list, position=0, leave=True):
            ev_count = ev_sst_count = ev_st_count = ev_t_count = 0
            arg_count = order_count = rel_count = 0
            schema_id = None
            if '@id' in schema.keys():
                schema_id = schema['@id']
            if 'steps' in schema.keys():
                step_list = schema['steps']
                if step_list:
                    for step in step_list:
                        ev_count += 1

                        ev_type: str = step['@type']
                        if ',' in ev_type:
                            sys.exit('Event type is a list')
                        if ev_type.endswith('.Unspecified.Unspecified'):
                            ev_t_count += 1
                        elif ev_type.endswith('.Unspecified'):
                            ev_st_count += 1
                        else:
                            ev_sst_count += 1

                        if 'participants' in step.keys():
                            participant_list = step['participants']
                            if participant_list:
                                arg_count += len(participant_list)
            if 'order' in schema.keys():
                order_list = schema['order']
                if order_list:
                    order_count += len(order_list)
            if 'entityRelations' in schema.keys():
                ent_rel_list = schema['entityRelations']
                if ent_rel_list:
                    for ent_rel in ent_rel_list:
                        if 'relations' in ent_rel.keys():
                            relation_list = ent_rel['relations']
                            if relation_list:
                                rel_count += len(relation_list)

            if ev_count != (ev_sst_count + ev_st_count + ev_t_count):
                sys.exit('Event count error')
            stats_row = {'file_name': file_name, 'TA1': ta1_team_name, 'task2': False, 'schema_id': schema_id,
                         'ev_count': ev_count, 'ev_sst_count': ev_sst_count, 'ev_st_count': ev_st_count,
                         'ev_t_count': ev_t_count,
                         'arg_count': arg_count, 'order_count': order_count, 'rel_count': rel_count}

            ta1_team_stats_df = ta1_team_stats_df.append(stats_row, ignore_index=True)

    return ta1_team_stats_df


def compute_ta1_submission_stats(ta1_output_directory: str, submission_stats_analysis_directory: str) -> None:
    ta1_stats_df = pd.DataFrame(columns=['file_name', 'TA1', 'task2', 'schema_id', 'ev_count',
                                         'ev_sst_count', 'ev_st_count', 'ev_t_count',
                                         'arg_count', 'order_count', 'rel_count'])

    ta1_team_name_list = ['CMU', 'IBM', 'ISI', 'JHU', 'RESIN', 'SBU']
    for ta1_team_name in ta1_team_name_list:
        ta1_team_directory = ta1_output_directory + ta1_team_name + '/'
        if not os.path.isdir(ta1_team_directory):
            sys.exit('Directory not found: ' + ta1_team_directory)

        ta1_team_stats_df = pd.DataFrame(columns=['file_name', 'TA1', 'task2', 'schema_id', 'ev_count',
                                                  'ev_sst_count', 'ev_st_count', 'ev_t_count',
                                                  'arg_count', 'order_count', 'rel_count'])
        for file_name in os.listdir(ta1_team_directory):
            if file_name[-5:] != '.json':
                continue
            with open(ta1_team_directory + file_name) as json_file:
                ta1_json = json.load(json_file)
            print('computing TA1_' + ta1_team_name + ' library stats...')
            time.sleep(0.1)
            ta1_team_stats_df = add_single_ta1_file_stats(file_name, ta1_team_name, ta1_team_stats_df, ta1_json)

        # write to excel files
        output_directory = os.path.join(submission_stats_analysis_directory[:-1], 'TA1_schema_libraries', ta1_team_name)
        if not os.path.isdir(output_directory):
            os.makedirs(output_directory)
        ta1_team_stats_fn = os.path.join(output_directory, ta1_team_name + '_schema_library_stats.xlsx')
        ta1_team_stats_df.to_excel(ta1_team_stats_fn, index=False)
        # merge to ta1 stats
        ta1_stats_df = ta1_stats_df.append(ta1_team_stats_df)

    # save the merged ta1 stats
    merged_output_directory = os.path.join(submission_stats_analysis_directory[:-1], 'TA1_schema_libraries')
    if not os.path.isdir(merged_output_directory):
        os.makedirs(merged_output_directory)
    ta1_stats_fn = os.path.join(merged_output_directory, 'TA1_schema_library_stats.xlsx')
    ta1_stats_df.to_excel(ta1_stats_fn, index=False)


def compute_submission_stats() -> None:
    base_dir = ''
    # get path of directories
    try:
        config = configparser.ConfigParser()
        base_dir = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(base_dir, "../config.ini")) as configfile:
            config.read_file(configfile)
    except:
        sys.exit('CAN NOT OPEN CONFIG FILE: ' + os.path.join(os.path.dirname(base_dir), "../config.ini"))

    ta1_output_directory = config['Phase1Eval']['ta1_output_directory']
    task1_score_directory = config['Phase1Eval']['task1_score_directory']
    task2_score_directory = config['Phase1Eval']['task2_score_directory']
    submission_stats_analysis_directory = config['Phase1Eval']['submission_stats_analysis_directory']

    if not os.path.isdir(task1_score_directory) or not os.path.isdir(task2_score_directory) or \
            not os.path.isdir(task2_score_directory):
        sys.exit('Directory not found: ' + task1_score_directory + ' or ' + task2_score_directory +
                 ' or ' + ta1_output_directory)

    # compute ta1 schema library stats
    # compute_ta1_submission_stats(ta1_output_directory, submission_stats_analysis_directory)

    # compute ta2 task1 submission stats
    task1_submission_stats_analysis_directory = submission_stats_analysis_directory + 'TA2_Task1/'
    if not os.path.isdir(task1_submission_stats_analysis_directory):
        os.makedirs(task1_submission_stats_analysis_directory)
    compute_ta2_submission_stats_from_extraction(task1_score_directory, task1_submission_stats_analysis_directory,
                                                 'task1')

    # compute ta2 task2 submission stats
    task2_submission_stats_analysis_directory = submission_stats_analysis_directory + 'TA2_Task2/'
    if not os.path.isdir(task2_submission_stats_analysis_directory):
        os.makedirs(task2_submission_stats_analysis_directory)
    compute_ta2_submission_stats_from_extraction(task2_score_directory, task2_submission_stats_analysis_directory,
                                                 'task2')


if __name__ == "__main__":
    compute_submission_stats()
