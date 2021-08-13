#!/usr/bin/env python3

__author__ = "Xiongnan Jin (xiongnan.jin@nist.gov)"
__version__ = "Development: 1.0.0"
__date__ = "03/10/2021"

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
# TBD
######################################################################################
import configparser
import os
import sys
import time

import numpy as np
import pandas as pd

from tqdm.auto import tqdm


def get_duplicated_events(ev_row, ev_df: pd.DataFrame) -> pd.DataFrame:
    ungrouped_ev_df = ev_df.loc[ev_df['ev_group_id'] == -1]

    ev_type = ev_row.ev_type
    ev_name = ev_row.ev_name
    ev_ta1ref = ev_row.ev_ta1ref
    ev_comment = ev_row.ev_comment
    ev_provenance = ev_row.ev_provenance
    ev_modality = ev_row.ev_modality
    ev_earliestStartTime = ev_row.ev_earliestStartTime
    ev_latestStartTime = ev_row.ev_latestStartTime
    ev_earliestEndTime = ev_row.ev_earliestEndTime
    ev_latestEndTime = ev_row.ev_latestEndTime
    ev_requires = ev_row.ev_requires
    ev_achieves = ev_row.ev_achieves

    dup_ev_df = ungrouped_ev_df.loc[(ungrouped_ev_df['ev_type'] == ev_type) & (ungrouped_ev_df['ev_name'] == ev_name) &
                                    (ungrouped_ev_df['ev_ta1ref'] == ev_ta1ref) &
                                    (ungrouped_ev_df['ev_comment'] == ev_comment) &
                                    (ungrouped_ev_df['ev_provenance'] == ev_provenance) &
                                    (ungrouped_ev_df['ev_modality'] == ev_modality) &
                                    (ungrouped_ev_df['ev_earliestStartTime'] == ev_earliestStartTime) &
                                    (ungrouped_ev_df['ev_latestStartTime'] == ev_latestStartTime) &
                                    (ungrouped_ev_df['ev_earliestEndTime'] == ev_earliestEndTime) &
                                    (ungrouped_ev_df['ev_latestEndTime'] == ev_latestEndTime) &
                                    (ungrouped_ev_df['ev_requires'] == ev_requires) & (
                                            ungrouped_ev_df['ev_achieves'] == ev_achieves)]

    return dup_ev_df


def add_ev_group_id(ev_df: pd.DataFrame) -> pd.DataFrame:
    ev_df = ev_df.loc[:, 'file_name':'ev_achieves']
    ev_df = ev_df.drop(columns=['ev_confidence'])

    if 'ev_group_id' not in ev_df.columns:
        ev_df.insert(len(ev_df.columns), 'ev_group_id', -1)

    ev_df = ev_df.replace(np.nan, 'empty', regex=True)

    cur_group_id = 0
    for ev_row in tqdm(ev_df.itertuples(), total=ev_df.shape[0], position=0, leave=True):
        ev_group_id = ev_row.ev_group_id
        if ev_group_id == -1:
            duplicated_events_df = get_duplicated_events(ev_row, ev_df)
            if len(duplicated_events_df) > 1:
                duplicated_event_indices = duplicated_events_df.index
                for index in duplicated_event_indices:
                    ev_df.at[index, 'ev_group_id'] = cur_group_id
                cur_group_id += 1

    return ev_df


def compute_ta2_duplicated_event_stats(score_directory: str, duplicated_ke_analysis_directory: str,
                                       ta2_team_name: str, target_task: str) -> None:
    input_directory = os.path.join(score_directory[:-1], ta2_team_name, 'Event_arguments')
    if not os.path.isdir(input_directory):
        sys.exit('Directory not found: ' + input_directory)

    ev_df = pd.DataFrame()
    print('Aggregating TA2_{0}_task1 output files...'.format(ta2_team_name))
    time.sleep(0.1)
    for file_name in tqdm(os.listdir(input_directory), position=0, leave=True):
        if file_name[-4:] != '.csv':
            continue
        file_ev_arg_fn = os.path.join(input_directory, file_name)
        file_ev_arg_df = pd.read_csv(file_ev_arg_fn)
        file_ev_df = file_ev_arg_df.drop_duplicates(subset=['schema_id', 'ev_id'])
        ev_df = ev_df.append(file_ev_df, ignore_index=True)

    print('Adding event group id...')
    time.sleep(0.1)
    ev_df = ev_df.loc[ev_df['ev_provenance'].notnull() & (ev_df['ev_provenance'] != 'n/a')]
    ev_df = add_ev_group_id(ev_df)
    # ev_grouped_df.sort_values(by=['ev_group_id'], ascending=False, inplace=True)
    ev_all_count = len(ev_df)
    ev_unique_df = ev_df.loc[ev_df['ev_group_id'] == -1]
    ev_unique_count = len(ev_unique_df)
    ev_count_df = pd.DataFrame()
    ev_count_row = {'ev_all_count': ev_all_count, 'ev_unique_count': ev_unique_count}
    ev_count_df = ev_count_df.append(ev_count_row, ignore_index=True)
    duplicated_ev_all_df = ev_df.loc[ev_df['ev_group_id'] != -1]
    grouped_ev_all_count_df = duplicated_ev_all_df.groupby(['ev_group_id']).size().reset_index(name='member_ev_count')
    duplicated_ev_wi_ta1ref_df = ev_df.loc[(ev_df['ev_group_id'] != -1) & (ev_df['ev_ta1ref'] != 'kairos:NULL')]
    grouped_ev_wi_ta1ref_count_df = duplicated_ev_wi_ta1ref_df.groupby(['ev_group_id']).size(). \
        reset_index(name='member_ev_count')
    duplicated_ev_wo_ta1ref_df = ev_df.loc[(ev_df['ev_group_id'] != -1) & (ev_df['ev_ta1ref'] == 'kairos:NULL')]
    grouped_ev_wo_ta1ref_count_df = duplicated_ev_wo_ta1ref_df.groupby(['ev_group_id']).size(). \
        reset_index(name='member_ev_count')

    ev_df.to_excel(duplicated_ke_analysis_directory + ta2_team_name + '_ev_grouped.xlsx', index=False)
    ev_count_df.to_excel(duplicated_ke_analysis_directory + ta2_team_name + '_ev_count.xlsx', index=False)
    grouped_ev_all_count_df.to_excel(duplicated_ke_analysis_directory + ta2_team_name +
                                     '_duplicated_ev_all_distribution.xlsx', index=False)
    grouped_ev_wi_ta1ref_count_df.to_excel(duplicated_ke_analysis_directory + ta2_team_name +
                                           '_duplicated_ev_wi_ta1ref_distribution.xlsx', index=False)
    grouped_ev_wo_ta1ref_count_df.to_excel(duplicated_ke_analysis_directory + ta2_team_name +
                                           '_duplicated_ev_wo_ta1ref_distribution.xlsx', index=False)


def computed_duplicated_events() -> None:
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
    task2_score_directory = config['Phase1Eval']['task2_score_directory']
    duplicated_ke_analysis_directory = config['Phase1Eval']['duplicated_ke_analysis_directory']

    compute_ta2_duplicated_event_stats(task1_score_directory, duplicated_ke_analysis_directory, 'CMU', 'task1')
    compute_ta2_duplicated_event_stats(task1_score_directory, duplicated_ke_analysis_directory, 'IBM', 'task1')
    compute_ta2_duplicated_event_stats(task1_score_directory, duplicated_ke_analysis_directory, 'JHU', 'task1')
    compute_ta2_duplicated_event_stats(task1_score_directory, duplicated_ke_analysis_directory, 'RESIN', 'task1')


if __name__ == '__main__':
    computed_duplicated_events()
