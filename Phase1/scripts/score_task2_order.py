#!/usr/bin/env python3

__author__ = "Xiongnan Jin (xiongnan.jin@nist.gov)"
__version__ = "Development: 0.5.3"
__date__ = "02/05/2021"

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
# extract order information from TA2 output JSON files and save as csv files
# read extracted order csv files and compute recall score
######################################################################################
import argparse
import configparser
import logging
import os
import sys
import time
from typing import Tuple

import pandas as pd
import copy
from tqdm import tqdm
from scripts import extract_order

scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../scripts")
sys.path.append(scripts_path)


def initiate_file_and_schema_order_stats_df() -> Tuple[pd.DataFrame, pd.DataFrame]:
    file_order_stats_df = pd.DataFrame(columns=['file_name', 'CE', 'TA2', 'TA1',
                                                'file_order_all_count', 'file_order_valid_count', 'file_gorder_count',
                                                'gorder_count'])
    schema_order_score_df = pd.DataFrame(columns=['file_name', 'schema_id', 'schema_super', 'CE', 'TA2', 'TA1',
                                                  'schema_order_all_count', 'schema_order_valid_count',
                                                  'schema_gorder_count', 'gorder_count'])

    return file_order_stats_df, schema_order_score_df


def is_ta1ref_valid(order_row: pd.Series) -> bool:
    order_ta1ref = order_row.order_ta1ref
    if order_ta1ref == 'nist:scorer_inferred':
        return True

    if ':' in order_ta1ref:
        prefix = order_ta1ref.split(':')[0]
        prefix = prefix.lower
        if prefix in ['cmu', 'ibm', 'isi', 'jhu', 'resin', 'sbu']:
            return True

    return False


def assess_order(input_order_df: pd.DataFrame, gorder_df: pd.DataFrame) -> pd.DataFrame:
    cur_gorder_df = copy.deepcopy(gorder_df)
    cur_gorder_df.insert(len(cur_gorder_df.columns), 'visited', False)
    gorder_before_set = set(cur_gorder_df.order_before.tolist())
    gorder_after_set = set(cur_gorder_df.order_after.tolist())
    for i, row in input_order_df.iterrows():
        # if ta1ref points to proper ta1 schema library,
        if is_ta1ref_valid(row):
            order_before_provenance = row.order_before_provenance
            order_after_provenance = row.order_after_provenance
            valid = False
            # Changed scoring to insist that a (before, after) appears in a row
            if order_before_provenance in gorder_before_set and \
                   order_after_provenance in gorder_after_set:
            # if len(gorder_df.loc[(gorder_df.order_before == order_before_provenance) &
            #                      (gorder_df.order_after == order_after_provenance), :]) > 0:
                valid = True
            input_order_df.at[i, 'valid'] = valid
            correct_df = pd.DataFrame()
            if valid:
                correct_df = cur_gorder_df.loc[
                    (cur_gorder_df['order_before'] == order_before_provenance) &
                    (cur_gorder_df['order_after'] == order_after_provenance) &
                    (cur_gorder_df['visited'] == False)
                ]
            if len(correct_df) > 0:
                input_order_df.at[i, 'assessment'] = True
                j = correct_df.index.values[0]
                cur_gorder_df.at[j, 'visited'] = True

    return input_order_df


def add_to_order_stats(input_order_df: pd.DataFrame, gorder_count: int,
                       input_order_stats_df: pd.DataFrame, base_unit: str) -> pd.DataFrame:
    file_name = input_order_df.iloc[0]['file_name']
    fn_hyphen_list = file_name.split('-')
    ta1 = fn_hyphen_list[0]
    ta2 = fn_hyphen_list[1]
    target_ce = fn_hyphen_list[2]
    input_order_all_count = len(input_order_df)
    input_order_valid_count = len(input_order_df.loc[input_order_df['valid']])
    input_gorder_count = len(input_order_df.loc[input_order_df['assessment']])
    if base_unit == 'file':
        score_order_row = {'file_name': file_name, 'CE': target_ce, 'TA2': ta2, 'TA1': ta1,
                           'file_order_all_count': input_order_all_count,
                           'file_order_valid_count': input_order_valid_count,
                           'file_gorder_count': input_gorder_count, 'gorder_count': gorder_count}
    elif base_unit == 'schema':
        schema_id = input_order_df.iloc[0]['schema_id']
        schema_super = input_order_df.iloc[0]['schema_super']
        score_order_row = {'file_name': file_name, 'schema_id': schema_id, 'schema_super': schema_super,
                           'CE': target_ce, 'TA2': ta2, 'TA1': ta1,
                           'schema_order_all_count': input_order_all_count,
                           'schema_order_valid_count': input_order_valid_count,
                           'schema_gorder_count': input_gorder_count, 'gorder_count': gorder_count}
    else:
        sys.exit('Base input error, should be file or schema')

    input_order_stats_df = input_order_stats_df.append(score_order_row, ignore_index=True)

    return input_order_stats_df


def compute_schema_order_stats(file_order_df: pd.DataFrame, gorder_df: pd.DataFrame,
                               schema_order_stats_df: pd.DataFrame) -> pd.DataFrame:
    schema_list = file_order_df.schema_id.unique()
    if len(schema_list) > 0:
        # file order is already assessed
        grouped = file_order_df.groupby(file_order_df.schema_id)

        for schema in schema_list:
            schema_order_df = grouped.get_group(schema)
            schema_order_df = assess_order(schema_order_df, gorder_df)
            schema_order_stats_df = add_to_order_stats(schema_order_df, len(gorder_df),
                                                       schema_order_stats_df, 'schema')

    return schema_order_stats_df


def get_order_stats(ta2_team_name: str, score_directory: str, graph_g_directory: str):
    file_order_stats_df, schema_order_stats_df = initiate_file_and_schema_order_stats_df()

    order_directory = score_directory + ta2_team_name + '/Orders/'
    for file_name in tqdm(os.listdir(order_directory), position=0, leave=False):
        # only consider .csv file (ignoring system hidden files like .DS_store)
        fn_surfix = file_name[-4:]
        if fn_surfix != '.csv':
            continue

        target_ce = file_name.split('-')[2]

        # compute file order stats
        file_order_df = pd.read_csv(order_directory + file_name)
        gorder_fn = graph_g_directory + target_ce + '/' + target_ce + '_GraphG_order.csv'
        if not os.path.isfile(gorder_fn):
            sys.exit('File not found: ' + gorder_fn)
        gorder_df = pd.read_csv(gorder_fn)
        # if it is task 1 file, then skip
        task2 = False
        if 'task2' in file_name:
            task2 = True
        if not task2:
            continue

        if 'valid' not in file_order_df.columns:
            file_order_df.insert(1, 'valid', False)
        if 'assessment' not in file_order_df.columns:
            file_order_df.insert(1, 'assessment', False)

        file_order_df = assess_order(file_order_df, gorder_df)

        # re-write assessed file_order
        file_order_df.to_csv(order_directory + file_name, index=False)

        file_order_stats_df = add_to_order_stats(file_order_df, len(gorder_df), file_order_stats_df, 'file')

        # compute schema order stats
        schema_order_stats_df = compute_schema_order_stats(file_order_df, gorder_df, schema_order_stats_df)

    return file_order_stats_df, schema_order_stats_df


def score_recall(file_order_stats_df: pd.DataFrame, schema_order_stats_df: pd.DataFrame):
    # compute file based recall
    file_order_stats_df.insert(4, 'recall', -1.0)

    for i, row in file_order_stats_df.iterrows():
        molecular = row.file_gorder_count
        denominator = row.gorder_count
        recall = molecular / denominator
        file_order_stats_df.at[i, 'recall'] = recall

    # compute schema based recall
    schema_order_stats_df.insert(5, 'recall', -1.0)

    for i, row in schema_order_stats_df.iterrows():
        molecular = row.schema_gorder_count
        denominator = row.gorder_count
        recall = molecular / denominator
        schema_order_stats_df.at[i, 'recall'] = recall

    return file_order_stats_df, schema_order_stats_df


def score(ta2_team_name: str, graph_g_ext: bool, ta2_ext: bool, mode: str) -> None:
    print('Graph G extraction is set as ' + str(graph_g_ext))
    print('TA2 output extraction is set as ' + str(ta2_ext))

    ta2_team_name = ta2_team_name.upper()
    # get path of directories
    base_dir = ''
    try:
        config = configparser.ConfigParser()
        base_dir = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(base_dir, "../config.ini")) as configfile:
            config.read_file(configfile)
    except:
        sys.exit('CAN NOT OPEN CONFIG FILE: ' + os.path.join(os.path.dirname(base_dir), "../config.ini"))

    if mode == 'evaluation':
        ta2_output_directory = config['Phase1Eval']['ta2_task2_output_directory']
        task2_score_directory = config['Phase1Eval']['task2_score_directory']
        graph_g_directory = config['Phase1Eval']['graph_g_directory']
    elif mode == 'test':
        ta2_output_directory = config['Test']['ta2_task2_output_directory']
        task2_score_directory = config['Test']['task2_score_directory']
        graph_g_directory = config['Test']['graph_g_directory']
    else:
        sys.exit('Please enter the correct mode: evaluation or test')

    if not os.path.exists(task2_score_directory):
        os.makedirs(task2_score_directory)

    if graph_g_ext:
        print('---------------------------------------------------------')
        print('Start to extract graph G orders...')
        time.sleep(0.1)
        extract_order.extract_graph_g_order_from_json(graph_g_directory)
        print('Extraction completed!')
    else:
        print('---------------------------------------------------------')
        print('Skip order extraction from graph G.')

    if ta2_ext:
        print('---------------------------------------------------------')
        print('Start to extract orders from json outputs of ' + ta2_team_name + '...')
        time.sleep(0.1)
        extract_order.extract_ta2_order_from_json(ta2_team_name, ta2_output_directory, task2_score_directory, [])
        print('Extraction completed!')
    else:
        print('---------------------------------------------------------')
        print('Skip order extraction from TA2 outputs.')

    print('-----------------------------------------------------------------------------')
    print('Start to compute order score of ' + ta2_team_name + '...')
    time.sleep(0.1)
    file_order_stats_df, schema_order_stats_df = get_order_stats(ta2_team_name, task2_score_directory,
                                                                 graph_g_directory)
    file_order_score_df, schema_order_score_df = score_recall(file_order_stats_df, schema_order_stats_df)

    print('Scoring finished!')

    # save score order as excel files
    if len(file_order_score_df) == 0:
        print('No TA2 task2 score of order from TA2_' + ta2_team_name)
    else:
        file_order_score_df.to_excel(task2_score_directory + ta2_team_name + '/task2_file_score_order_'
                                     + ta2_team_name + '.xlsx', index=False)
        schema_order_score_df.to_excel(task2_score_directory + ta2_team_name + '/task2_schema_score_order_'
                                       + ta2_team_name + '.xlsx', index=False)


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 'y', 't', '1'):
        return True
    elif v.lower() in ('no', 'false', 'n', 'f', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def cli_parser():
    parser = argparse.ArgumentParser(
        description='Task2 scorer for order'
    )

    parser.add_argument('-t', '--ta2-team-name', help='supported ta2 team names are: CMU, IBM, JHU,'
                                                      'RESIN_PRIMARY, RESIN_RELAXED_ALL, and RESIN_RELAXED_ATTACK',
                        required=True, type=str)
    parser.add_argument('-gge', '--graph_g_ext', help='whether to conduct graph g extraction',
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


if __name__ == "__main__":
    cli_parser()

    # ta2_team_name = ['CMU']
    # graph_g_ext = True
    # ta2_ext = True
    # score(ta2_team_name, ta2_ext, graph_g_ext)

    # ta2_team_name = ['CMU', 'IBM', 'JHU', 'RESIN_primary', 'RESIN_relaxed_all', 'RESIN_relaxed_attack']
    # graph_g_ext = True
    # ta2_ext = True
    # for ta2 in ta2_team_name:
    #     score([ta2], ta2_ext, graph_g_ext)
