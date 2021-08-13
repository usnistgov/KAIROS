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
# extract order information from TA2 output JSON files and save as csv files
# read extracted order csv, event assessment, and temporal annotation files
# then compute precision, recall, and F1 measure
######################################################################################
import argparse
import configparser
import logging
import os
import sys
import time
from typing import Tuple

import pandas as pd
from tqdm.auto import tqdm
from scripts import extract_order

scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../scripts")
sys.path.append(scripts_path)


def get_ep_assessment(order_before: str, order_after: str, file_name: str,
                      score_directory: str, ta2_team_name: str):
    assess_file_name = score_directory + ta2_team_name + '/Assessment/' \
                       + file_name[:-9] + 'ep_ass.csv'
    assess_df = pd.read_csv(assess_file_name)
    order_before_row = assess_df.loc[assess_df['SEP'] == order_before]
    order_before_rep = order_before_row.iloc[0]['Ref_EP']
    order_after_row = assess_df.loc[assess_df['SEP'] == order_after]
    order_after_rep = order_after_row.iloc[0]['Ref_EP']

    return order_before_rep, order_after_rep


def add_assessment_columns(file_order_df: pd.DataFrame) -> pd.DataFrame:
    if 'order_before_ref_id' not in file_order_df.columns:
        file_order_df.insert(len(file_order_df.columns), 'order_before_ref_id', 'empty')
    if 'order_after_ref_id' not in file_order_df.columns:
        file_order_df.insert(len(file_order_df.columns), 'order_after_ref_id', 'empty')
    if 'assessment' not in file_order_df.columns:
        file_order_df.insert(len(file_order_df.columns), 'assessment', 'empty')

    return file_order_df


def get_assessed_file_ev(task1_score_directory: str, ta2_team_name: str, file_name: str) -> pd.DataFrame:
    assessed_file_ev_dir = task1_score_directory + ta2_team_name + '/Assessed_events/'
    if not os.path.isdir(assessed_file_ev_dir):
        sys.exit('Directory not found: ' + assessed_file_ev_dir)
    assessed_file_ev_fn = assessed_file_ev_dir + file_name[:-10] + '_ev_arg.csv'
    if not os.path.isfile(assessed_file_ev_fn):
        sys.exit('File not found: ' + assessed_file_ev_fn)
    assessed_file_ev_df = pd.read_csv(assessed_file_ev_fn)

    return assessed_file_ev_df


def get_true_file_ev_count(assessed_file_ev_df: pd.DataFrame) -> pd.DataFrame:
    true_file_ev_count = 0

    for i, row in assessed_file_ev_df.iterrows():
        ev_reference_id = row.ev_reference_id
        if ev_reference_id != 'empty' and ev_reference_id[:2] == 'VP':
            true_file_ev_count += 1

    return true_file_ev_count


def get_reference_ev_id(schema_id: str, order_ev: str, assessed_file_ev_df: pd.DataFrame) -> str:
    order_ev_ref_ev_id = 'empty'
    order_ev_mapping_ev_df = assessed_file_ev_df.loc[
        (assessed_file_ev_df['schema_id'] == schema_id) &
        (assessed_file_ev_df['ev_id'] == order_ev)]
    if len(order_ev_mapping_ev_df) > 0:
        ev_reference_id = order_ev_mapping_ev_df.iloc[0]['ev_reference_id']
        for i, row in order_ev_mapping_ev_df.iterrows():
            cur_ev_ref_id = row.ev_reference_id
            if ev_reference_id != cur_ev_ref_id:
                sys.exit('One system event is mapped to multiple annotation events!')
        order_ev_ref_ev_id = ev_reference_id

    return order_ev_ref_ev_id


def get_target_annotation_order(target_ce: str, annotation_directory: str) -> pd.DataFrame:
    target_annotation_order_fn = annotation_directory + target_ce + '/' + target_ce + '_order.xlsx'
    if not os.path.isfile(target_annotation_order_fn):
        sys.exit('File not found: ' + target_annotation_order_fn)
    target_annotation_order_df = pd.read_excel(target_annotation_order_fn)

    return target_annotation_order_df


def assess_order_pair(order_before_ref_ev_id: str, order_after_ref_ev_id: str,
                      target_annotation_order_df: pd.DataFrame) -> str:
    matching_order_df = target_annotation_order_df.loc[
        (target_annotation_order_df['before'] == order_before_ref_ev_id) &
        (target_annotation_order_df['after'] == order_after_ref_ev_id)]
    if len(matching_order_df) > 0:
        return 'true'
    else:
        return 'false'


def assess_file_orders(file_order_df: pd.DataFrame, assessed_file_ev_df: pd.DataFrame,
                       target_ce: str, annotation_directory: str) -> pd.DataFrame:
    for i, fo_row in file_order_df.iterrows():
        schema_id = fo_row.schema_id
        # if one of the order events is not assessed or referred to a relation, skip this pair
        order_before = fo_row.order_before
        order_before_ref_id = get_reference_ev_id(schema_id, order_before, assessed_file_ev_df)
        if order_before_ref_id[:2] != 'VP':
            continue
        order_after = fo_row.order_after
        order_after_ref_id = get_reference_ev_id(schema_id, order_after, assessed_file_ev_df)
        if order_after_ref_id[:2] != 'VP':
            continue
        # otherwise, assess the order pair based on the annotation_order
        target_annotation_order_df = get_target_annotation_order(target_ce, annotation_directory)
        order_pair_assessment = assess_order_pair(order_before_ref_id, order_after_ref_id,
                                                  target_annotation_order_df)
        file_order_df.at[i, 'order_before_ref_id'] = order_before_ref_id
        file_order_df.at[i, 'order_after_ref_id'] = order_after_ref_id
        file_order_df.at[i, 'assessment'] = order_pair_assessment

    return file_order_df


def add_order_stats(input_order_stats_df: pd.DataFrame, input_order_df: pd.DataFrame,
                    annotation_directory: str, input_unit: str) -> pd.DataFrame:
    true_order_df = input_order_df.loc[input_order_df['assessment'] == 'true']
    distinct_true_order_df = true_order_df.drop_duplicates(
        ['order_before_ref_id', 'order_after_ref_id'])

    json_file_name = input_order_df.iloc[0]['file_name']
    fn_hyphen_list = json_file_name.split('-')
    ta1 = fn_hyphen_list[0]
    ta2 = fn_hyphen_list[1]
    target_ce = fn_hyphen_list[2]

    all_order_count = len(input_order_df)
    assessed_order_df = input_order_df.loc[input_order_df['assessment'] != 'empty']
    assessed_order_count = len(assessed_order_df)
    true_order_count = len(true_order_df)
    distinct_true_order_count = len(distinct_true_order_df)
    ref_order_df = get_target_annotation_order(target_ce, annotation_directory)
    ref_order_count = len(ref_order_df)

    if input_unit == 'file':
        stats_dict = {'file_name': json_file_name, 'TA1': ta1, 'TA2': ta2, 'target_ce': target_ce,
                      'all_order_count': all_order_count, 'assessed_order_count': assessed_order_count,
                      'true_order_count': true_order_count,
                      'distinct_true_order_count': distinct_true_order_count,
                      'ref_order_count': ref_order_count}
    elif input_unit == 'schema':
        schema_id = input_order_df.iloc[0]['schema_id']
        schema_super = input_order_df.iloc[0]['schema_super']
        stats_dict = {'file_name': json_file_name, 'schema_id': schema_id, 'schema_super': schema_super, 'TA1': ta1, 'TA2': ta2,
                      'target_ce': target_ce, 'all_order_count': all_order_count,
                      'assessed_order_count': assessed_order_count, 'true_order_count': true_order_count,
                      'distinct_true_order_count': distinct_true_order_count,
                      'ref_order_count': ref_order_count}
    else:
        sys.exit('input_unit should be file or schema!')

    input_order_stats_df = input_order_stats_df.append(stats_dict, ignore_index=True)

    return input_order_stats_df


def compute_schema_order_stats(schema_order_stats_df: pd.DataFrame, file_order_df: pd.DataFrame,
                               annotation_directory: str) -> pd.DataFrame:
    schema_list = file_order_df.schema_id.unique()
    if len(schema_list) > 0:
        grouped = file_order_df.groupby(file_order_df.schema_id)
        for schema_id in schema_list:
            schema_order_df = grouped.get_group(schema_id)
            schema_order_stats_df = add_order_stats(schema_order_stats_df, schema_order_df,
                                                    annotation_directory, 'schema')

    return schema_order_stats_df


def compute_order_stats(ta2_team_name: str, task1_score_directory: str, annotation_directory: str) \
        -> Tuple[pd.DataFrame, pd.DataFrame]:
    file_order_stats_df = pd.DataFrame(columns=['file_name', 'TA1', 'TA2', 'target_ce', 'all_order_count',
                                                'assessed_order_count', 'true_order_count', 'distinct_true_order_count',
                                                'ref_order_count'])
    schema_order_stats_df = pd.DataFrame(columns=['file_name', 'schema_id', 'schema_super', 'TA1', 'TA2', 'target_ce',
                                                  'all_order_count', 'assessed_order_count', 'true_order_count',
                                                  'distinct_true_order_count', 'ref_order_count'])

    extracted_ta2_task1_order_directory = task1_score_directory + ta2_team_name + '/Orders/'
    if not os.path.isdir(extracted_ta2_task1_order_directory):
        sys.exit('Directory not found: ' + extracted_ta2_task1_order_directory)

    for file_name in tqdm(os.listdir(extracted_ta2_task1_order_directory), position=0, leave=False):
        fn_suffix = file_name[-4:]
        # skip non csv files
        if fn_suffix != '.csv':
            continue

        file_order_df = pd.read_csv(extracted_ta2_task1_order_directory + file_name, low_memory=False)
        file_order_df = add_assessment_columns(file_order_df)

        assessed_file_ev_df = get_assessed_file_ev(task1_score_directory, ta2_team_name, file_name)
        true_file_ev_count = get_true_file_ev_count(assessed_file_ev_df)

        # if there exist matching events, assess orders
        if true_file_ev_count > 0:
            target_ce = file_name.split('-')[2]
            # assess file orders
            assessed_file_order_df = assess_file_orders(
                file_order_df, assessed_file_ev_df, target_ce, annotation_directory)
            # save the assessed order as a file
            assessed_order_directory = task1_score_directory + ta2_team_name + '/Assessed_orders/'
            if not os.path.isdir(assessed_order_directory):
                os.makedirs(assessed_order_directory)
            assessed_order_fp = assessed_order_directory + file_name[:-4] + '_order.csv'
            assessed_file_order_df.to_csv(assessed_order_fp, index=False)
            # compute file order stats
            file_order_stats_df = add_order_stats(
                file_order_stats_df, assessed_file_order_df, annotation_directory, 'file')
            # compute schema order stats
            schema_order_stats_df = compute_schema_order_stats(schema_order_stats_df, file_order_df,
                                                               annotation_directory)

    return file_order_stats_df, schema_order_stats_df


def add_accuracy_columns(input_order_score_df: pd.DataFrame, base_index: int) -> pd.DataFrame:
    input_order_score_df.insert(base_index, 'precision', None)
    base_index += 1
    input_order_score_df.insert(base_index, 'recall', None)
    base_index += 1
    input_order_score_df.insert(base_index, 'f_measure', None)

    return input_order_score_df


def compute_precision_recall_f_measure(input_order_score_df: pd.DataFrame) -> pd.DataFrame:
    for i, row in input_order_score_df.iterrows():
        # get file stats
        assessed_order_count = row.assessed_order_count
        true_order_count = row.true_order_count
        distinct_true_order_count = row.distinct_true_order_count
        ref_order_count = row.ref_order_count
        precision = recall = None

        if assessed_order_count > 0:
            precision = true_order_count / assessed_order_count
            input_order_score_df.at[i, 'precision'] = precision
        if ref_order_count > 0:
            recall = distinct_true_order_count / ref_order_count
            input_order_score_df.at[i, 'recall'] = recall
        if (precision is not None) and (recall is not None):
            if precision + recall > 0:
                f_measure = (2 * precision * recall) / (precision + recall)
                input_order_score_df.at[i, 'f_measure'] = f_measure

    return input_order_score_df


def score_order(input_order_stats_df: pd.DataFrame, input_unit: str) -> pd.DataFrame:
    input_order_score_df = input_order_stats_df.copy()

    if input_unit == 'file':
        base_index = 4
    elif input_unit == 'schema':
        base_index = 5
    else:
        sys.exit('Error input unit, please use file or schema')

    input_order_score_df = add_accuracy_columns(input_order_score_df, base_index)

    input_order_score_df = compute_precision_recall_f_measure(input_order_score_df)

    return input_order_score_df


def score_file_and_schema_order(file_order_stats_df: pd.DataFrame, schema_order_stats_df: pd.DataFrame) \
        -> Tuple[pd.DataFrame, pd.DataFrame]:
    file_order_score_df = score_order(file_order_stats_df, input_unit='file')
    # compute schema based event score
    schema_order_score_df = score_order(schema_order_stats_df, input_unit='schema')

    return file_order_score_df, schema_order_score_df


def score(ta2_team_name: str, mode: str, ta2_ext: bool, ann_ext: bool) -> None:
    if mode == 'test':
        target_pairs = []
    else:
        target_pairs = ['cmu-cmu', 'isi-cmu', 'ibm-ibm', 'isi-ibm',
                        'jhu-jhu', 'sbu-jhu', 'resin-resin', 'sbu-resin']

    ta2_team_name = ta2_team_name.upper()

    # get path of directories
    base_dir = ''
    try:
        config = configparser.ConfigParser()
        base_dir = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(base_dir, "../config.ini")) as configfile:
            config.read_file(configfile)
    except:
        sys.exit('CAN NOT OPEN CONFIG FILE: {0}'.format(os.path.join(os.path.dirname(base_dir), "../config.ini")))

    if mode == 'evaluation':
        ta2_task1_output_directory = config['Phase1Eval']['ta2_task1_output_directory']
        task1_score_directory = config['Phase1Eval']['task1_score_directory']
        assessment_directory = config['Phase1Eval']['assessment_directory']
        annotation_directory = config['Phase1Eval']['annotation_directory']
    elif mode == 'test':
        ta2_task1_output_directory = config['Test']['ta2_task1_output_directory']
        task1_score_directory = config['Test']['task1_score_directory']
        assessment_directory = config['Test']['assessment_directory']
        annotation_directory = config['Test']['annotation_directory']
    else:
        sys.exit('Please enter the correct mode: evaluation or test')

    if not os.path.isdir(ta2_task1_output_directory):
        sys.exit('Input directory not found: {0}'.format(ta2_task1_output_directory))
    if not os.path.isdir(task1_score_directory):
        sys.exit('Output directory not found: {0}, Please run score_task1_event_argument.py first!'.format(
            task1_score_directory))
    if not os.path.isdir(assessment_directory):
        sys.exit('Input directory not found: {0}'.format(assessment_directory))
    if not os.path.isdir(annotation_directory):
        sys.exit('Input directory not found: {0}'.format(annotation_directory))

    print('---------------------------------------------------------')
    if ta2_ext:
        print('Start to extract order from TA2_{0} Task1 json outputs...'.format(ta2_team_name))
        time.sleep(0.1)
        extract_order.extract_ta2_order_from_json(ta2_team_name, ta2_task1_output_directory,
                                                  task1_score_directory, target_pairs)
        print('Extraction completed!')
    else:
        print('Skip order extraction from TA2_{0} Task1 outputs.'.format(ta2_team_name))

    print('---------------------------------------------------------')
    if ann_ext:
        print('Start to extract reference order from temporal annotations...')
        time.sleep(0.1)
        extract_order.extract_reference_order(annotation_directory)
        print('Extraction completed!')
    else:
        print('Skip reference order extraction from temporal annotations.')

    print('-----------------------------------------------------------------------------')
    print('Start to score TA2_{0} Task1 orders in terms of precision, recall, and F measure...'.format(ta2_team_name))
    time.sleep(0.1)
    file_order_stats_df, schema_order_stats_df = compute_order_stats(ta2_team_name, task1_score_directory,
                                                                     annotation_directory)
    file_order_score_df, schema_order_score_df = score_file_and_schema_order(
        file_order_stats_df, schema_order_stats_df)

    # save order scores as excel file
    file_order_score_df.to_excel(
        '{0}{1}/task1_file_score_order_{2}.xlsx'.format(task1_score_directory, ta2_team_name, ta2_team_name),
        index=False)
    schema_order_score_df.to_excel(
        '{0}{1}/task1_schema_score_order_{2}.xlsx'.format(task1_score_directory, ta2_team_name, ta2_team_name),
        index=False)
    print('Scoring finished!')


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
        description='Task1 scorer for order'
    )

    parser.add_argument('-t', '--ta2-team-name', help='supported ta2 team names are: CMU, IBM, JHU, and RESIN',
                        required=True, type=str)
    parser.add_argument('-m', '--mode', help='the mode of the scorer: test or evaluation',
                        required=False, type=str, default='evaluation')
    parser.add_argument('-t2e', '--ta2_ext', help='whether to conduct ta2 output extraction',
                        required=False, type=str2bool, default=True)
    parser.add_argument('-ae', '--ann_ext', help='whether to conduct annotation temporal extraction to get '
                                                 'reference orders',
                        required=False, type=str2bool, default=True)
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
    # cli_parser()

    mode = 'evaluation'
    ta2_ext = False
    ann_ext = False

    # ta2_team_name = 'cmu'
    # score(ta2_team_name, mode, ta2_ext, ann_ext)
    # ta2_team_name = 'ibm'
    # score(ta2_team_name, mode, ta2_ext, ann_ext)
    ta2_team_name = 'jhu'
    score(ta2_team_name, mode, ta2_ext, ann_ext)
    # ta2_team_name = 'resin'
    # score(ta2_team_name, mode, ta2_ext, ann_ext)
