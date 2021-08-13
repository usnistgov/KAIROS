#!/usr/bin/env python3

__author__ = "Xiongnan Jin (xiongnan.jin@nist.gov)"
__version__ = "Development: 0.5.2"
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

########################################################################################
# extract entity relation information from TA2 output JSON files and save as csv files
# read extracted order csv files and compute recall score
########################################################################################
import argparse
import configparser
import copy
import logging
import os
import sys
import time

import pandas as pd
from tqdm import tqdm
from scripts import extract_relation

scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../scripts")
sys.path.append(scripts_path)


def initiate_rel_score_df():
    file_score_rel_df = pd.DataFrame(columns=['file_name', 'CE', 'TA2', 'TA1', 'file_rel_count',
                                              'file_grel_count', 'grel_count'])
    schema_score_rel_df = pd.DataFrame(columns=['file_name', 'schema_id', 'schema_super', 'CE', 'TA2', 'TA1', 'schema_rel_count',
                                                'schema_grel_count', 'grel_count'])

    return file_score_rel_df, schema_score_rel_df


# def assess_er_df(er_df: pd.DataFrame, annotation_directory: str, target_ce: str):
#     ger_df = pd.read_excel(annotation_directory + target_ce + '_relations_selected.xlsx')
#     if 'assessment' not in er_df.columns:
#         er_df.insert(1, 'assessment', False)
#     for i, row in er_df.iterrows():
#         rel_id = row.rel_relationProvenance
#         rel_ta1ref = row.rel_ta1ref
#         # if ta1ref points to a proper ta1 team name and relation_id, subject_id, and object_id match graph G,
#         # then the relation is assessed as True
#         if ':' in rel_ta1ref:
#             prefix = rel_ta1ref.split(':')[0]
#             if prefix.lower() in ['cmu', 'ibm', 'isi', 'jhu', 'resin', 'sbu'] and rel_id in ger_df.relation_id.unique():
#                 sub_id = row.er_provenance
#                 obj_id = row.rel_provenance
#                 rarg_df = pd.read_excel(annotation_directory + target_ce + '_arguments.xlsx')
#                 # if subject id and object id both are graphG arg_id
#                 if sub_id in rarg_df.arg_id.unique() and obj_id in rarg_df.arg_id.unique():
#                     rel_arg_df = rarg_df.loc[rarg_df['relation_id'] == rel_id]
#                     if (sub_id == rel_arg_df.iloc[0]['arg_id'] and obj_id == rel_arg_df.iloc[1]['arg_id']) or \
#                             (sub_id == rel_arg_df.iloc[1]['arg_id'] and obj_id == rel_arg_df.iloc[0]['arg_id']):
#                         er_df.at[i, 'assessment'] = True
#                         ger_df = ger_df.loc[ger_df['relation_id'] != rel_id]
#                 # if subject id and object id both are graphG entity_id
#                 if sub_id in rarg_df.entity_id.unique() and obj_id in rarg_df.entity_id.unique():
#                     rel_arg_df = rarg_df.loc[rarg_df['relation_id'] == rel_id]
#                     if (sub_id == rel_arg_df.iloc[0]['entity_id'] and
#                         obj_id == rel_arg_df.iloc[1]['entity_id']) or \
#                             (sub_id == rel_arg_df.iloc[1]['entity_id'] and
#                              obj_id == rel_arg_df.iloc[0]['entity_id']):
#                         er_df.at[i, 'assessment'] = True
#                         ger_df = ger_df.loc[ger_df['relation_id'] != rel_id]
#
#     return er_df


def is_ta1ref_valid(rel_ta1ref) -> bool:
    if isinstance(rel_ta1ref, str):
        prefix = rel_ta1ref.split(':')[0]
        prefix = prefix.lower()
        if prefix in ['cmu', 'ibm', 'isi', 'jhu', 'resin', 'sbu']:
            return True

    return False


def compute_unit_rel_stats(grel_df: pd.DataFrame, sys_rel_df: pd.DataFrame, sys_rel_stats_df: pd.DataFrame,
                           unit: str) -> pd.DataFrame:
    sys_grel_df = pd.DataFrame()
    file_name = sys_rel_df.iloc[0]['file_name']
    fn_list = file_name.split('-')
    ta1 = fn_list[0].upper()
    ta2 = fn_list[1].upper()
    target_ce = fn_list[2]

    cur_grel_df = copy.deepcopy(grel_df)
    for i, sys_rel_row in sys_rel_df.iterrows():
        rel_subject_provenance = sys_rel_row.rel_subject_provenance
        rel_predicate = sys_rel_row.rel_predicate
        rel_object_provenance = sys_rel_row.rel_object_provenance
        rel_provenance = sys_rel_row.rel_provenance
        rel_ta1ref = sys_rel_row.rel_ta1ref
        if is_ta1ref_valid(rel_ta1ref):
            if (not isinstance(rel_subject_provenance, str)) or (not isinstance(rel_predicate, str)) or \
                    (not isinstance(rel_object_provenance, str)) or (not isinstance(rel_provenance, str)):
                match_df = pd.DataFrame()
            else:
                match_df = cur_grel_df.loc[
                    cur_grel_df['rel_subject_provenance'] == rel_subject_provenance &
                    cur_grel_df['rel_predicate'] == rel_predicate &
                    cur_grel_df['rel_object_provenance'] == rel_object_provenance &
                    cur_grel_df['rel_provenance'] == rel_provenance
                    ]

            if len(match_df) > 0:
                sys_grel_df = sys_grel_df.append(sys_rel_row, ignore_index=True)
                index = match_df.index.values[0]
                cur_grel_df.drop(index=index)
                example_directory = '../../../../Phase_1_evaluation/Examples/' + ta2 + '/'
                if not os.path.isdir(example_directory):
                    os.makedirs(example_directory)
                example_fp = example_directory + ta2 + '_ta2_grel.xlsx'
                if not os.path.isfile(example_fp):
                    example: pd.Series = sys_rel_row.take([0, 2, 3, 6, 7, 9, 11, 13, 14], axis=1)
                    example.to_excel(example_fp, header=False)
            else:
                example_directory = '../../../../Phase_1_evaluation/Examples/' + ta2 + '/'
                if not os.path.isdir(example_directory):
                    os.makedirs(example_directory)
                example_fp = example_directory + ta2 + '_ta2_grel_no_matching_grel.xlsx'
                if not os.path.isfile(example_fp):
                    example: pd.Series = sys_rel_row.take([0, 2, 3, 6, 7, 9, 11, 13, 14], axis=1)
                    example.to_excel(example_fp, header=False)
        else:
            example_directory = '../../../../Phase_1_evaluation/Examples/' + ta2 + '/'
            if not os.path.isdir(example_directory):
                os.makedirs(example_directory)
            example_fp = example_directory + ta2 + '_ta2_grel_ta1ref_not_valid.xlsx'
            if not os.path.isfile(example_fp):
                example: pd.Series = sys_rel_row.take([0, 2, 3, 6, 7, 9, 11, 13, 14], axis=1)
                example.to_excel(example_fp, header=False)

    sys_rel_count = len(sys_rel_df)
    sys_grel_count = len(sys_grel_df)
    grel_count = len(grel_df)
    if unit == 'file':
        rel_stats_row = {'file_name': file_name, 'CE': target_ce, 'TA2': ta2, 'TA1': ta1,
                         '{0}_rel_count'.format(unit): sys_rel_count, '{0}_grel_count'.format(unit): sys_grel_count,
                         'grel_count': grel_count}
    elif unit == 'schema':
        schema_id = sys_rel_df.iloc[0]['schema_id']
        rel_stats_row = {'file_name': file_name, 'schema_id': schema_id, 'CE': target_ce, 'TA2': ta2, 'TA1': ta1,
                         '{0}_rel_count'.format(unit): sys_rel_count, '{0}_grel_count'.format(unit): sys_grel_count,
                         'grel_count': grel_count}
    else:
        sys.exit('Unit error: ' + unit)

    sys_rel_stats_df = sys_rel_stats_df.append(rel_stats_row, ignore_index=True)

    return sys_rel_stats_df


def compute_rel_stats(ta2_team_name: str, score_directory: str, graph_g_directory: str):
    file_rel_stats_df, schema_rel_stats_df = initiate_rel_score_df()

    rel_directory = score_directory + ta2_team_name + '/Relations/'
    for file_name in tqdm(os.listdir(rel_directory), position=0, leave=False):
        fn_suffix = file_name[-4:]
        if fn_suffix != '.csv':
            continue

        # compute file entity relation stats
        file_rel_df = pd.read_csv(rel_directory + file_name)
        # if it is task 1 file, then skip
        task2 = False
        if 'task2' in file_name:
            task2 = True
        if not task2:
            continue
        # compute file based entity relation score
        # find ta1 schema library from super keyword (first schema)
        target_ce = file_name.split('-')[2]
        grel_fn = graph_g_directory + target_ce + '/' + target_ce + '_GraphG_rel.csv'
        if not os.path.isfile(grel_fn):
            # print('Cannot find the graph G csv file ' + grel_fn)
            continue
        else:
            grel_df = pd.read_csv(grel_fn)

        file_rel_stats_df = compute_unit_rel_stats(grel_df, file_rel_df, file_rel_stats_df, 'file')
        # compute schema based entity relation score
        schema_list = file_rel_df.schema_id.unique()
        if len(schema_list) > 0:
            grouped = file_rel_df.groupby(file_rel_df.schema_id)
            for schema in schema_list:
                schema_rel_df = grouped.get_group(schema)
                schema_rel_stats_df = compute_unit_rel_stats(grel_df, schema_rel_df, schema_rel_stats_df, 'schema')

    return file_rel_stats_df, schema_rel_stats_df


def compute_unit_rel_score(sys_rel_stats_df: pd.DataFrame, unit: str) -> pd.DataFrame:
    if unit == 'file':
        base = 4
    elif unit == 'schema':
        base = 5
    else:
        sys.exit('Unit error: ' + unit)
    sys_rel_stats_df.insert(base, 'recall', -1.0)
    for i, row in sys_rel_stats_df.iterrows():
        molecular = sys_rel_stats_df.at[i, '{0}_grel_count'.format(unit)]
        denominator = sys_rel_stats_df.at[i, 'grel_count']
        recall = None
        if denominator != 0:
            recall = molecular / denominator
        sys_rel_stats_df.at[i, 'recall'] = recall

    return sys_rel_stats_df


def compute_rel_score(file_rel_stats_df: pd.DataFrame, schema_rel_stats_df: pd.DataFrame):
    # compute file based recall
    file_rel_score_df = compute_unit_rel_score(file_rel_stats_df, 'file')

    # compute schema based recall
    schema_rel_score_df = compute_unit_rel_score(schema_rel_stats_df, 'schema')

    return file_rel_score_df, schema_rel_score_df


def score_f_measure(score_df: pd.DataFrame):
    score_df.insert(6, 'f_measure', -1.0)
    # compute f measure
    for i, row in score_df.iterrows():
        precision = row.get('precision')
        recall = row.get('recall')
        if precision == 0 and recall == 0:
            f_measure = None
        else:
            f_measure = (2 * precision * recall) / (precision + recall)
        score_df.at[i, 'f_measure'] = f_measure

    return score_df


def score(ta2_team_name: str, graph_g_ext: bool, ta2_ext: bool, mode: str) -> None:
    print('Graph G extraction is set as ' + str(graph_g_ext))
    print('TA2 output extraction is set as ' + str(ta2_ext))
    ta2_team_name = ta2_team_name.upper()

    base_dir = ''
    try:
        config = configparser.ConfigParser()
        base_dir = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(base_dir, "../config.ini")) as configfile:
            config.read_file(configfile)
    except:
        sys.exit('CAN NOT OPEN CONFIG FILE: {0}'.format(os.path.join(os.path.dirname(base_dir), "../config.ini")))

    if mode == 'evaluation':
        ta2_output_directory = config['Phase1Eval']['ta2_task2_output_directory']
        task2_score_directory = config['Phase1Eval']['task2_score_directory']
        graph_g_directory = config['Phase1Eval']['graph_g_directory']
        annotation_directory = config['Phase1Eval']['annotation_directory']
    elif mode == 'test':
        ta2_output_directory = config['Test']['ta2_task2_output_directory']
        task2_score_directory = config['Test']['task2_score_directory']
        graph_g_directory = config['Test']['graph_g_directory']
        annotation_directory = config['Test']['annotation_directory']
    else:
        sys.exit('Please enter the correct mode: evaluation or test')

    if not os.path.exists(task2_score_directory):
        os.makedirs(task2_score_directory)

    if graph_g_ext:
        print('----------------------------------------------------------------------')
        print('Start to extract relations from graph G json files...')
        time.sleep(0.1)
        extract_relation.extract_graph_g_relation_from_json(graph_g_directory)
        print('Extraction completed!')
    else:
        print('----------------------------------------------------------------------')
        print('Skip relation extraction graph G.')

    if ta2_ext:
        print('----------------------------------------------------------------------')
        print('Start to extract relations from json outputs of {0}...'.format(ta2_team_name))
        time.sleep(0.1)
        extract_relation.extract_ta2_relation_from_json(ta2_team_name, ta2_output_directory,
                                                        task2_score_directory, [], annotation_directory)
        print('Extraction completed!')
    else:
        print('----------------------------------------------------------------------')
        print('Skip relation extraction from TA2 output.')

    print('------------------------------------------------------------------------------------------')
    print('Start to compute recall of relations for {0}...'.format(ta2_team_name))
    time.sleep(0.1)
    file_rel_stats_df, schema_rel_stats_df = compute_rel_stats(ta2_team_name, task2_score_directory,
                                                               graph_g_directory)
    file_rel_score_df, schema_rel_score_df = compute_rel_score(file_rel_stats_df, schema_rel_stats_df)
    # file_rel_stats_df = score_f_measure(file_rel_stats_df)
    # schema_rel_stats_df = score_f_measure(schema_rel_stats_df)

    print('Scoring finished!')

    # write to excel files
    if len(file_rel_score_df) == 0:
        print('No TA2 task2 score of relations from {0}'.format(ta2_team_name))
    else:
        file_rel_score_df.to_excel(
            '{0}{1}/task2_file_score_relation_{2}.xlsx'.format(task2_score_directory, ta2_team_name, ta2_team_name),
            index=False)
        schema_rel_score_df.to_excel(
            '{0}{1}/task2_schema_score_relation_{2}.xlsx'.format(task2_score_directory, ta2_team_name, ta2_team_name),
            index=False)


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
        description='Task2 scorer for relations'
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

    # ta2_team_name = 'ibm'
    # graph_g_ext = False
    # ta2_ext = False
    # mode = 'evaluation'
    # score(ta2_team_name, graph_g_ext, ta2_ext, mode)
