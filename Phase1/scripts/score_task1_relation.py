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

#############################################################################################
# extract entity relation information from TA2 Task1 output JSON files and save as csv files
# assign LDC assessment to the extracted entity relation files
# read assessed entity relation csv files and compute precision, recall, and F1 score
#############################################################################################
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
from scripts import extract_relation
from scripts import score_task1_event_argument as st1ea

scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "")
sys.path.append(scripts_path)


def score(ta2_team_name: str, mode: str, ta2_ext: bool, rel_assess: bool):
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
        sys.exit('CAN NOT OPEN CONFIG FILE: {0}'.format(os.path.join(os.path.dirname(base_dir), "../config.ini")))

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

    # extract relation from json-ld outputs
    print('-----------------------------------------------------------')
    if ta2_ext:
        print('Start to extract REL from TA2_{0} Task1 json outputs...'.format(ta2_team_name))
        time.sleep(0.1)
        extract_relation.extract_ta2_relation_from_json(
            ta2_team_name, ta2_task1_output_directory, task1_score_directory, target_pairs, annotation_directory)
        print('Extraction completed!')
        print('Start to extract ENT from TA2_{0} Task1 json outputs...'.format(ta2_team_name))
        time.sleep(0.1)
        extract_relation.extract_ta2_relation_entity_from_json(
            ta2_team_name, ta2_task1_output_directory, task1_score_directory, target_pairs, annotation_directory)
        print('Extraction completed!')
    else:
        print('Skip REL and ENT extraction from TA2_{0} Task1 outputs.'.format(ta2_team_name))

    # assign LDC relation assessment
    print('-------------------------------------')
    if rel_assess:
        time.sleep(0.1)
        st1ea.assign_ke_assessment(ta2_team_name, task1_score_directory, assessment_directory,
                                   caci_directory, 'rel', target_pairs)
        print('REL assignment completed!')
        time.sleep(0.1)
        st1ea.assign_ke_assessment(ta2_team_name, task1_score_directory, assessment_directory,
                                   caci_directory, 'ent', target_pairs)
        print('ENT assignment completed!')
    else:
        print('Skip assignment of REL and ENT assessments for TA2_{0}'.format(ta2_team_name))

    print('----------------------------------------------------------------------------------------------------------')
    print('Start to score TA2_{0} Task1 REL in terms of precision, recall, and F measure...'.format(ta2_team_name))
    k = 20
    file_rel_stats_df, schema_rel_stats_df = st1ea.compute_ke_stats(ta2_team_name, task1_score_directory,
                                                                    annotation_directory, 'rel', k)
    file_rel_score_df, schema_rel_score_df = st1ea.score_file_and_schema_ke(file_rel_stats_df, schema_rel_stats_df,
                                                                            'rel', k)

    # write to files
    file_rel_score_df.to_excel(
        '{0}{1}/task1_file_score_relation_{2}.xlsx'.format(task1_score_directory, ta2_team_name, ta2_team_name),
        index=False)
    schema_rel_score_df.to_excel(
        '{0}{1}/task1_schema_score_relation_{2}.xlsx'.format(task1_score_directory, ta2_team_name, ta2_team_name),
        index=False)
    print('Scoring TA2_{0} Task1 REL finished!'.format(ta2_team_name))

    print('Start to score TA2_{0} Task1 ENT in terms of precision, recall, and F measure...'.format(ta2_team_name))
    file_ent_stats_df, schema_ent_stats_df = st1ea.compute_ke_stats(ta2_team_name, task1_score_directory,
                                                                    annotation_directory, 'ent', k)
    file_ent_score_df, schema_ent_score_df = st1ea.score_file_and_schema_ke(file_ent_stats_df, schema_ent_stats_df,
                                                                            'ent', k)

    # write to files
    file_ent_score_df.to_excel(
        '{0}{1}/task1_file_score_relation_entity_{2}.xlsx'.format(task1_score_directory, ta2_team_name, ta2_team_name),
        index=False)
    schema_ent_score_df.to_excel(
        '{0}{1}/task1_schema_score_relation_entity_{2}.xlsx'.format(task1_score_directory, ta2_team_name,
                                                                    ta2_team_name), index=False)
    print('Scoring TA2_{0} Task1 ENT finished!'.format(ta2_team_name))


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
        description='Task1 scorer for relations'
    )

    parser.add_argument('-t', '--ta2-team-name', help='supported ta2 team names are: CMU, IBM, JHU, and RESIN',
                        required=True, type=str)
    parser.add_argument('-t2e', '--ta2_ext', help='whether to conduct ta2 output extraction',
                        required=False, type=str2bool, default=True)
    parser.add_argument('-ra', '--rel_assess', help='whether to conduct assigning of '
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
    rel_assess = True

    # ta2_team_name = 'cmu'
    # score(ta2_team_name, mode, ta2_ext, rel_assess)
    # ta2_team_name = 'ibm'
    # score(ta2_team_name, mode, ta2_ext, rel_assess)
    # ta2_team_name = 'jhu'
    # score(ta2_team_name, mode, ta2_ext, rel_assess)
    ta2_team_name = 'resin'
    score(ta2_team_name, mode, ta2_ext, rel_assess)
