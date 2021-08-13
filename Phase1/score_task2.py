#!/usr/bin/env python3

__author__ = "Xiongnan Jin (xiongnan.jin@nist.gov)"
__version__ = "Development: 0.5.1"
__date__ = "01/22/2021"

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
# main entry for task 2 scorer
# executes ev&arg, order, relation scorers in sequence
##########################################################################################
import argparse
import logging
import os
import sys
from scripts import score_task2_event_argument, score_task2_order, score_task2_relation

scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "")
sys.path.append(scripts_path)


def score(ta2_team_name: str, graph_g_ext: bool, ta1_ext: bool, ta2_ext: bool, mode: str):
    score_task2_event_argument.score(ta2_team_name, graph_g_ext, ta1_ext, ta2_ext, mode)
    score_task2_order.score(ta2_team_name, graph_g_ext, ta2_ext, mode)
    score_task2_relation.score(ta2_team_name, graph_g_ext, ta2_ext, mode)


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


if __name__ == "__main__":
    cli_parser()

    # graph_g_ext = False
    # ta1_ext = True
    # ta2_ext = True
    # mode = 'test'
    #
    # ta2_team_name = 'cmu_simple'
    # score(ta2_team_name, graph_g_ext, ta1_ext, ta2_ext, mode)