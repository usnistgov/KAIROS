#!/usr/bin/env python3

import configparser
import os
import sys
import pandas as pd
import argparse

from kevs.generate_stats import compute_ta1_submission_stats, compute_ta2_submission_stats


def compute_submission_stats(config_filepath: str, config_mode: str, score_tasks: str) -> None:
    try:
        config = configparser.ConfigParser()
        with open(config_filepath) as configfile:
            config.read_file(configfile)
    except ImportError:
        sys.exit('CAN NOT OPEN CONFIG FILE: ' + config_filepath)

    root_dir = config[config_mode]["root_dir"]
    output_subdir = config[config_mode]["output_subdir"]
    eval_phase_subdir = config[config_mode]["eval_phase_subdir"]
    output_dir_prefix = os.path.join(root_dir, output_subdir, eval_phase_subdir)

    # Subfolders of root_directory/input_subdir/eval_phase_subdir

    # subfolders of root_directory/output_subdir/eval_phase_subdir
    ta1_score_dir = os.path.join(output_dir_prefix,
                                 config[config_mode]["ta1_score_subdir"])
    ta2_task1_score_dir = os.path.join(output_dir_prefix,
                                       config[config_mode]["ta2_task1_score_subdir"])
    ta2_task2_score_dir = os.path.join(output_dir_prefix,
                                       config[config_mode]["ta2_task2_score_subdir"])
    ta1_analysis_dir = os.path.join(output_dir_prefix,
                                    config[config_mode]["ta1_analysis_subdir"])
    ta2_task1_analysis_dir = os.path.join(output_dir_prefix,
                                          config[config_mode]["ta2_task1_analysis_subdir"])
    ta2_task2_analysis_dir = os.path.join(output_dir_prefix,
                                          config[config_mode]["ta2_task2_analysis_subdir"])
    graph_g_extraction_dir = os.path.join(output_dir_prefix,
                                          config[config_mode]["graph_g_extraction_subdir"])
    graph_g_analysis_dir = os.path.join(output_dir_prefix,
                                        config[config_mode]["graph_g_analysis_subdir"])

    # if TA1 or TA2 extraction directories do not exist, throw error message
    if not os.path.isdir(ta1_score_dir) or not os.path.isdir(ta2_task1_score_dir) \
            or not os.path.isdir(ta2_task2_score_dir):
        sys.exit('Directory not found: ' + ta1_score_dir + ' or ' + ta2_task1_score_dir +
                 ' or ' + ta2_task2_score_dir)

    # if TA1 analysis directories do not exist, create them
    if not os.path.isdir(ta1_analysis_dir):
        os.makedirs(ta1_analysis_dir)
    else:
        pass

    if score_tasks == "all" or score_tasks == "ta1":
        # compute ta1 submission stats
        print("Computing TA1 Stats")
        ta1_stats_df, ta1_qnode_df, ta1_ent_ev_df = \
            compute_ta1_submission_stats(ta1_score_dir, ta1_analysis_dir)

    # # if TA2 Task1 analysis directories do not exist, create them
    if not os.path.isdir(ta2_task1_analysis_dir):
        os.makedirs(ta2_task1_analysis_dir)
    else:
        pass

    # compute ta2 submission stats
    if score_tasks == "all" or score_tasks == "ta2task1":
        print("Computing TA2 Task 1 Stats")
        ta2_stats_df, ta2_group_stats_df, ta2_qnode_df, ta2_ent_ev_df, ta2_ins_ent_ev_df = \
            compute_ta2_submission_stats(ta2_task1_score_dir, ta2_task1_analysis_dir)

    # # if TA2 Task2 analysis directories do not exist, create them
    if not os.path.isdir(ta2_task2_analysis_dir):
        os.makedirs(ta2_task2_analysis_dir)
    else:
        pass

    # compute ta2 submission stats
    if score_tasks == "all" or score_tasks == "ta2task2":
        print("Computing TA2 Task 2 Stats")
        ta2_stats_df, ta2_group_stats_df, ta2_qnode_df, ta2_ent_ev_df, ta2_ins_ent_ev_df = \
            compute_ta2_submission_stats(ta2_task2_score_dir, ta2_task2_analysis_dir)

    # # if TA2 GraphG analysis directories do not exist, create them
    if not os.path.isdir(graph_g_analysis_dir):
        os.makedirs(graph_g_analysis_dir)
    else:
        pass

    # compute ta2 submission stats
    if score_tasks == "all" or score_tasks == "graphg":
        print("Computing Graph_G Stats")
        ta2_stats_df, ta2_group_stats_df, ta2_qnode_df, ta2_ent_ev_df, ta2_ins_ent_ev_df = \
            compute_ta2_submission_stats(graph_g_extraction_dir, graph_g_analysis_dir,
                                         extract_for_graph_g=True)

    print("Done")


def define_parser():
    """
    Defines accepted CLI syntax and the actions to take for command and args.

    Returns:
        argparse parser

    """
    base_dir = os.path.dirname(os.path.realpath(__file__))
    default_config_filepath = os.path.join(base_dir, "config.ini")

    parser = argparse.ArgumentParser(
        description="Generate SDF Versions of Graph G"
    )
    parser.add_argument("-f", "--config_file", help='Location of Configuration file',
                        required=False, type=str, default=default_config_filepath)
    parser.add_argument("-m", "--config_mode", help='Mode of Configuration_file',
                        required=False, type=str, default="Default")
    parser.add_argument("-s", "--score_tasks",
                        help="Which tasks to score: 'all', 'ta1', 'ta2task1', 'ta2task2', " +
                             "or 'graphg'",
                        type=str, default="all")
    parser.add_argument('-v', '--verbose', help='Enable Verbose output',
                        required=False, action='store_true', default=False)

    # This tells the code to automatically execute this function
    parser.set_defaults(func=code_main)

    return parser


def code_main(args):
    config_filepath = args.config_file
    config_mode = args.config_mode
    score_tasks = args.score_tasks
    print("Config Filepath Used: {}".format(config_filepath))
    print("Config Mode Used: {}".format(config_mode))
    print("Tasks Scored: {}".format(score_tasks))
    compute_submission_stats(config_filepath, config_mode, score_tasks)


def main():
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)
    pd.set_option('display.max_colwidth', 200)
    parser = define_parser()
    args = parser.parse_args()
    if hasattr(args, "func") and args.func is not None:
        args.func(args)
    else:
        parser.print_help()

    # function code_main(args) is automatically called here


if __name__ == "__main__":
    main()
