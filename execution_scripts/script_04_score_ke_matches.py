#!/usr/bin/env python3

import configparser
import os
import sys
import pandas as pd
import argparse

from kevs.TA1Library import TA1Collection
from kevs.TA2Instantiation import TA2Collection
from kevs.Annotation import Annotation
from kevs.match_ke import match_ke_elements


def score_ke_matches(config_filepath: str, config_mode: str, score_tasks: str) -> None:
    try:
        config = configparser.ConfigParser()
        with open(config_filepath) as configfile:
            config.read_file(configfile)
    except ImportError:
        sys.exit('Cannot open config file: ' + config_filepath)

    root_dir = config[config_mode]["root_dir"]
    input_subdir = config[config_mode]["input_subdir"]
    output_subdir = config[config_mode]["output_subdir"]
    eval_phase_subdir = config[config_mode]["eval_phase_subdir"]
    input_dir_prefix = os.path.join(root_dir, input_subdir, eval_phase_subdir)
    output_dir_prefix = os.path.join(root_dir, output_subdir, eval_phase_subdir)

    # Subfolders of root_directory/input_subdir/eval_phase_subdir
    task1_annotation_dir = os.path.join(input_dir_prefix,
                                        config[config_mode]["task1_annotation_subdir"])
    task2_annotation_dir = os.path.join(input_dir_prefix,
                                        config[config_mode]["task2_annotation_subdir"])

    # subfolders of root_directory/output_subdir/eval_phase_subdir
    # use ta1 for later: ta1_score_dir = os.path.join(output_dir_prefix,
    #                             config[config_mode]["ta1_score_subdir"])
    ta1_score_dir = os.path.join(output_dir_prefix,
                                 config[config_mode]["ta1_score_subdir"])
    ta2_task1_score_dir = os.path.join(output_dir_prefix,
                                       config[config_mode]["ta2_task1_score_subdir"])
    ta2_task2_score_dir = os.path.join(output_dir_prefix,
                                       config[config_mode]["ta2_task2_score_subdir"])
    # for later: ta1_analysis_dir = os.path.join(output_dir_prefix,
    # for later                                config[config_mode]["ta1_analysis_subdir"])
    ta2_task1_analysis_dir = os.path.join(output_dir_prefix,
                                          config[config_mode]["ta2_task1_analysis_subdir"])
    ta2_task2_analysis_dir = os.path.join(output_dir_prefix,
                                          config[config_mode]["ta2_task2_analysis_subdir"])
    graph_g_extraction_dir = os.path.join(output_dir_prefix,
                                          config[config_mode]["graph_g_extraction_subdir"])
    graph_g_analysis_dir = os.path.join(output_dir_prefix,
                                        config[config_mode]["graph_g_analysis_subdir"])
    qnode_dir = os.path.join(output_dir_prefix,
                             config[config_mode]["qnode_subdir"])

    task1_complex_event_list = os.listdir(task1_annotation_dir)
    task1_complex_event_list.remove('.DS_Store')

    task2_complex_event_list = os.listdir(task2_annotation_dir)
    task2_complex_event_list.remove('.DS_Store')

    # the cache file for qnode similarity scores
    qnode_sim_fp = os.path.join(qnode_dir, 'nist_isi_qnode_sim_cache.tsv')

    # if TA2 Task1 analysis directories do not exist, create them
    # For later: ta1_as_path = os.path.join(ta1_analysis_dir, "Automated_Scoring")
    ta2_t1_as_path = os.path.join(ta2_task1_analysis_dir, "Automated_Scoring")
    ta2_t2_as_path = os.path.join(ta2_task2_analysis_dir, "Automated_Scoring")
    ta2_g_as_path = os.path.join(graph_g_analysis_dir, "Automated_Scoring")

    # if TA2 Task2 analysis directories do not exist, create them
    # if not os.path.isdir(ta2_t2_as_path):
    #    os.makedirs(ta2_t2_as_path)

    # Annotation must be importated for all tasks
    print("Importing Annotations")
    task1_annotation_collection = Annotation(is_task2=False)
    task1_annotation_collection.import_all_annotation(task1_annotation_dir)
    task2_annotation_collection = Annotation(is_task2=True)
    task2_annotation_collection.import_all_annotation(task2_annotation_dir)

    print("Importing TA2 Task 1 Submissions")
    ta2_task1_collection = TA2Collection(is_task2=False)
    if score_tasks == "all" or score_tasks == "ta2task1":
        if not os.path.isdir(ta2_task1_score_dir):
            sys.exit('Directory not found: ' + ta2_task1_score_dir)
        ta2_task1_collection.import_extractions_from_file_collection(ta2_task1_score_dir)

    print("Importing TA2 Task 2 Submissions")
    ta2_task2_collection = TA2Collection(is_task2=True)
    if score_tasks == "all" or score_tasks == "ta2task2":
        if not os.path.isdir(ta2_task2_score_dir):
            sys.exit('Directory not found: ' + ta2_task2_score_dir)
        ta2_task2_collection.import_extractions_from_file_collection(ta2_task2_score_dir)

    # Graph G must be imported for multiple tasks
    print("Importing Graph G")
    graph_g_collection = TA2Collection(is_task2=True)
    if score_tasks == "all" or score_tasks == "graphg" or \
            score_tasks == "ta2task1" or score_tasks == "ta2task2":
        graph_g_collection.import_extractions_from_file_collection(graph_g_extraction_dir,
                                                                   extract_for_graph_g=True)

    if score_tasks == "all" or score_tasks == "ta2task2":
        print("Automatically Scoring TA2 Task 2 Output")
        if ta2_task2_collection.ta2dict:
            match_ke_elements(ta2_t2_as_path, task1_annotation_collection, ta2_task2_collection,
                              graph_g_collection, qnode_dir,
                              qnode_sim_fp, is_task2=True, use_graph_g=False,
                              min_confidence_threshold=0)
        else:
            print("No TA2 Task 2 Submissions to Score")

    if score_tasks == "all" or score_tasks == "ta2task1":
        print("Automatically Scoring TA2 Task 1 Output")
        if ta2_task1_collection.ta2dict:
            match_ke_elements(ta2_t1_as_path, task1_annotation_collection, ta2_task1_collection,
                              graph_g_collection, qnode_dir,
                              qnode_sim_fp, is_task2=False, use_graph_g=False,
                              min_confidence_threshold=0)
            pass
        else:
            print("No TA2 Task 1 Submissions to Score")

    if score_tasks == "all" or score_tasks == "graphg":
        print("Automatically Scoring Graph G")
        if graph_g_collection.ta2dict:
            match_ke_elements(ta2_g_as_path, task1_annotation_collection, graph_g_collection,
                              graph_g_collection, qnode_dir,
                              qnode_sim_fp, is_task2=True, use_graph_g=True,
                              min_confidence_threshold=0)
            pass
        else:
            print("No Graph G Submissions to Score")

    print("Importing TA1 Libraries")
    ta1_collection = TA1Collection()
    if score_tasks == "all" or score_tasks == "ta1":
        ta1_collection.import_extractions_from_file_collection(ta1_score_dir)

    if score_tasks == "all" or score_tasks == "ta1":
        print("Automatically Scoring Ta1")
        if ta1_collection.ta1dict:
            #    match_ta1_ke_elements(ta1_as_path, task1_annotation_collection, ta1_collection,
            #                          graph_g_collection, qnode_dir,
            #                          qnode_sim_fp, is_task2=True, use_graph_g=False,
            #                          min_confidence_threshold=0)
            pass
        else:
            print("No TA1 Submissions to Score")


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
    score_ke_matches(config_filepath, config_mode, score_tasks)


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
