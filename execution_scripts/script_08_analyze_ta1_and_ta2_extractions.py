#!/usr/bin/env python3

import configparser
import os
import sys
import pandas as pd
import argparse

from kevs.TA1Library import TA1Collection
from kevs.TA2Instantiation import TA2Collection
from kevs.Annotation import Annotation
from kevs.Assessment import Assessment
from kevs.compare_graph_g_versions import get_graph_g_comparison
from kevs.compare_assment_versions import get_assessment_comparison
from kevs.validate_ta1_references_by_ta2 import validate_ta1_references_by_ta2


def generate_analysis(config_filepath: str, config_mode: str, score_tasks: str) -> None:
    try:
        config = configparser.ConfigParser()
        with open(config_filepath) as configfile:
            config.read_file(configfile)
    except ImportError:
        sys.exit('CAN NOT OPEN CONFIG FILE: ' + config_filepath)

    root_dir = config[config_mode]["root_dir"]
    input_subdir = config[config_mode]["input_subdir"]
    output_subdir = config[config_mode]["output_subdir"]
    eval_phase_subdir = config[config_mode]["eval_phase_subdir"]
    include_all_events = (config[config_mode]['include_all_events'].lower() == "true")
    input_dir_prefix = os.path.join(root_dir, input_subdir, eval_phase_subdir)
    output_dir_prefix = os.path.join(root_dir, output_subdir, eval_phase_subdir)

    # Subfolders of root_directory/input_subdir/eval_phase_subdir
    task1_annotation_dir = os.path.join(input_dir_prefix,
                                        config[config_mode]["task1_annotation_subdir"])
    task2_annotation_dir = os.path.join(input_dir_prefix,
                                        config[config_mode]["task2_annotation_subdir"])
    assessment_dir = os.path.join(input_dir_prefix,
                                  config[config_mode]["assessment_subdir"])

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

    # if TA1 or TA2 extraction directories do not exist, throw error message
    if not os.path.isdir(ta1_score_dir) or not os.path.isdir(ta2_task1_score_dir) \
            or not os.path.isdir(ta2_task2_score_dir):
        sys.exit('Directory not found: ' + ta1_score_dir + ' or ' + ta2_task1_score_dir +
                 ' or ' + ta2_task2_score_dir)
    # if TA1 analysis directories do not exist, create them
    if not os.path.isdir(ta1_analysis_dir):
        os.makedirs(ta1_analysis_dir)
    # if TA2 Task1 analysis directories do not exist, create them
    if not os.path.isdir(ta2_task1_analysis_dir):
        os.makedirs(ta2_task1_analysis_dir)
    # if TA2 Task2 analysis directories do not exist, create them
    if not os.path.isdir(ta2_task2_analysis_dir):
        os.makedirs(ta2_task2_analysis_dir)

    # TA1
    ta1_collection = TA1Collection()
    print("Importing TA1 Collection")
    ta1_collection.import_extractions_from_file_collection(ta1_score_dir)
    if score_tasks == "all" or score_tasks == "ta1":
        print("Producing TA1 Event Trees")
        ta1_collection.produce_event_trees(ta1_score_dir)

    # TA2 Task 1
    ta2_task1_collection = TA2Collection(is_task2=False)
    print("Importing TA2 Task 1 Collection")
    ta2_task1_collection.import_extractions_from_file_collection(ta2_task1_score_dir)
    if score_tasks == "all" or score_tasks == "ta2task1":
        print("Validating TA1 References")
        validate_ta1_references_by_ta2(ta2_task1_analysis_dir, ta1_collection,
                                       ta2_task1_collection)
        print("Producing TA2 Task 1 Event Trees")
        ta2_task1_collection.produce_event_trees(ta2_task1_score_dir, include_all_events)

    # TA2 Task 2
    ta2_task2_collection = TA2Collection(is_task2=True)
    print("Importing TA2 Task 2 Collection")
    ta2_task2_collection.import_extractions_from_file_collection(ta2_task2_score_dir)
    if score_tasks == "all" or score_tasks == "ta2task2":
        print("Validating TA1 References")
        validate_ta1_references_by_ta2(ta2_task2_analysis_dir, ta1_collection,
                                       ta2_task2_collection)
        print("Producing TA2 Task 2 Event Trees")
        ta2_task2_collection.produce_event_trees(ta2_task2_score_dir, include_all_events)

    # Graph G
    graph_g_collection = TA2Collection(is_task2=True)
    print("Importing Graph G Collection")
    graph_g_collection.import_extractions_from_file_collection(graph_g_extraction_dir,
                                                               extract_for_graph_g=True)
    if score_tasks == "all" or score_tasks == "graphg":
        print("Producing Graph G Event Trees")
        graph_g_collection.produce_event_trees(graph_g_extraction_dir, include_all_events=True)

    print("Importing Annotations")
    task1_annotation_collection = Annotation(is_task2=False)
    task1_annotation_collection.import_all_annotation(task1_annotation_dir)
    task2_annotation_collection = Annotation(is_task2=True)
    task2_annotation_collection.import_all_annotation(task2_annotation_dir)

    print("Importing Task 1 Assessment")
    assessment_collection = Assessment()
    assessment_collection.import_assessment_data(assessment_dir)

    # if TA2 Task1 analysis directories do not exist, create them
    ta2_t2_as_path = os.path.join(ta2_task2_analysis_dir, "Graph_G_Comparison")
    ta2_t2_asm_path = os.path.join(ta2_task2_analysis_dir, "Automated_Scoring")
    ta2_t1_as_path = os.path.join(ta2_task1_analysis_dir, "Assessment_Comparison")
    ta2_t1_asm_path = os.path.join(ta2_task1_analysis_dir, "Automated_Scoring")

    if not os.path.isdir(ta2_t2_as_path):
        os.makedirs(ta2_t2_as_path)
    if not os.path.isdir(ta2_t2_asm_path):
        os.makedirs(ta2_t2_asm_path)
    if not os.path.isdir(ta2_t1_as_path):
        os.makedirs(ta2_t1_as_path)
    if not os.path.isdir(ta2_t1_asm_path):
        os.makedirs(ta2_t1_asm_path)

    # Producing Composite Graph G Analysis comparing abridged vs. no suffix
    if score_tasks == "all" or score_tasks == "task2":
        print("Comparing TA2 Task2 Graphs")
        get_graph_g_comparison(ta2_t2_as_path, task1_annotation_collection, ta2_task2_collection,
                               graph_g_collection, task2_annotation_dir, ta2_t2_asm_path)
    else:
        print("No Graph G Submissions to Score")

    if score_tasks == "all" or score_tasks == "task1":
        print("Comparing TA2 Task1 Graphs")
        get_assessment_comparison(ta2_t1_as_path, task1_annotation_collection, ta2_task1_collection,
                                  assessment_collection, task1_annotation_dir, ta2_t1_asm_path)

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
    generate_analysis(config_filepath, config_mode, score_tasks)


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
