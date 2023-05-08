#!/usr/bin/env python3

import os
import sys
import pandas as pd
import argparse
import configparser

from kevs.TA1Library import TA1Library, TA1Collection
from kevs.TA2Instantiation import TA2Instantiation, TA2Collection


def extract_sdf(config_filepath: str, config_mode: str, score_tasks: str):
    # get path of directories
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
    ta1_submission_dir = os.path.join(input_dir_prefix,
                                      config[config_mode]["ta1_submission_subdir"])
    ta2_task1_submission_dir = os.path.join(input_dir_prefix,
                                            config[config_mode]["ta2_task1_submission_subdir"])
    ta2_task2_submission_dir = os.path.join(input_dir_prefix,
                                            config[config_mode]["ta2_task2_submission_subdir"])

    # subfolders of root_directory/output_subdir/eval_phase_subdir
    ta1_score_dir = os.path.join(output_dir_prefix,
                                 config[config_mode]["ta1_score_subdir"])
    ta2_task1_score_dir = os.path.join(output_dir_prefix,
                                       config[config_mode]["ta2_task1_score_subdir"])
    ta2_task2_score_dir = os.path.join(output_dir_prefix,
                                       config[config_mode]["ta2_task2_score_subdir"])
    graph_g_dir = os.path.join(output_dir_prefix,
                               config[config_mode]["graph_g_subdir"])
    graph_g_extraction_dir = os.path.join(output_dir_prefix,
                                          config[config_mode]["graph_g_extraction_subdir"])
    # TA1
    ta1_collection = TA1Collection()
    if score_tasks == "ta1" or score_tasks == "all":
        print("Extracting TA1 Libraries from SDF")
        try:
            cmu_ta1_library = TA1Library("CMU")
            cmu_ta1_library.extract_contents_from_sdf(ta1_submission_dir)
            cmu_ta1_library.write_extractions_to_files(ta1_score_dir)
            ta1_collection.ta1dict['{}'.format("CMU")] = cmu_ta1_library
        except FileNotFoundError as e:
            print("Warning: TA1 CMU Submission Not Found. Exception Message:")
            print(e)
        try:
            ibm_ta1_library = TA1Library("IBM")
            ibm_ta1_library.extract_contents_from_sdf(ta1_submission_dir)
            ibm_ta1_library.write_extractions_to_files(ta1_score_dir)
            ta1_collection.ta1dict['{}'.format("IBM")] = ibm_ta1_library
        except FileNotFoundError as e:
            print("Warning: TA1 IBM Submission Not Found. Exception Message:")
            print(e)
        try:
            isi_ta1_library = TA1Library("ISI")
            isi_ta1_library.extract_contents_from_sdf(ta1_submission_dir)
            isi_ta1_library.write_extractions_to_files(ta1_score_dir)
            ta1_collection.ta1dict['{}'.format("ISI")] = isi_ta1_library
        except FileNotFoundError as e:
            print("Warning: TA1 ISI Submission Not Found. Exception Message:")
            print(e)
        try:
            resin_ta1_library = TA1Library("RESIN")
            resin_ta1_library.extract_contents_from_sdf(ta1_submission_dir)
            resin_ta1_library.write_extractions_to_files(ta1_score_dir)
            ta1_collection.ta1dict['{}'.format("RESIN")] = resin_ta1_library
        except FileNotFoundError as e:
            print("Warning: TA1 RESIN Submission Not Found. Exception Message:")
            print(e)
        try:
            sbu_ta1_library = TA1Library("SBU")
            sbu_ta1_library.extract_contents_from_sdf(ta1_submission_dir)
            sbu_ta1_library.write_extractions_to_files(ta1_score_dir)
            ta1_collection.ta1dict['{}'.format("SBU")] = sbu_ta1_library
        except FileNotFoundError as e:
            print("Warning: TA1 SBU Submission Not Found. Exception Message:")
            print(e)

    # TA2 Task 1
    ta2_task1_collection = TA2Collection(is_task2=False)
    if score_tasks == "ta2task1" or score_tasks == "all":
        print("Extracting TA2 Task 1 Instantiations from SDF")
        ta2_output_directory = ta2_task1_submission_dir
        ta2_score_directory = ta2_task1_score_dir
        try:
            cmu_ta2_task1_instantiation = TA2Instantiation("CMU", is_task2=False)
            cmu_ta2_task1_instantiation.extract_contents_from_sdf(ta2_output_directory)
            cmu_ta2_task1_instantiation.write_extractions_to_files(ta2_score_directory)
            ta2_task1_collection.ta2dict['{}'.format("CMU")] = cmu_ta2_task1_instantiation
        except FileNotFoundError as e:
            print("Warning: TA2 Task1 CMU Submission Not Found. Exception Message:")
            print(e)
        try:
            ibm_ta2_task1_instantiation = TA2Instantiation("IBM", is_task2=False)
            ibm_ta2_task1_instantiation.extract_contents_from_sdf(ta2_output_directory)
            ibm_ta2_task1_instantiation.write_extractions_to_files(ta2_score_directory)
            ta2_task1_collection.ta2dict['{}'.format("IBM")] = ibm_ta2_task1_instantiation
        except FileNotFoundError as e:
            print("Warning: TA2 Task1 IBM Submission Not Found. Exception Message:")
            print(e)
        try:
            resin_ta2_task1_instantiation = TA2Instantiation("RESIN", is_task2=False)
            resin_ta2_task1_instantiation.extract_contents_from_sdf(ta2_output_directory)
            resin_ta2_task1_instantiation.write_extractions_to_files(ta2_score_directory)
            ta2_task1_collection.ta2dict['{}'.format("RESIN")] = resin_ta2_task1_instantiation
        except FileNotFoundError as e:
            print("Warning: TA2 Task1 RESIN Submission Not Found. Exception Message:")
            print(e)
    # TA2 Task 2
    ta2_task2_collection = TA2Collection(is_task2=True)

    if score_tasks == "all" or score_tasks == "ta2task2":
        print("Extracting TA2 Task 2 Instantiations from SDF")
        ta2_output_directory = ta2_task2_submission_dir
        ta2_score_directory = ta2_task2_score_dir
        try:
            cmu_ta2_task2_instantiation = TA2Instantiation("CMU", is_task2=True)
            cmu_ta2_task2_instantiation.extract_contents_from_sdf(ta2_output_directory)
            cmu_ta2_task2_instantiation.write_extractions_to_files(ta2_score_directory)
            ta2_task2_collection.ta2dict['{}'.format("CMU")] = cmu_ta2_task2_instantiation
        except FileNotFoundError as e:
            print("Warning: TA2 Task2 CMU Submission Not Found. Exception Message:")
            print(e)
        try:
            ibm_ta2_task2_instantiation = TA2Instantiation("IBM", is_task2=True)
            ibm_ta2_task2_instantiation.extract_contents_from_sdf(ta2_output_directory)
            ibm_ta2_task2_instantiation.write_extractions_to_files(ta2_score_directory)
            ta2_task2_collection.ta2dict['{}'.format("IBM")] = ibm_ta2_task2_instantiation
        except FileNotFoundError as e:
            print("Warning: TA2 Task2 IBM Submission Not Found. Exception Message:")
            print(e)
        try:
            resin_ta2_task2_instantiation = TA2Instantiation("RESIN", is_task2=True)
            resin_ta2_task2_instantiation.extract_contents_from_sdf(ta2_output_directory)
            resin_ta2_task2_instantiation.write_extractions_to_files(ta2_score_directory)
            ta2_task2_collection.ta2dict['{}'.format("RESIN")] = resin_ta2_task2_instantiation
        except FileNotFoundError as e:
            print("Warning: TA2 Task2 RESIN Submission Not Found. Exception Message:")
            print(e)

    graph_g_collection = TA2Collection(is_task2=True)
    if score_tasks == "all" or score_tasks == "graphg":
        print("Extracting Graph G from Graph G SDF")
        graph_g_instantiation = TA2Instantiation("GRAPHG", is_task2=True)
        graph_g_instantiation.extract_contents_from_sdf(graph_g_dir)
        graph_g_instantiation.write_extractions_to_files(graph_g_extraction_dir)
        graph_g_collection.ta2dict['{}'.format("GRAPHG")] = graph_g_instantiation

    print("Done")
    return ta1_collection, ta2_task1_collection, ta2_task2_collection, graph_g_collection


def define_parser():
    """
    Defines accepted CLI syntax and the actions to take for command and args.

    Returns:
        argparse parser

    """
    base_dir = os.path.dirname(os.path.realpath(__file__))
    default_config_filepath = os.path.join(base_dir, "config.ini")

    parser = argparse.ArgumentParser(
        description="Produce TA1 and TA2 Extractions from SDF Submissions"
    )
    parser.add_argument("-f", "--config_file", help='Location of Configuration file',
                        required=False, type=str, default=default_config_filepath)
    parser.add_argument("-m", "--config_mode", help='Mode of Configuration_file',
                        required=False, type=str, default="Default")
    parser.add_argument("-s", "--score_tasks",
                        help="Which tasks to score: 'all', 'ta1', 'ta2task1' or 'ta2task2'",
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
    # We can extract the objects if we wish to chain the stats with the extraction
    ta1_collection, ta2_task1_collection, \
        ta2_task2_collection, graph_g_collection = extract_sdf(config_filepath, config_mode,
                                                               score_tasks)


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
