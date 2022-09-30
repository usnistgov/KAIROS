import configparser
import os
import sys
import pandas as pd
import argparse

from kevs.TA1Library import TA1Collection
from kevs.compute_ta1_coverage import compute_ta1_coverage, compute_task1_ins_schema


def generate_t1_coverage_stats(config_filepath: str, config_mode: str, score_tasks: str) -> None:
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
    input_dir_prefix = os.path.join(root_dir, input_subdir, eval_phase_subdir)
    output_dir_prefix = os.path.join(root_dir, output_subdir, eval_phase_subdir)

    # subfolders of root_directory/output_subdir/eval_phase_subdir
    ta1_score_dir = os.path.join(output_dir_prefix,
                                 config[config_mode]["ta1_score_subdir"])
    ta1_analysis_dir = os.path.join(output_dir_prefix, config[config_mode]["ta1_analysis_subdir"])
    ta2_task1_score_dir = os.path.join(output_dir_prefix,
                                       config[config_mode]["ta2_task1_score_subdir"])
    task1_annotation_dir = os.path.join(input_dir_prefix,
                                        config[config_mode]["task1_annotation_subdir"])

    print("Importing TA1 Collection")
    ta1_collection = TA1Collection()
    ta1_collection.import_extractions_from_file_collection(ta1_score_dir)

    if score_tasks == "all" or score_tasks == "ta1":
        compute_ta1_coverage(ta1_score_dir, ta1_collection, ta1_analysis_dir)
        compute_task1_ins_schema(ta2_task1_score_dir, task1_annotation_dir,
                                 ta1_collection, ta1_analysis_dir)
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
        description="Generate TA1 Coverage Stats"
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
    generate_t1_coverage_stats(config_filepath, config_mode, score_tasks)


def main():
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)
    parser = define_parser()
    args = parser.parse_args()
    if hasattr(args, "func") and args.func is not None:
        args.func(args)
    else:
        parser.print_help()

    # function code_main(args) is automatically called here


if __name__ == "__main__":
    main()
