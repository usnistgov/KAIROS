import os
import argparse
import pandas as pd

# The first method is the method in sc
from script_02_extract_ta1_and_ta2_sdf import extract_sdf
from script_03_generate_ta1_and_ta2_stats import compute_submission_stats
from script_04_score_ke_matches import score_ke_matches
from script_05_score_task1_assessments import score_task1_assessment
from script_06_generate_ta1_coverage_stats import generate_t1_coverage_stats
from script_07_generate_graph_g_stats import generate_graph_g_stats
from script_08_analyze_ta1_and_ta2_extractions import generate_analysis


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
    print("Running Scoring and Analysis Scripts")
    print("Config Filepath Used: {}".format(config_filepath))
    print("Config Mode Used: {}".format(config_mode))
    print("Tasks Scored: {}".format(score_tasks))
    # put all of the scoring scripts here in order
    extract_sdf(config_filepath, config_mode, score_tasks)
    compute_submission_stats(config_filepath, config_mode, score_tasks)
    score_ke_matches(config_filepath, config_mode, score_tasks)
    score_task1_assessment(config_filepath, config_mode, score_tasks)
    generate_t1_coverage_stats(config_filepath, config_mode, score_tasks)
    generate_graph_g_stats(config_filepath, config_mode, score_tasks)
    generate_analysis(config_filepath, config_mode, score_tasks)


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
