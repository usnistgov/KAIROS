import argparse
import os
import pandas as pd

import kevs.convert_annotation_to_graph_g


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


def generate_graph_g_files(config_filepath, config_mode, score_tasks):
    if score_tasks == "all" or score_tasks == "graphg":
        kevs.convert_annotation_to_graph_g.generate_graphg(config_filepath, config_mode)


def code_main(args):
    config_filepath = args.config_file
    config_mode = args.config_mode
    score_tasks = args.score_tasks
    print("Config Filepath Used: {}".format(config_filepath))
    print("Config Mode Used: {}".format(config_mode))
    print("Tasks Scored: {}".format(score_tasks))
    generate_graph_g_files(config_filepath, config_mode, score_tasks)


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
