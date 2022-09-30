import sys
import os
import configparser

from kevs.TA1Library import TA1Collection
from kevs.TA2Instantiation import TA2Collection

local_path = os.path.join("..")
sys.path.append(local_path)


class TestTrees(object):
    """
    Testing Extraction Methods
    """

    def test_ta1_trees(self):
        config_filepath = "./execution_scripts/config.ini"
        config_mode = "Test"
        try:
            config = configparser.ConfigParser()
            with open(config_filepath) as configfile:
                config.read_file(configfile)
        except ImportError:
            sys.exit('Cannot open config file: ' + config_filepath)

        root_dir = config[config_mode]["root_dir"]
        output_subdir = config[config_mode]["output_subdir"]
        eval_phase_subdir = config[config_mode]["eval_phase_subdir"]
        output_dir_prefix = os.path.join(root_dir, output_subdir, eval_phase_subdir)

        # Subfolders of root_directory/input_subdir/eval_phase_subdir

        # subfolders of root_directory/output_subdir/eval_phase_subdir
        ta1_score_dir = os.path.join(output_dir_prefix,
                                     config[config_mode]["ta1_score_subdir"])

        ta1_collection = TA1Collection()
        ta1_collection.import_extractions_from_file_collection(ta1_score_dir)
        ta1_collection.produce_event_trees(ta1_score_dir)
        assert True

    def test_ta2_task1_trees(self):
        config_filepath = "./execution_scripts/config.ini"
        config_mode = "Test"
        try:
            config = configparser.ConfigParser()
            with open(config_filepath) as configfile:
                config.read_file(configfile)
        except ImportError:
            sys.exit('Cannot open config file: ' + config_filepath)

        root_dir = config[config_mode]["root_dir"]
        output_subdir = config[config_mode]["output_subdir"]
        eval_phase_subdir = config[config_mode]["eval_phase_subdir"]
        include_all_events = (config[config_mode]['include_all_events'].lower() == "true")
        output_dir_prefix = os.path.join(root_dir, output_subdir, eval_phase_subdir)

        # Subfolders of root_directory/input_subdir/eval_phase_subdir

        # subfolders of root_directory/output_subdir/eval_phase_subdir
        ta2_task1_score_dir = os.path.join(output_dir_prefix,
                                           config[config_mode]["ta2_task1_score_subdir"])

        # TA2 Task 1
        ta2_task1_collection = TA2Collection(is_task2=False)
        ta2_task1_collection.import_extractions_from_file_collection(ta2_task1_score_dir)
        print("Producing TA2 Task 1 Event Trees")
        ta2_task1_collection.produce_event_trees(ta2_task1_score_dir, include_all_events)
        assert True

    def test_ta2_task2_stats(self):
        config_filepath = "./execution_scripts/config.ini"
        config_mode = "Test"
        try:
            config = configparser.ConfigParser()
            with open(config_filepath) as configfile:
                config.read_file(configfile)
        except ImportError:
            sys.exit('Cannot open config file: ' + config_filepath)

        root_dir = config[config_mode]["root_dir"]
        output_subdir = config[config_mode]["output_subdir"]
        eval_phase_subdir = config[config_mode]["eval_phase_subdir"]
        include_all_events = (config[config_mode]['include_all_events'].lower() == "true")
        output_dir_prefix = os.path.join(root_dir, output_subdir, eval_phase_subdir)

        # Subfolders of root_directory/input_subdir/eval_phase_subdir

        # subfolders of root_directory/output_subdir/eval_phase_subdir
        ta2_task2_score_dir = os.path.join(output_dir_prefix,
                                           config[config_mode]["ta2_task2_score_subdir"])

        # TA2 Task 2
        ta2_task2_collection = TA2Collection(is_task2=True)
        ta2_task2_collection.import_extractions_from_file_collection(ta2_task2_score_dir)
        print("Producing TA2 Task 2 Event Trees")
        ta2_task2_collection.produce_event_trees(ta2_task2_score_dir, include_all_events)
        assert True

    def test_ta2_graphg_trees(self):
        config_filepath = "./execution_scripts/config.ini"
        config_mode = "Test"
        try:
            config = configparser.ConfigParser()
            with open(config_filepath) as configfile:
                config.read_file(configfile)
        except ImportError:
            sys.exit('Cannot open config file: ' + config_filepath)

        root_dir = config[config_mode]["root_dir"]
        output_subdir = config[config_mode]["output_subdir"]
        eval_phase_subdir = config[config_mode]["eval_phase_subdir"]
        output_dir_prefix = os.path.join(root_dir, output_subdir, eval_phase_subdir)

        # Subfolders of root_directory/input_subdir/eval_phase_subdir

        # subfolders of root_directory/output_subdir/eval_phase_subdir
        graph_g_extraction_dir = os.path.join(output_dir_prefix,
                                              config[config_mode]["graph_g_extraction_subdir"])

        # Graph G
        graph_g_collection = TA2Collection(is_task2=True)
        graph_g_collection.import_extractions_from_file_collection(graph_g_extraction_dir,
                                                                   extract_for_graph_g=True)
        # For Graph G, ignore the config and produce all events
        print("Producing Graph G Event Trees")
        graph_g_collection.produce_event_trees(graph_g_extraction_dir,
                                               include_all_events=True)
        assert True
