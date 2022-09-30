import sys
import os
import configparser
import pandas as pd

from kevs.TA1Library import TA1Library, TA1Collection
from kevs.TA2Instantiation import TA2Instantiation, TA2Collection

local_path = os.path.join("..")
sys.path.append(local_path)


class TestExtractions(object):
    """
    Testing Extraction Methods
    """

    def test_ta1_event_extractions(self):
        config_filepath = "./execution_scripts/config.ini"
        config_mode = "Test"
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

        # subfolders of root_directory/output_subdir/eval_phase_subdir
        ta1_score_dir = os.path.join(output_dir_prefix,
                                     config[config_mode]["ta1_score_subdir"])

        # Extract the files
        ta1_collection = TA1Collection()
        test_ta1_library = TA1Library("NISTTESTA")
        test_ta1_library.extract_contents_from_sdf(ta1_submission_dir)
        test_ta1_library.write_extractions_to_files(ta1_score_dir)
        ta1_collection.ta1dict['{}'.format("NISTTESTA")] = test_ta1_library

        # Read everything just extracted
        ta1_import_collection = TA1Collection()
        ta1_import_collection.import_extractions_from_file_collection(ta1_score_dir)

        # Assert import check by doing a dataframe diff

        # Do some basic event tests
        ev_df = ta1_import_collection.ta1dict["NISTTESTA"].ev_df
        assert ev_df.shape[0] == 9
        assert ev_df.loc[ev_df["ev_id"] == "nist:Events/10002/Step_Infection",
                         "ev_name"].size == 1
        assert ev_df.loc[ev_df["ev_id"] == "nist:Events/10002/Step_Infection",
                         "ev_name"].iloc[0] == "Sequence of Infection"
        assert ev_df.loc[ev_df["ev_id"] == "nist:Events/10002/Step_Infection",
                         "ev_children_gate"].size == 1
        assert ev_df.loc[ev_df["ev_id"] == "nist:Events/10002/Step_Infection",
                         "ev_children_gate"].iloc[0] == "and"
        assert ev_df.loc[ev_df["ev_id"] == "nist:Events/10003/Step_Medical_Affect",
                         "ev_children_gate"].size == 1
        assert ev_df.loc[ev_df["ev_id"] == "nist:Events/10003/Step_Medical_Affect",
                         "ev_children_gate"].iloc[0] == "or"

    def test_ta2_task1_event_extractions(self):
        config_filepath = "./execution_scripts/config.ini"
        config_mode = "Test"
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
        ta2_task1_submission_dir = os.path.join(input_dir_prefix,
                                                config[config_mode]["ta2_task1_submission_subdir"])

        # subfolders of root_directory/output_subdir/eval_phase_subdir
        ta2_task1_score_dir = os.path.join(output_dir_prefix,
                                           config[config_mode]["ta2_task1_score_subdir"])

        # TA2 Task 1
        ta2_task1_collection = TA2Collection(is_task2=False)
        ta2_output_directory = ta2_task1_submission_dir
        ta2_score_directory = ta2_task1_score_dir
        test_ta2_task1_instantiation = TA2Instantiation("NISTTESTA", is_task2=False)
        test_ta2_task1_instantiation.extract_contents_from_sdf(ta2_output_directory)
        test_ta2_task1_instantiation.write_extractions_to_files(ta2_score_directory)
        ta2_task1_collection.ta2dict['{}'.format("NISTTESTA")] = test_ta2_task1_instantiation

        # Read everything just extracted
        ta2_import_collection = TA2Collection()
        ta2_import_collection.import_extractions_from_file_collection(ta2_task1_score_dir)

        # Assert import check by doing a dataframe diff

        # Check the schema_instance_id to make sure it matches
        schema_fpath = os.path.join(ta2_task1_score_dir, "NISTTESTA", "Extracted_Schemas",
                                    "nisttesta-nisttesta-task1-ce2002-00001_schema.csv")
        schema_df = pd.read_csv(schema_fpath)
        assert schema_df.shape == (1, 23)
        schema_row = schema_df.iloc[0, ]
        # assert str(schema_row['instance_id_short']) == "00001"
        assert schema_row['instance_id'] == "nist:Instances/00001/nist-task1-ce2002"
        assert schema_row['instance_name'] == "nist TA2 Task1 ce2002"
        assert schema_row['schema_instance_id'] == \
            "nisttesta-nisttesta-task1-ce2002.json_00001"
        assert True

    def test_ta2_task2_event_extractions(self):
        config_filepath = "./execution_scripts/config.ini"
        config_mode = "Test"
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
        ta2_task2_submission_dir = os.path.join(input_dir_prefix,
                                                config[config_mode]["ta2_task2_submission_subdir"])

        # subfolders of root_directory/output_subdir/eval_phase_subdir
        ta2_task2_score_dir = os.path.join(output_dir_prefix,
                                           config[config_mode]["ta2_task2_score_subdir"])

        ta2_task2_collection = TA2Collection(is_task2=True)
        ta2_output_directory = ta2_task2_submission_dir
        ta2_score_directory = ta2_task2_score_dir

        test_ta2_task2_instantiation = TA2Instantiation("NISTTESTA", is_task2=True)
        test_ta2_task2_instantiation.extract_contents_from_sdf(ta2_output_directory)
        test_ta2_task2_instantiation.write_extractions_to_files(ta2_score_directory)
        ta2_task2_collection.ta2dict['{}'.format("NISTTESTA")] = test_ta2_task2_instantiation

        # Read everything just extracted
        ta2_import_collection = TA2Collection()
        ta2_import_collection.import_extractions_from_file_collection(ta2_task2_score_dir)
        assert True

    def test_ta2_graphg_event_extractions(self):
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
        graph_g_dir = os.path.join(output_dir_prefix,
                                   config[config_mode]["graph_g_subdir"])
        graph_g_extraction_dir = os.path.join(output_dir_prefix,
                                              config[config_mode]["graph_g_extraction_subdir"])

        graph_g_collection = TA2Collection(is_task2=True)
        graph_g_instantiation = TA2Instantiation("GRAPHG", is_task2=True)
        graph_g_instantiation.extract_contents_from_sdf(graph_g_dir)
        graph_g_instantiation.write_extractions_to_files(graph_g_extraction_dir)
        graph_g_collection.ta2dict['{}'.format("GRAPHG")] = graph_g_instantiation

        # Read everything just extracted
        ta2_import_collection = TA2Collection()
        ta2_import_collection.import_extractions_from_file_collection(graph_g_extraction_dir)
        assert True
