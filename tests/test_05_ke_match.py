import sys
import os
import configparser

from kevs.Annotation import Annotation
from kevs.TA2Instantiation import TA2Collection
from kevs.match_ke import match_ke_elements

local_path = os.path.join("..")
sys.path.append(local_path)


class TestKEMatch(object):
    def __init__(self, config_filepath: str, config_mode: str) -> None:
        try:
            config = configparser.ConfigParser()
            with open(config_filepath) as configfile:
                config.read_file(configfile)
        except ImportError:
            sys.exit('Cannot open config file: ' + config_filepath)

        self.root_dir = config[config_mode]["root_dir"]
        self.input_subdir = config[config_mode]["input_subdir"]
        self.output_subdir = config[config_mode]["output_subdir"]
        self.eval_phase_subdir = config[config_mode]["eval_phase_subdir"]
        self.input_dir_prefix = os.path.join(
            self.root_dir, self.input_subdir, self.eval_phase_subdir)
        self.output_dir_prefix = os.path.join(
            self.root_dir, self.output_subdir, self.eval_phase_subdir)

        # Subfolders of root_directory/input_subdir/eval_phase_subdir
        self.task1_annotation_dir = os.path.join(self.input_dir_prefix,
                                                 config[config_mode]["task1_annotation_subdir"])
        self.task2_annotation_dir = os.path.join(self.input_dir_prefix,
                                                 config[config_mode]["task2_annotation_subdir"])

        # subfolders of root_directory/output_subdir/eval_phase_subdir
        # use ta1 for later: ta1_score_dir = os.path.join(output_dir_prefix,
        #                             config[config_mode]["ta1_score_subdir"])
        self.ta2_task1_score_dir = os.path.join(self.output_dir_prefix,
                                                config[config_mode]["ta2_task1_score_subdir"])
        self.ta2_task2_score_dir = os.path.join(self.output_dir_prefix,
                                                config[config_mode]["ta2_task2_score_subdir"])
        self.ta2_task1_analysis_dir = os.path.join(self.output_dir_prefix,
                                                   config[config_mode]["ta2_task1_analysis_subdir"])
        self.ta2_task2_analysis_dir = os.path.join(self.output_dir_prefix,
                                                   config[config_mode]["ta2_task2_analysis_subdir"])
        self.graph_g_extraction_dir = os.path.join(self.output_dir_prefix,
                                                   config[config_mode]["graph_g_extraction_subdir"])
        self.graph_g_analysis_dir = os.path.join(self.output_dir_prefix,
                                                 config[config_mode]["graph_g_analysis_subdir"])
        self.qnode_dir = os.path.join(self.output_dir_prefix,
                                      config[config_mode]["qnode_subdir"])

        self.task1_complex_event_list = os.listdir(self.task1_annotation_dir)
        self.task1_complex_event_list.remove('.DS_Store')

        self.task2_complex_event_list = os.listdir(self.task2_annotation_dir)
        self.task2_complex_event_list.remove('.DS_Store')

        # the cache file for qnode similarity scores
        self.qnode_sim_fp = os.path.join(self.qnode_dir, 'nist_isi_qnode_sim_cache.tsv')

        # if TA1 or TA2 extraction directories do not exist, throw error message
        if not os.path.isdir(self.ta2_task1_score_dir) \
                or not os.path.isdir(self.ta2_task2_score_dir):
            sys.exit('Directory not found: ' + self.ta2_task1_score_dir +
                     ' or ' + self.ta2_task2_score_dir)
        # if TA2 Task1 analysis directories do not exist, create them
        self.ta2_t1_as_path = os.path.join(self.ta2_task1_analysis_dir, "Automated_Scoring")
        self.ta2_t2_as_path = os.path.join(self.ta2_task2_analysis_dir, "Automated_Scoring")
        self.ta2_g_as_path = os.path.join(self.graph_g_analysis_dir, "Automated_Scoring")

        # if TA2 Task2 analysis directories do not exist, create them
        # if not os.path.isdir(ta2_t2_as_path):
        #    os.makedirs(ta2_t2_as_path)

        print("Importing Annotations")
        self.task1_annotation_collection = Annotation(is_task2=False)
        self.task1_annotation_collection.import_all_annotation(self.task1_annotation_dir)
        self.task2_annotation_collection = Annotation(is_task2=True)
        self.task2_annotation_collection.import_all_annotation(self.task2_annotation_dir)

        print("Importing TA2 Task 1 Submissions")
        self.ta2_task1_collection = TA2Collection(is_task2=False)
        self.ta2_task1_collection.import_extractions_from_file_collection(self.ta2_task1_score_dir)

        print("Importing TA2 Task 2 Submissions")
        self.ta2_task2_collection = TA2Collection(is_task2=True)
        self.ta2_task2_collection.import_extractions_from_file_collection(self.ta2_task2_score_dir)

        print("Importing Graph G")
        self.graph_g_collection = TA2Collection(is_task2=True)
        self.graph_g_collection.import_extractions_from_file_collection(self.graph_g_extraction_dir,
                                                                        extract_for_graph_g=True)
        pass

    def test_task2_ke_matching(self):
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
        task1_annotation_dir = os.path.join(input_dir_prefix,
                                            config[config_mode]["task1_annotation_subdir"])
        task2_annotation_dir = os.path.join(input_dir_prefix,
                                            config[config_mode]["task2_annotation_subdir"])

        # subfolders of root_directory/output_subdir/eval_phase_subdir
        ta2_task1_score_dir = os.path.join(output_dir_prefix,
                                           config[config_mode]["ta2_task1_score_subdir"])
        ta2_task2_score_dir = os.path.join(output_dir_prefix,
                                           config[config_mode]["ta2_task2_score_subdir"])
        ta2_task2_analysis_dir = os.path.join(output_dir_prefix,
                                              config[config_mode]["ta2_task2_analysis_subdir"])
        graph_g_extraction_dir = os.path.join(output_dir_prefix,
                                              config[config_mode]["graph_g_extraction_subdir"])
        qnode_dir = os.path.join(output_dir_prefix,
                                 config[config_mode]["qnode_subdir"])

        task1_complex_event_list = os.listdir(task1_annotation_dir)
        task1_complex_event_list.remove('.DS_Store')

        task2_complex_event_list = os.listdir(task2_annotation_dir)
        task2_complex_event_list.remove('.DS_Store')

        # the cache file for qnode similarity scores
        qnode_sim_fp = os.path.join(qnode_dir, 'nist_isi_qnode_sim_cache.tsv')

        # if TA1 or TA2 extraction directories do not exist, throw error message
        if not os.path.isdir(ta2_task1_score_dir) \
                or not os.path.isdir(ta2_task2_score_dir):
            sys.exit('Directory not found: ' + ta2_task1_score_dir +
                     ' or ' + ta2_task2_score_dir)
        # For later: ta1_as_path = os.path.join(ta1_analysis_dir, "Automated_Scoring")
        ta2_t2_as_path = os.path.join(ta2_task2_analysis_dir, "Automated_Scoring")

        # Annotation must be importated for all tasks
        print("Importing Annotations")
        task1_annotation_collection = Annotation(is_task2=False)
        task1_annotation_collection.import_all_annotation(task1_annotation_dir)
        task2_annotation_collection = Annotation(is_task2=True)
        task2_annotation_collection.import_all_annotation(task2_annotation_dir)

        print("Importing TA2 Task 2 Submissions")
        ta2_task2_collection = TA2Collection(is_task2=True)
        ta2_task2_collection.import_extractions_from_file_collection(ta2_task2_score_dir)

        # Graph G must be imported for multiple tasks
        print("Importing Graph G")
        graph_g_collection = TA2Collection(is_task2=True)
        graph_g_collection.import_extractions_from_file_collection(graph_g_extraction_dir,
                                                                   extract_for_graph_g=True)

        print("Automatically Scoring TA2 Task 2 Output")
        match_ke_elements(ta2_t2_as_path, task1_annotation_collection, ta2_task2_collection,
                          graph_g_collection, qnode_dir,
                          qnode_sim_fp, is_task2=True, use_graph_g=False,
                          min_confidence_threshold=0)

        assert True
