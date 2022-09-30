import configparser
import os
import sys

from kevs.TA2Instantiation import TA2Collection
from kevs.Annotation import Annotation

local_path = os.path.join("..")
sys.path.append(local_path)


class TestComputeGraphG(object):
    def test_graph_g_generation(self):
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

        task2_annotation_dir = os.path.join(input_dir_prefix,
                                            config[config_mode]["task2_annotation_subdir"])

        # subfolders of root_directory/output_subdir/eval_phase_subdir
        ta2_task2_score_dir = os.path.join(output_dir_prefix,
                                           config[config_mode]["ta2_task2_score_subdir"])
        graph_g_extraction_dir = os.path.join(output_dir_prefix,
                                              config[config_mode]["graph_g_extraction_subdir"])

        ta2_task2_collection = TA2Collection(is_task2=True)
        ta2_task2_collection.import_extractions_from_file_collection(ta2_task2_score_dir)

        graph_g_collection = TA2Collection(is_task2=True)
        graph_g_collection.import_extractions_from_file_collection(graph_g_extraction_dir,
                                                                   extract_for_graph_g=True)

        task2_annotation_collection = Annotation(is_task2=True)
        task2_annotation_collection.import_all_annotation(task2_annotation_dir)

        complex_event_list = os.listdir(task2_annotation_dir)
        complex_event_list.remove('.DS_Store')
        assert len(complex_event_list) > 0

        graphg_keys = list(graph_g_collection.ta2dict['GRAPHG'].ta2dict.keys())
        graphg_ceinstance = graph_g_collection.ta2dict['GRAPHG'].ta2dict[graphg_keys[0]]
        assert graphg_ceinstance.ev_df.shape[0] > 0
        assert graphg_ceinstance.arg_df.shape[0] > 0
        assert graphg_ceinstance.ent_df.shape[0] > 0
