import sys
import os
import pandas as pd
import configparser

local_path = os.path.join("..")
sys.path.append(local_path)


class TestGenerateG(object):

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
        eval_phase_subdir = config[config_mode]["eval_phase_subdir"]
        input_dir_prefix = os.path.join(root_dir, input_subdir, eval_phase_subdir)

        # Subfolders of root_directory/input_subdir/eval_phase_subdir
        task2_annotation_dir = os.path.join(input_dir_prefix,
                                            config[config_mode]["task2_annotation_subdir"])

        complex_event_list = os.listdir(task2_annotation_dir)
        complex_event_list.remove('.DS_Store')
        complex_event = complex_event_list[0]

        ep_sel_df = pd.read_excel(os.path.join(task2_annotation_dir, complex_event,
                                               complex_event + '_events_selected.xlsx'))
        ep_df = pd.read_excel(os.path.join(task2_annotation_dir, complex_event,
                                           complex_event + '_events.xlsx'))
        arg_df = pd.read_excel(os.path.join(task2_annotation_dir, complex_event,
                                            complex_event + '_arguments.xlsx'))
        temporal_df = pd.read_excel(os.path.join(task2_annotation_dir, complex_event,
                                                 complex_event + '_temporal.xlsx'))
        entity_qnode_df = pd.read_excel(os.path.join(task2_annotation_dir, complex_event,
                                                     complex_event + '_kb_linking.xlsx'))

        assert ep_sel_df.shape[0] >= 0
        assert ep_df.shape[0] >= 0
        assert arg_df.shape[0] >= 0
        assert temporal_df.shape[0] >= 0
        assert entity_qnode_df.shape[0] >= 0
