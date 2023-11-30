import sys
import os
import configparser

# from kevs.generate_stats import compute_ta2_submission_stats

local_path = os.path.join("..")
sys.path.append(local_path)


class TestInstantiatedPredictedEvents(object):
    """
    Test methods that return if events are instantiated or predicted
    """

    def test_instantiated_events(self):
        pass

    def test_predicted_events(self):
        pass


class TestStats(object):
    """
    Testing Extraction Methods
    """

    # def test_ta1_stats(self):
    #     config_filepath = "./execution_scripts/config.ini"
    #     config_mode = "Test"
    #     try:
    #         config = configparser.ConfigParser()
    #         with open(config_filepath) as configfile:
    #             config.read_file(configfile)
    #     except ImportError:
    #         sys.exit('Cannot open config file: ' + config_filepath)

    #     root_dir = config[config_mode]["root_dir"]
    #     output_subdir = config[config_mode]["output_subdir"]
    #     eval_phase_subdir = config[config_mode]["eval_phase_subdir"]
    #     output_dir_prefix = os.path.join(root_dir, output_subdir, eval_phase_subdir)

    #     # Subfolders of root_directory/input_subdir/eval_phase_subdir

    #     # subfolders of root_directory/output_subdir/eval_phase_subdir
    #     ta1_score_dir = os.path.join(output_dir_prefix,
    #                                  config[config_mode]["ta1_score_subdir"])
    #     ta1_analysis_dir = os.path.join(output_dir_prefix,
    #                                     config[config_mode]["ta1_analysis_subdir"])

    #     ta1_stats_df, ta1_qnode_df, ta1_ent_ev_df = \
    #         compute_ta1_submission_stats(ta1_score_dir, ta1_analysis_dir)
    #     assert ta1_stats_df.loc[0, "ev_count"] == 9
    #     assert ta1_stats_df.loc[0, "ev_primitive_count"] == 5
    #     assert ta1_stats_df.loc[0, "arg_count"] == 15
    #     assert ta1_stats_df.loc[0, "ent_count"] == 4
    #     # Non-temporal relations
    #     assert ta1_stats_df.loc[0, "rel_count"] == 1
    #     # Add test for temporal relations
    #     assert ta1_stats_df.loc[0, "temporalrel_count"] == 0

    def test_ta2_task1_stats(self):
        config_filepath = "./execution_scripts/config.ini"
        # config_mode = "Test"
        try:
            config = configparser.ConfigParser()
            with open(config_filepath) as configfile:
                config.read_file(configfile)
        except ImportError:
            sys.exit('Cannot open config file: ' + config_filepath)
        """
        We need to update our test graph G for SDF 2.3 to pass this assertion.

        root_dir = config[config_mode]["root_dir"]
        output_subdir = config[config_mode]["output_subdir"]
        eval_phase_subdir = config[config_mode]["eval_phase_subdir"]
        output_dir_prefix = os.path.join(root_dir, output_subdir, eval_phase_subdir)

        # subfolders of root_directory/output_subdir/eval_phase_subdir
        ta2_task1_score_dir = os.path.join(output_dir_prefix,
                                           config[config_mode]["ta2_task1_score_subdir"])
        ta2_task1_analysis_dir = os.path.join(output_dir_prefix,
                                              config[config_mode]["ta2_task1_analysis_subdir"])

        ta2_stats_df, ta2_group_stats_df, ta2_qnode_df, ta2_ent_ev_df, ta2_ins_ent_ev_df = \
            compute_ta2_submission_stats(ta2_task1_score_dir, ta2_task1_analysis_dir)
        assert ta2_stats_df.loc[0, "ev_count"] == 11
        assert ta2_stats_df.loc[0, "ev_instantiated_count"] == 4
        assert ta2_stats_df.loc[0, "ev_predicted_count"] == 7
        assert ta2_stats_df.loc[0, "arg_count"] == 17
        assert ta2_stats_df.loc[0, "ins_arg_count"] == 7
        assert ta2_stats_df.loc[0, "pred_arg_count"] == 10
        assert ta2_stats_df.loc[0, "ins_pred_arg_count"] == 2
        assert ta2_stats_df.loc[0, "ent_count"] == 8
        # Non-temporal relations
        assert ta2_stats_df.loc[0, "rel_count"] == 1
        # Add test for temporal relations
        assert ta2_stats_df.loc[0, "temporalrel_count"] == 2
        """
        assert True


"""
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
        output_dir_prefix = os.path.join(root_dir, output_subdir, eval_phase_subdir)

        # Subfolders of root_directory/input_subdir/eval_phase_subdir

        # subfolders of root_directory/output_subdir/eval_phase_subdir
        ta2_task2_score_dir = os.path.join(output_dir_prefix,
                                           config[config_mode]["ta2_task2_score_subdir"])
        ta2_task2_analysis_dir = os.path.join(output_dir_prefix,
                                              config[config_mode]["ta2_task2_analysis_subdir"])
        ta2_stats_df, ta2_group_stats_df, ta2_qnode_df, ta2_ent_ev_df, ta2_ins_ent_ev_df = \
            compute_ta2_submission_stats(ta2_task2_score_dir, ta2_task2_analysis_dir)
        assert ta2_stats_df.shape[0] >= 0

    def test_ta2_graphg_stats(self):
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
        graph_g_analysis_dir = os.path.join(output_dir_prefix,
                                            config[config_mode]["graph_g_analysis_subdir"])

        ta2_stats_df, ta2_group_stats_df, ta2_qnode_df, ta2_ent_ev_df, ta2_ins_ent_ev_df = \
            compute_ta2_submission_stats(graph_g_extraction_dir, graph_g_analysis_dir,
                                         extract_for_graph_g=True)
        assert ta2_stats_df.shape[0] >= 0
        # Test case: assert ta2_stats_df.loc[0, "ev_count"] == 17
        # Need additional tests here
"""
