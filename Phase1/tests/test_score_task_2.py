from unittest import TestCase

import math
import numpy as np
import pandas as pd
import pytest
import sys
import os
import configparser


# content of conftest.py or a tests file (e.g. in your tests or root directory)

import sys
import os
import argparse


# we want to be able to import score_task_2.py file
# it is in ../ and we want to add this folder to lib_path, so that
# we can use import statement on score_task_2
lib_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../")
sys.path.append(lib_path)

local_path = os.path.join("..")
sys.path.append(local_path)

import scripts.extract_event_argument
import scripts.extract_relation
import scripts.extract_order

import scripts.score_task2_order
import scripts.score_task2_relation
import scripts.score_task2_event_argument

# testing score_task_2

# the import below works because we first added the location of score_task_2 path
# to the lib_path above
from score_task2 import str2bool


class TestTask2Scoring(object):

    def test_simple_ta2_order_scoring(self):
        config = configparser.ConfigParser()
        with open("./config.ini") as configfile:
            config.read_file(configfile)
        ta2_output_directory = config['Test']['ta2_task2_output_directory']
        ta1_output_directory = config['Test']['ta1_output_directory']
        task2_score_directory = config['Test']['task2_score_directory']
        annotation_directory = config['Test']['annotation_directory']
        graph_g_directory = config['Test']['graph_g_directory']
        test_key_directory = config['Test']['test_key_directory']
        # Extract order so that overwritten files have default version
        scripts.extract_order.extract_ta2_order_from_json("CMU_SIMPLE", ta2_output_directory,
                                                          task2_score_directory, [])
        # Check assess order
        file_order_stats_df, schema_order_stats_df = scripts.score_task2_order.initiate_file_and_schema_order_stats_df()
        order_directory = task2_score_directory + "CMU_SIMPLE" + '/Orders/'
        file_name = 'cmu-cmu_simple-ce1011-task2_order.csv'
        target_ce = file_name.split('-')[2]
        # compute file order stats
        file_order_df = pd.read_csv(order_directory + file_name)
        gorder_fn = graph_g_directory + target_ce + '/' + target_ce + '_GraphG_order.csv'
        gorder_df = pd.read_csv(gorder_fn)
        # if it is task 1 file, then skip
        task2 = True
        if 'valid' not in file_order_df.columns:
            file_order_df.insert(1, 'valid', False)
        if 'assessment' not in file_order_df.columns:
            file_order_df.insert(1, 'assessment', False)

        file_order_out_df = scripts.score_task2_order.assess_order(file_order_df, gorder_df)

        row = file_order_out_df.loc[(file_order_out_df["order_before"] == "cmu:schema_item_0/Steps/formulate-a-plot_1/Instantiated") &
                                         (file_order_out_df["order_after"] == "cmu:schema_item_0/Steps/assemble-a-group/Instantiated"), :]
        # assert row["valid"].all()

        row = file_order_out_df.loc[(file_order_out_df["order_before"] == "cmu:schema_item_0/Steps/formulate-a-plot_1/Instantiated") &
                                         (file_order_out_df["order_after"] == "cmu:schema_item_0/Steps/buy-bomb-components_1/Instantiated"), :]
        # assert row["valid"].all()

        row = file_order_out_df.loc[(file_order_out_df["order_before_provenance"] == "cmu:schema_item_0/Steps/assemble-a-group/Instantiated") &
                                         (file_order_out_df["order_after_provenance"] == "cmu:schema_item_0/Steps/formulate-a-plot_1/Instantiated"), :]
        # assert not (row["valid"].all())

    def test_simple_ibm_ta2_order_scoring(self):
        config = configparser.ConfigParser()
        with open("./config.ini") as configfile:
            config.read_file(configfile)
        ta2_output_directory = config['Test']['ta2_task2_output_directory']
        ta1_output_directory = config['Test']['ta1_output_directory']
        task2_score_directory = config['Test']['task2_score_directory']
        annotation_directory = config['Test']['annotation_directory']
        graph_g_directory = config['Test']['graph_g_directory']
        test_key_directory = config['Test']['test_key_directory']
        # Extract order so that overwritten files have default version
        scripts.extract_order.extract_ta2_order_from_json("IBM_SIMPLE", ta2_output_directory,
                                                          task2_score_directory, [])
        # Check assess order
        file_order_stats_df, schema_order_stats_df = scripts.score_task2_order.initiate_file_and_schema_order_stats_df()
        order_directory = task2_score_directory + "IBM_SIMPLE" + '/Orders/'
        file_name = 'ibm-ibm_simple-ce1011-task2_order.csv'
        target_ce = file_name.split('-')[2]
        # compute file order stats
        file_order_df = pd.read_csv(order_directory + file_name)
        gorder_fn = graph_g_directory + target_ce + '/' + target_ce + '_GraphG_order.csv'
        gorder_df = pd.read_csv(gorder_fn)
        # if it is task 1 file, then skip
        task2 = True
        if 'valid' not in file_order_df.columns:
            file_order_df.insert(1, 'valid', False)
        if 'assessment' not in file_order_df.columns:
            file_order_df.insert(1, 'assessment', False)

        file_order_out_df = scripts.score_task2_order.assess_order(file_order_df, gorder_df)

        row = file_order_out_df.loc[(file_order_out_df["order_before"] == "ibm:TA2/VP1011.1002") &
                                    (file_order_out_df["order_after"] == "ibm:TA2/VP1011.1004"), :]
        #assert row["valid"].all()

        row = file_order_out_df.loc[(file_order_out_df["order_before"] == "ibm:TA2/VP1011.1002") &
                                    (file_order_out_df["order_after"] == "ibm:TA2/VP1011.1019"), :]
        #assert row["valid"].all()

        row = file_order_out_df.loc[(file_order_out_df["order_before_provenance"] == "ibm:TA2/VP1011.1004") &
                                    (file_order_out_df["order_after_provenance"] == "ibm:TA2/VP1011.1002"), :]
        # assert not (row["valid"].all())

        # file_order_stats_df, schema_order_stats_df = \
        #     scripts.score_task2_order.get_order_stats("IBM_SIMPLE", task2_score_directory,
        #                                               graph_g_directory)
        #
        # ibm_ta2_simple_fpath = os.path.join(task2_score_directory, "IBM_SIMPLE/Orders/" \
        #                                                            "ibm-ibm_simple-ce1011-task2_order.csv")
        # ibm_df = pd.read_csv(ibm_ta2_simple_fpath)

    def test_ta2_order_scoring_buggy_case(self):
        config = configparser.ConfigParser()
        with open("./config.ini") as configfile:
            config.read_file(configfile)
        ta2_output_directory = config['Test']['ta2_task2_output_directory']
        ta1_output_directory = config['Test']['ta1_output_directory']
        task2_score_directory = config['Test']['task2_score_directory']
        annotation_directory = config['Test']['annotation_directory']
        graph_g_directory = config['Test']['graph_g_directory']
        test_key_directory = config['Test']['test_key_directory']
        # Use Pre-extracted order
        # Check assess order
        file_order_stats_df, schema_order_stats_df = scripts.score_task2_order.initiate_file_and_schema_order_stats_df()
        order_directory = task2_score_directory + "IBM_SIMPLE_BUGGY" + '/Orders/'
        file_name = 'ibm-ibm_simple_buggy-ce1011-task2_order.csv'
        target_ce = file_name.split('-')[2]
        # compute file order stats
        file_order_df = pd.read_csv(order_directory + file_name)
        gorder_fn = graph_g_directory + target_ce + '/' + target_ce + '_GraphG_order.csv'
        gorder_df = pd.read_csv(gorder_fn)
        # if it is task 1 file, then skip
        task2 = True
        if 'valid' not in file_order_df.columns:
            file_order_df.insert(1, 'valid', False)
        if 'assessment' not in file_order_df.columns:
            file_order_df.insert(1, 'assessment', False)

        file_order_out_df = scripts.score_task2_order.assess_order(file_order_df, gorder_df)

        # Check one True row and one false row
        true_row = file_order_out_df.loc[(file_order_out_df["order_before"] == "ibm:TA2/ibm_TA1/Schemas/Executed_VIED/Steps/step05AcquireVehicle.Transaction.ExchangeBuySell.Unspecified") &
                                         (file_order_out_df["order_after"] == "ibm:TA2/ibm_TA1/Schemas/Executed_VIED/Steps/step06LoadBombInVehicle.ArtifactExistence.ManufactureAssemble.Unspecified"), :]
        # assert true_row["valid"].all()

        false_row = file_order_out_df.loc[(file_order_out_df["order_before"] == "ibm:TA2/ibm_TA1/Schemas/Executed_VIED/Steps/step06LoadBombInVehicle.ArtifactExistence.ManufactureAssemble.Unspecified") &
                                          (file_order_out_df["order_after"] == "ibm:TA2/ibm_TA1/Schemas/Executed_VIED/Steps/step05AcquireVehicle.Transaction.ExchangeBuySell.Unspecified"), :]
        # assert not false_row["valid"].all()

        true_row = file_order_out_df.loc[(file_order_out_df["order_before"] == "ibm:TA2/ibm_TA1/Schemas/Executed_VIED/Steps/step06LoadBombInVehicle.ArtifactExistence.ManufactureAssemble.Unspecified") &
                                         (file_order_out_df["order_after"] == "ibm:TA2/ibm_TA1/Schemas/Executed_VIED/Steps/step07Transport.Movement.Transportation.Unspecified"), :]
        # assert true_row["valid"].all()

        false_row = file_order_out_df.loc[(file_order_out_df["order_before"] == "ibm:TA2/ibm_TA1/Schemas/Executed_VIED/Steps/step07Transport.Movement.Transportation.Unspecified") &
                                          (file_order_out_df["order_after"] == "ibm:TA2/ibm_TA1/Schemas/Executed_VIED/Steps/step06LoadBombInVehicle.ArtifactExistence.ManufactureAssemble.Unspecified"), :]
        # assert not false_row["valid"].all()

        # file_order_stats_df, schema_order_stats_df = \
        #     scripts.score_task2_order.get_order_stats("IBM_SIMPLE", task2_score_directory,
        #                                               graph_g_directory)
        #
        # ibm_ta2_simple_fpath = os.path.join(task2_score_directory, "IBM_SIMPLE/Orders/" \
        #                                                            "ibm-ibm_simple-ce1011-task2_order.csv")
        # ibm_df = pd.read_csv(ibm_ta2_simple_fpath)

    def test_simple_ta2_relation_scoring(self):
        config = configparser.ConfigParser()
        with open("./config.ini") as configfile:
            config.read_file(configfile)
        ta2_output_directory = config['Test']['ta2_task2_output_directory']
        ta1_output_directory = config['Test']['ta1_output_directory']
        task2_score_directory = config['Test']['task2_score_directory']
        annotation_directory = config['Test']['annotation_directory']
        graph_g_directory = config['Test']['graph_g_directory']
        test_key_directory = config['Test']['test_key_directory']

        scripts.extract_relation.extract_ta2_relation_from_json("IBM_SIMPLE", ta2_output_directory,
                                                                task2_score_directory, [], annotation_directory)

        file_score_er_df, schema_score_er_df = \
            scripts.score_task2_relation.compute_rel_stats("IBM_SIMPLE", task2_score_directory,
                                                       graph_g_directory)

        assert file_score_er_df["file_rel_count"][0] == 3
        assert file_score_er_df["file_grel_count"][0] == 0

        # End to end recall check
        # Extract order so that overwritten files have default version
        # scripts.score_task2_relation.score("IBM_SIMPLE", True, True, "test")
        # Read file and check that recall exceeds 0; # To Do: Replace with proper recall
        ibm_rel_fpath = os.path.join(task2_score_directory, "IBM_SIMPLE/task_2_schema_score_relation_IBM_SIMPLE.xlsx")
        ibm_rel_df = pd.read_excel(ibm_rel_fpath)

        assert ibm_rel_df["recall"][0] == 0


    def test_ibm_simple_ta2_ep_stats(self):
        config = configparser.ConfigParser()
        with open("./config.ini") as configfile:
            config.read_file(configfile)
        ta2_output_directory = config['Test']['ta2_task2_output_directory']
        ta1_output_directory = config['Test']['ta1_output_directory']
        task2_score_directory = config['Test']['task2_score_directory']
        annotation_directory = config['Test']['annotation_directory']
        graph_g_directory = config['Test']['graph_g_directory']
        # Everything has been pre-extracted

        file_stats_df, schema_stats_df = scripts.score_task2_event_argument. \
            get_ev_arg_stats("IBM_SIMPLE_SUBSET_A", task2_score_directory, annotation_directory,
                             graph_g_directory)

        # Check a few key points on the file stats based on hand-generated knowledge
        assert file_stats_df.loc[0, "CE"] == "ce1011"
        assert file_stats_df.loc[0, "file_ev_all_count"] == 3
        assert file_stats_df.loc[0, "file_ta2_gev_all_count"] == 1
        assert file_stats_df.loc[0, "file_ev_all_count"] == 3

        file_score_df, schema_score_df = scripts.score_task2_event_argument.score_ev_arg_recall(file_stats_df, schema_stats_df)
        file_diagnosis_df, schema_diagnosis_df = scripts.score_task2_event_argument.diagnose_ev_arg_recall(file_stats_df, schema_stats_df)

        # Check main recall scores

        # Graph Events Check
        assert file_stats_df.loc[0, "file_ta2_gev_all_count"] == 1
        assert file_stats_df.loc[0, "file_ta1_gev_sst_all_count"] == 2
        assert file_score_df.loc[0, "gev_all_count"] == 6
        assert file_score_df.loc[0, "recall_ta1_gev_all"] == \
               file_score_df.loc[0, "file_ta2_gev_all_count"] / file_score_df.loc[0,"gev_all_count"]

        # Graph Arg check sst level
        # Give the conservative of allowing an additional argument to be counted in Graph G.
        assert file_stats_df.loc[0, "file_ta1_gev_sst_ta1_garg_count"] == 5
        assert file_stats_df.loc[0, "file_ta2_gev_ta2_garg_count"] == 4
        assert file_score_df.loc[0, "garg_count"] == 32
        # In code, it is defined as
        # file_ta2_gev_ta2_garg_count/ garg_count
        assert file_diagnosis_df.loc[0, "recall_ta1_gev_sst_ta1_garg"] == \
               file_diagnosis_df.loc[0, "file_ta1_gev_sst_ta1_garg_count"] / file_diagnosis_df.loc[0,"garg_count"]
        assert file_score_df.loc[0, "recall_ta1_gev_ta1_garg"] == \
               file_score_df.loc[0, "file_ta2_gev_ta2_garg_count"] / file_score_df.loc[0,"garg_count"]

        # Annotation Event Check
        assert file_stats_df.loc[0, "file_ta1_aev_sst_all_count"] == 3
        assert file_score_df.loc[0, "aev_all_count"] == 70
        assert file_score_df.loc[0, "recall_ta1_aev_sst_all"] == \
               file_score_df.loc[0, "file_ta1_aev_sst_all_count"] / file_score_df.loc[0,"aev_all_count"]

        # Annotation Argument Check sst level
        assert file_stats_df.loc[0, "file_ta1_aev_sst_ta1_aarg_count"] == 7
        # Ignores empty arguments
        assert file_score_df.loc[0, "aarg_count"] == 275
        assert file_score_df.loc[0, "recall_ta1_aev_sst_ta1_aarg"] == \
               file_score_df.loc[0, "file_ta1_aev_sst_ta1_aarg_count"] / file_score_df.loc[0,"aarg_count"]

        #scripts.score_task2_event_argument.write_to_files(task2_score_directory, "IBM_SIMPLE_SUBSET_A", file_score_df, schema_score_df,
        #                file_diagnosis_df, schema_diagnosis_df)



    def test_cmu_simple_ta2_ep_stats(self):
        config = configparser.ConfigParser()
        with open("./config.ini") as configfile:
            config.read_file(configfile)
        ta2_output_directory = config['Test']['ta2_task2_output_directory']
        ta1_output_directory = config['Test']['ta1_output_directory']
        task2_score_directory = config['Test']['task2_score_directory']
        annotation_directory = config['Test']['annotation_directory']
        graph_g_directory = config['Test']['graph_g_directory']
        # Everything has been pre-extracted

        file_stats_df, schema_stats_df = scripts.score_task2_event_argument. \
            get_ev_arg_stats("CMU_SIMPLE_SUBSET_A", task2_score_directory, annotation_directory,
                             graph_g_directory)
        file_score_df, schema_score_df = scripts.score_task2_event_argument.score_ev_arg_recall(file_stats_df, schema_stats_df)
        file_diagnosis_df, schema_diagnosis_df = scripts.score_task2_event_argument.diagnose_ev_arg_recall(file_stats_df, schema_stats_df)

        # Check main recall scores
        # Graph Events Check
        assert file_stats_df.loc[0, "file_ta2_gev_all_count"] == 2
        assert file_stats_df.loc[0, "file_ta1_gev_sst_all_count"] == 2
        assert file_score_df.loc[0, "gev_all_count"] == 6
        assert file_score_df.loc[0, "recall_ta1_gev_all"] == \
               file_score_df.loc[0, "file_ta2_gev_all_count"] / file_score_df.loc[0,"gev_all_count"]

        # Graph Arg check sst level
        assert file_stats_df.loc[0, "file_ta1_gev_sst_ta1_garg_count"] == 3
        assert file_stats_df.loc[0, "file_ta2_gev_ta2_garg_count"] == 3
        assert file_score_df.loc[0, "garg_count"] == 32
        assert file_diagnosis_df.loc[0, "recall_ta1_gev_sst_ta1_garg"] == \
               file_diagnosis_df.loc[0, "file_ta1_gev_sst_ta1_garg_count"] / file_diagnosis_df.loc[0,"garg_count"]
        assert file_score_df.loc[0, "recall_ta1_gev_ta1_garg"] == \
               file_score_df.loc[0, "file_ta2_gev_ta2_garg_count"] / file_score_df.loc[0,"garg_count"]

        # Annotation Event Check
        assert file_stats_df.loc[0, "file_ta1_aev_sst_all_count"] == 2
        assert file_score_df.loc[0, "aev_all_count"] == 70
        assert file_score_df.loc[0, "recall_ta1_aev_sst_all"] == \
               file_score_df.loc[0, "file_ta1_aev_sst_all_count"] / file_score_df.loc[0,"aev_all_count"]

        # Annotation Argument Check sst level
        assert file_stats_df.loc[0, "file_ta1_aev_sst_ta1_aarg_count"] == 3
        # Ignores empty arguments
        assert file_score_df.loc[0, "aarg_count"] == 275
        assert file_score_df.loc[0, "recall_ta1_aev_sst_ta1_aarg"] == \
               file_score_df.loc[0, "file_ta1_aev_sst_ta1_aarg_count"] / file_score_df.loc[0,"aarg_count"]

        #scripts.score_task2_event_argument.write_to_files(task2_score_directory, "CMU_SIMPLE_SUBSET_A", file_score_df, schema_score_df,
        #                file_diagnosis_df, schema_diagnosis_df)


    def test_resin_simple_ta2_ep_stats(self):
        config = configparser.ConfigParser()
        with open("./config.ini") as configfile:
            config.read_file(configfile)
        ta2_output_directory = config['Test']['ta2_task2_output_directory']
        ta1_output_directory = config['Test']['ta1_output_directory']
        task2_score_directory = config['Test']['task2_score_directory']
        annotation_directory = config['Test']['annotation_directory']
        graph_g_directory = config['Test']['graph_g_directory']
        # Everything has been pre-extracted

        file_stats_df, schema_stats_df = scripts.score_task2_event_argument. \
            get_ev_arg_stats("RESIN_SIMPLE_SUBSET_A", task2_score_directory, annotation_directory,
                             graph_g_directory)
        file_score_df, schema_score_df = scripts.score_task2_event_argument.score_ev_arg_recall(file_stats_df, schema_stats_df)
        file_diagnosis_df, schema_diagnosis_df = scripts.score_task2_event_argument.diagnose_ev_arg_recall(file_stats_df, schema_stats_df)

        # Check main recall scores

        # Graph Events Check
        assert file_stats_df.loc[0, "file_ta2_gev_all_count"] == 2
        assert file_stats_df.loc[0, "file_ta1_gev_sst_all_count"] == 2
        assert file_score_df.loc[0, "gev_all_count"] == 6
        assert file_score_df.loc[0, "recall_ta1_gev_all"] == \
               file_score_df.loc[0, "file_ta2_gev_all_count"] / file_score_df.loc[0,"gev_all_count"]

        # Graph Arg check sst level
        assert file_stats_df.loc[0, "file_ta1_gev_sst_ta1_garg_count"] == 8
        assert file_stats_df.loc[0, "file_ta2_gev_ta2_garg_count"] == 8
        assert file_score_df.loc[0, "garg_count"] == 32
        assert file_diagnosis_df.loc[0, "recall_ta1_gev_sst_ta1_garg"] == \
               file_diagnosis_df.loc[0, "file_ta1_gev_sst_ta1_garg_count"] / file_diagnosis_df.loc[0,"garg_count"]
        assert file_score_df.loc[0, "recall_ta1_gev_ta1_garg"] == \
               file_score_df.loc[0, "file_ta2_gev_ta2_garg_count"] / file_score_df.loc[0,"garg_count"]

        # Annotation Event Check
        assert file_stats_df.loc[0, "file_ta1_aev_sst_all_count"] == 2
        assert file_score_df.loc[0, "aev_all_count"] == 70
        assert file_score_df.loc[0, "recall_ta1_aev_sst_all"] == \
               file_score_df.loc[0, "file_ta1_aev_sst_all_count"] / file_score_df.loc[0,"aev_all_count"]

        # Annotation Argument Check sst level
        assert file_stats_df.loc[0, "file_ta1_aev_sst_ta1_aarg_count"] == 8
        # Ignores empty arguments
        assert file_score_df.loc[0, "aarg_count"] == 275
        assert file_score_df.loc[0, "recall_ta1_aev_sst_ta1_aarg"] == \
               file_score_df.loc[0, "file_ta1_aev_sst_ta1_aarg_count"] / file_score_df.loc[0,"aarg_count"]

        #scripts.score_task2_event_argument.write_to_files(task2_score_directory, "RESIN_SIMPLE_SUBSET_A", file_score_df, schema_score_df,
        #                file_diagnosis_df, schema_diagnosis_df)


    def test_jhu_simple_ta2_ep_stats(self):
        config = configparser.ConfigParser()
        with open("./config.ini") as configfile:
            config.read_file(configfile)
        ta2_output_directory = config['Test']['ta2_task2_output_directory']
        ta1_output_directory = config['Test']['ta1_output_directory']
        task2_score_directory = config['Test']['task2_score_directory']
        annotation_directory = config['Test']['annotation_directory']
        graph_g_directory = config['Test']['graph_g_directory']
        # Everything has been pre-extracted

        file_stats_df, schema_stats_df = scripts.score_task2_event_argument. \
            get_ev_arg_stats("JHU_SIMPLE_SUBSET_A", task2_score_directory, annotation_directory,
                             graph_g_directory)
        file_score_df, schema_score_df = scripts.score_task2_event_argument.score_ev_arg_recall(file_stats_df, schema_stats_df)
        file_diagnosis_df, schema_diagnosis_df = scripts.score_task2_event_argument.diagnose_ev_arg_recall(file_stats_df, schema_stats_df)

        # Check main recall scores

        # Graph Events Check
        assert file_stats_df.loc[0, "file_ta2_gev_all_count"] == 1
        assert file_stats_df.loc[0, "file_ta1_gev_sst_all_count"] == 1
        assert file_score_df.loc[0, "gev_all_count"] == 6
        assert file_score_df.loc[0, "recall_ta1_gev_all"] == \
               file_score_df.loc[0, "file_ta2_gev_all_count"] / file_score_df.loc[0,"gev_all_count"]

        # Graph Arg check sst level
        assert file_stats_df.loc[0, "file_ta1_gev_sst_ta1_garg_count"] == 4
        assert file_stats_df.loc[0, "file_ta2_gev_ta2_garg_count"] == 3
        assert file_score_df.loc[0, "garg_count"] == 32
        assert file_diagnosis_df.loc[0, "recall_ta1_gev_sst_ta1_garg"] == \
               file_diagnosis_df.loc[0, "file_ta1_gev_sst_ta1_garg_count"] / file_diagnosis_df.loc[0,"garg_count"]
        assert file_score_df.loc[0, "recall_ta1_gev_ta1_garg"] == \
               file_score_df.loc[0, "file_ta2_gev_ta2_garg_count"] / file_score_df.loc[0,"garg_count"]

        # Annotation Event Check
        assert file_stats_df.loc[0, "file_ta1_aev_sst_all_count"] == 2
        assert file_score_df.loc[0, "aev_all_count"] == 70
        assert file_score_df.loc[0, "recall_ta1_aev_sst_all"] == \
               file_score_df.loc[0, "file_ta1_aev_sst_all_count"] / file_score_df.loc[0,"aev_all_count"]

        # Annotation Argument Check sst level
        assert file_stats_df.loc[0, "file_ta1_aev_sst_ta1_aarg_count"] == 5
        # Ignores empty arguments
        assert file_score_df.loc[0, "aarg_count"] == 275
        assert file_score_df.loc[0, "recall_ta1_aev_sst_ta1_aarg"] == \
               file_score_df.loc[0, "file_ta1_aev_sst_ta1_aarg_count"] / file_score_df.loc[0,"aarg_count"]

        #scripts.score_task2_event_argument.write_to_files(task2_score_directory, "JHU_SIMPLE_SUBSET_A", file_score_df, schema_score_df,
        #                file_diagnosis_df, schema_diagnosis_df)


class TestScoreTask2_str2bool(TestCase):

    def test_str2bool_yes(self):
        self.assertTrue(str2bool('yes'))

    def test_str2bool_no(self):
        self.assertFalse(str2bool('no'))

 # you could check if values are equal like this:
    def test_str2bool_no_testequal(self):
        self.assertEqual(str2bool('no'), False)

# This exception test passes
# note how the 'Yeah' is listed as a third argument to assertRaises,
# rather then an argument to str2bool('Yeah')
    def test_str2bool_exception(self):
        self.assertRaises(argparse.ArgumentTypeError, str2bool, 'Yeah')


