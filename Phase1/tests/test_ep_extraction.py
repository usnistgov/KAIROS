import math
import numpy as np
import pandas as pd
import pytest
import sys
import os
import configparser

local_path = os.path.join("..")
sys.path.append(local_path)

import scripts.extract_event_argument
import scripts.extract_relation
import scripts.extract_order


# content of conftest.py or a tests file (e.g. in your tests or root directory)


class TestEPExtraction(object):
    """
    Class of tests to test EP1 Extraction
    """

    def test_simple_ta1_ep_extraction(self):
        config = configparser.ConfigParser()
        with open("./config.ini") as configfile:
            config.read_file(configfile)
        # ta2_output_directory = config['Test']['ta2_task2_output_directory']
        ta1_output_directory = config['Test']['ta1_output_directory']
        task2_score_directory = config['Test']['task2_score_directory']
        # annotation_directory = config['Test']['annotation_directory']
        # graph_g_directory = config['Test']['graph_g_directory']
        test_key_directory = config['Test']['test_key_directory']
        scripts.extract_event_argument.extract_ta1_ev_arg_from_json('CMU_SIMPLE_FIRST', ta1_output_directory,
                                                                    task2_score_directory)
        cmu_simple_ta1_fpath = \
            os.path.join(task2_score_directory,
                         "TA1_library/CMU_SIMPLE_FIRST/Event_arguments/cmu_simple_first-schemalib_ev_arg.csv")
        cmu_simple_ta1_df = pd.read_csv(cmu_simple_ta1_fpath)

        cmu_simple_ta1_key_fpath = os.path.join(test_key_directory,
                                                "Task2/TA1_library/CMU_SIMPLE_FIRST/Event_arguments/"
                                                "cmu_simple_first-schemalib_ev_arg.csv")
        cmu_simple_ta1_key_df = pd.read_csv(cmu_simple_ta1_key_fpath)
        assert len(cmu_simple_ta1_key_df["ev_id"].value_counts()) == 22
        assert len(cmu_simple_ta1_key_df["arg_id"].value_counts()) == 64
        # Partially checked by hand: use as regression test
        assert cmu_simple_ta1_df.equals(cmu_simple_ta1_key_df)

    def test_simple_graph_g_ep_extraction(self):
        config = configparser.ConfigParser()
        with open("./config.ini") as configfile:
            config.read_file(configfile)
        graph_g_directory = config['Test']['graph_g_directory']
        test_key_directory = config['Test']['test_key_directory']
        scripts.extract_event_argument.extract_graph_g_ev_arg_from_json(graph_g_directory)
        graph_g_ce1005_events_fpath = os.path.join(graph_g_directory, "ce1005/ce1005_GraphG_ev.csv")
        ce1005_ev_df = pd.read_csv(graph_g_ce1005_events_fpath)
        # We have 12 steps, so should have 12 rows
        assert ce1005_ev_df.shape[0] == 12

        ce1005_ev_key_fpath = os.path.join(test_key_directory, "Graph_G/ce1005_GraphG_ev.csv")
        ce1005_ev_key_df = pd.read_csv(ce1005_ev_key_fpath)
        # Key file has been hand-checked for correctness
        assert ce1005_ev_df.equals(ce1005_ev_key_df)

    def test_simple_ta2_ep_extraction(self):
        config = configparser.ConfigParser()
        with open("./config.ini") as configfile:
            config.read_file(configfile)
        ta2_output_directory = config['Test']['ta2_task2_output_directory']
        task2_score_directory = config['Test']['task2_score_directory']
        test_key_directory = config['Test']['test_key_directory']
        annotation_directory = config['Test']['annotation_directory']
        scripts.extract_event_argument.extract_ta2_ev_arg_from_json("IBM_SIMPLE", ta2_output_directory,
                                                                    task2_score_directory, [], annotation_directory)
        ibm_ta2_simple_fpath = os.path.join(task2_score_directory, "IBM_SIMPLE/Event_arguments/" \
                                                                   "ibm-ibm_simple-ce1011-task2_ev_arg.csv")
        ibm_df = pd.read_csv(ibm_ta2_simple_fpath)
        assert ibm_df.loc[
               ibm_df["ev_type"] == "kairos:Primitives/Events/ArtifactExistence.DamageDestroyDisableDismantle.Damage",
               :].shape[0] > 0
        assert ibm_df.loc[ibm_df["ev_type"] == "kairos:Primitives/Events/Contact.Contact.Broadcast", :].shape[0] > 0
        # To make a test with the new file
        # assert cmu_df.shape == (1, 31)
        # assert cmu_df.loc[0, "ev_id"] == 'ibm:TA2/VP1005.1013'
        # assert cmu_df.loc[0, "ev_name"] == 'Khaled Khayat and Mahmoud Khayat bomb the Etihad ' \
        #                                    'Airways flight from Sydney to Abu Dhabi using an a ' \
        #                                    'fully functional IED'


class TestRelationExtraction(object):
    """
    Class of tests to test EP1 Extraction
    """

    def test_simple_ta1_relation_extraction(self):
        config = configparser.ConfigParser()
        with open("./config.ini") as configfile:
            config.read_file(configfile)
            # ta2_output_directory = config['Test']['ta2_task2_output_directory']
        ta1_output_directory = config['Test']['ta1_output_directory']
        task2_score_directory = config['Test']['task2_score_directory']
        # annotation_directory = config['Test']['annotation_directory']
        # graph_g_directory = config['Test']['graph_g_directory']
        test_key_directory = config['Test']['test_key_directory']
        scripts.extract_relation.extract_ta1_relation_from_json('CMU_SIMPLE_FIRST', ta1_output_directory,
                                                                task2_score_directory)
        # This is an empty file so it is a crash test because there is no file.
        assert True

    def test_simple_graph_g_relation_extraction(self):
        config = configparser.ConfigParser()
        with open("./config.ini") as configfile:
            config.read_file(configfile)
        graph_g_directory = config['Test']['graph_g_directory']
        test_key_directory = config['Test']['test_key_directory']
        scripts.extract_relation.extract_graph_g_relation_from_json(graph_g_directory)
        graph_g_ce1005_events_fpath = os.path.join(graph_g_directory,
                                                   "ce1005/ce1005_GraphG_rel.csv")
        ce1005_rel_df = pd.read_csv(graph_g_ce1005_events_fpath)
        # We have 12 steps, so should have 12 rows
        assert ce1005_rel_df.shape == (8, 10)

        ce1005_rel_key_fpath = os.path.join(test_key_directory, "Graph_G/ce1005_GraphG_rel.csv")
        ce1005_rel_key_df = pd.read_csv(ce1005_rel_key_fpath)
        # Key file has been hand-checked for correctness
        assert ce1005_rel_df.equals(ce1005_rel_key_df)

    def test_simple_ta2_relation_extraction(self):
        config = configparser.ConfigParser()
        with open("./config.ini") as configfile:
            config.read_file(configfile)
        ta2_output_directory = config['Test']['ta2_task2_output_directory']
        task2_score_directory = config['Test']['task2_score_directory']
        test_key_directory = config['Test']['test_key_directory']
        annotation_directory = config['Test']['annotation_directory']
        scripts.extract_relation.extract_ta2_relation_from_json("IBM_SIMPLE", ta2_output_directory,
                                                                task2_score_directory, [], annotation_directory)
        ibm_ta2_simple_fpath = os.path.join(task2_score_directory, "IBM_SIMPLE/Relations/" \
                                                                   "ibm-ibm_simple-ce1011-task2_rel.csv")
        ibm_df = pd.read_csv(ibm_ta2_simple_fpath)
        # Check for a row with event ID


class TestOrderExtraction(object):
    """
    Class of tests to test EP1 Extraction
    """

    def test_simple_ta1_order_extraction(self):
        config = configparser.ConfigParser()
        with open("./config.ini") as configfile:
            config.read_file(configfile)
            # ta2_output_directory = config['Test']['ta2_task2_output_directory']
        ta1_output_directory = config['Test']['ta1_output_directory']
        task2_score_directory = config['Test']['task2_score_directory']
        # annotation_directory = config['Test']['annotation_directory']
        # graph_g_directory = config['Test']['graph_g_directory']
        test_key_directory = config['Test']['test_key_directory']
        scripts.extract_order.extract_ta1_order_from_json('CMU_SIMPLE_FIRST', ta1_output_directory,
                                                          task2_score_directory)
        cmu_simple_ta1_fpath = os.path.join(task2_score_directory, "TA1_library/"
                                                                   "CMU_SIMPLE_FIRST/Orders/cmu_simple_first-schemalib_order.csv")
        cmu_simple_ta1_df = pd.read_csv(cmu_simple_ta1_fpath)

        cmu_simple_ta1_key_fpath = os.path.join(test_key_directory,
                                                "Task2/TA1_library/CMU_SIMPLE_FIRST/Orders/"
                                                "cmu_simple_first-schemalib_order.csv")
        cmu_simple_ta1_key_df = pd.read_csv(cmu_simple_ta1_key_fpath)
        # Key is incorrect: transitive closure is missing.
        # One edge is (cmu:data$relm_KSD_xdoc_event_283.06972_1, cmu:schema_item_0/Steps/detonate-bombs_1)
        cmu_simple_edges = cmu_simple_ta1_df.loc[:, ["order_before", "order_after"]]
        assert ((cmu_simple_edges["order_before"] == "cmu:data$relm_KSD_xdoc_event_283.06972_1") & \
                ((cmu_simple_edges["order_after"] == "cmu:schema_item_0/Steps/ignite-bombs_1"))).any()
        assert ((cmu_simple_edges["order_before"] == "cmu:schema_item_0/Steps/ignite-bombs_1") & \
                ((cmu_simple_edges["order_after"] == "cmu:schema_item_0/Steps/detonate-bombs_1"))).any()
        # Insist on transitively-closed edge (for later)
        #assert ((cmu_simple_edges["order_before"] == "cmu:data$relm_KSD_xdoc_event_283.06972_1") & \
        #        ((cmu_simple_edges["order_after"] == "cmu:schema_item_0/Steps/detonate-bombs_1"))).any()
        # assert len(cmu_simple_ta1_key_df["ev_id"].value_counts()) == 22
        # assert len(cmu_simple_ta1_key_df["arg_id"].value_counts()) == 64
        # Partially checked by hand: use as regression test
        # assert cmu_simple_ta1_df.equals(cmu_simple_ta1_key_df)

    def test_simple_graph_g_order_extraction(self):
        config = configparser.ConfigParser()
        with open("./config.ini") as configfile:
            config.read_file(configfile)
        graph_g_directory = config['Test']['graph_g_directory']
        test_key_directory = config['Test']['test_key_directory']
        scripts.extract_order.extract_graph_g_order_from_json(graph_g_directory)
        graph_g_ce1005_events_fpath = os.path.join(graph_g_directory,
                                                   "ce1005/ce1005_GraphG_order.csv")
        ce1005_or_df = pd.read_csv(graph_g_ce1005_events_fpath)
        # We have 12 steps, so should have 12 rows
        assert ce1005_or_df.shape == (50, 3)

        ce1005_or_key_fpath = os.path.join(test_key_directory, "Graph_G/ce1005_GraphG_order.csv")
        ce1005_or_key_df = pd.read_csv(ce1005_or_key_fpath)
        # Key file has been hand-checked for correctness
        assert ce1005_or_df.equals(ce1005_or_key_df)

    def test_simple_ta2_order_extraction(self):
        config = configparser.ConfigParser()
        with open("./config.ini") as configfile:
            config.read_file(configfile)
        ta2_output_directory = config['Test']['ta2_task2_output_directory']
        task2_score_directory = config['Test']['task2_score_directory']
        test_key_directory = config['Test']['test_key_directory']
        scripts.extract_order.extract_ta2_order_from_json("IBM_SIMPLE", ta2_output_directory,
                                                          task2_score_directory, [])
        ibm_ta2_simple_fpath = os.path.join(task2_score_directory, "IBM_SIMPLE/Orders/" \
                                                                   "ibm-ibm_simple-ce1011-task2_order.csv")
        ibm_df = pd.read_csv(ibm_ta2_simple_fpath)
        # Check for transitive edges
        ibm_simple_edges = ibm_df.loc[:, ["order_before", "order_after"]]
        # Test 1
        assert ((ibm_simple_edges["order_before"] == "ibm:TA2/VP1011.1002") & \
                ((ibm_simple_edges["order_after"] == "ibm:TA2/VP1011.1004"))).any()
        assert ((ibm_simple_edges["order_before"] == "ibm:TA2/VP1011.1004") & \
                ((ibm_simple_edges["order_after"] == "ibm:TA2/VP1011.1056"))).any()
        # Insist on transitively-closed edge
        assert ((ibm_simple_edges["order_before"] == "ibm:TA2/VP1011.1002") & \
                ((ibm_simple_edges["order_after"] == "ibm:TA2/VP1011.1056"))).any()
        # Test 2, Hand-Checking that required transitivity edge is not in key
        assert ((ibm_simple_edges[
                     "order_before"] == "ibm:TA2/ibm_TA1/Schemas/Executed_VIED/Steps/step05AcquireVehicle.Transaction.ExchangeBuySell.Unspecified") & \
                ((ibm_simple_edges[
                      "order_after"] == "ibm:TA2/ibm_TA1/Schemas/Executed_VIED/Steps/step06LoadBombInVehicle.ArtifactExistence.ManufactureAssemble.Unspecified"))).any()
        assert ((ibm_simple_edges[
                     "order_before"] == "ibm:TA2/ibm_TA1/Schemas/Executed_VIED/Steps/step06LoadBombInVehicle.ArtifactExistence.ManufactureAssemble.Unspecified") & \
                ((ibm_simple_edges[
                      "order_after"] == "ibm:TA2/ibm_TA1/Schemas/Executed_VIED/Steps/step07Transport.Movement.Transportation.Unspecified"))).any()
        # Insist on transitively-closed edge
        assert ((ibm_simple_edges[
                     "order_before"] == "ibm:TA2/ibm_TA1/Schemas/Executed_VIED/Steps/step05AcquireVehicle.Transaction.ExchangeBuySell.Unspecified") & \
                ((ibm_simple_edges[
                      "order_after"] == "ibm:TA2/ibm_TA1/Schemas/Executed_VIED/Steps/step07Transport.Movement.Transportation.Unspecified"))).any()
