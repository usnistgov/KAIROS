import math
import numpy as np
import pandas as pd
import pytest
import sys
import os
import configparser

from unittest import TestCase
from score_task1 import str2bool


# content of conftest.py or a tests file (e.g. in your tests or root directory)

import sys
import os
import argparse
import score_task1


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

import scripts.score_task1_order
import scripts.score_task1_relation
import scripts.score_task1_event_argument

# testing score_task_1

# the import below works because we first added the location of score_task_2 path
# to the lib_path above
import scripts.score_task1_event_argument


class TestTask1Scoring(object):

    def test_replace_context(self):
        config = configparser.ConfigParser()
        with open("./config.ini") as configfile:
            config.read_file(configfile)
        ta1_output_directory = config['Test']['ta1_output_directory']
        annotation_directory = config['Test']['annotation_directory']
        ta2_task1_output_directory = config['Test']['ta2_task1_output_directory']
        task1_score_directory = config['Test']['task1_score_directory']
        assessment_directory = config['Test']['assessment_directory']
        annotation_directory = config['Test']['annotation_directory']
        caci_directory = config['Test']['caci_directory']
        # Everything has been pre-extracted
        assessment_directory = config['Test']['assessment_directory']
        test_key_directory = config['Test']['test_key_directory']

        arg_map_fpath = os.path.join(task1_score_directory, "simple_test_replace_context_1.csv")
        arg_key_fpath = os.path.join(test_key_directory, "Task_1", "simple_test_replace_context_1_key.csv")

        arg_map_df = pd.read_csv(arg_map_fpath, index_col=0)
        arg_key_df = pd.read_csv(arg_key_fpath, index_col=0)

        assert arg_map_df.loc[820832, "prov_id"] == \
               'https://www.ibm.com/CHRONOS/TA2/Task1/Z49fxbPQAo/Steps/K0C0483KB.rsd.txt.events_8/Participants/BodyPart_G5BeeSH'
        arg_map_repl_df = \
            scripts.score_task1_event_argument.replace_context(arg_map_df, 'prov_id')

        assert arg_map_repl_df.loc[820832, "prov_id"] == 'ibm:TA2/Task1/Z49fxbPQAo/Steps/K0C0483KB.rsd.txt.events_8/Participants/BodyPart_G5BeeSH'
        assert arg_map_repl_df.equals(arg_key_df)

    def test_t1_resin_ep_assessment_link(self):
        config = configparser.ConfigParser()
        with open("./config.ini") as configfile:
            config.read_file(configfile)
        ta1_output_directory = config['Test']['ta1_output_directory']
        annotation_directory = config['Test']['annotation_directory']
        ta2_task1_output_directory = config['Test']['ta2_task1_output_directory']
        task1_score_directory = config['Test']['task1_score_directory']
        assessment_directory = config['Test']['assessment_directory']
        annotation_directory = config['Test']['annotation_directory']
        caci_directory = config['Test']['caci_directory']
        # Everything has been pre-extracted
        assessment_directory = config['Test']['assessment_directory']
        test_key_directory = config['Test']['test_key_directory']
        # Can comment if everything is pre-extracted
        scripts.score_task1_event_argument.score("RESIN", "test", True, True)

        # Read extract file
        resin_ep_fpath = os.path.join(task1_score_directory, "RESIN", "Assessed_events", "resin-resin-ce1011-task1_ev_arg.csv")
        resin_ep_df = pd.read_csv(resin_ep_fpath)

        assert resin_ep_df.loc[resin_ep_df["ev_id"] == "resin:Schemas/car-IED/Steps/kairos:Primitives/Events/Cognitive.TeachingTrainingLearning.Unspecified:1",
                               "ev_match_status"].iloc[0] == "empty"
        assert resin_ep_df.loc[resin_ep_df["ev_id"] == "caci:Schemas/Instantiated/cluster_0/Steps/EN_Event_0000046",
                               "ev_match_status"].iloc[0] == "extra-irrelevant"
        assert resin_ep_df.loc[resin_ep_df["ev_id"] == "caci:Schemas/Instantiated/cluster_0/Steps/EN_Event_0000048",
                               "ev_match_status"].iloc[0] == "match"
        assert resin_ep_df.loc[resin_ep_df["ev_id"] == "caci:Schemas/Instantiated/cluster_0/Steps/EN_Event_0000048",
                               "ev_reference_id"].iloc[0] == "VP1011.1025"
        assert resin_ep_df.loc[resin_ep_df["ev_id"] == "caci:Schemas/Instantiated/cluster_0/Steps/EN_Event_0000052",
                               "ev_match_status"].iloc[0] == "extra-relevant"
        assert resin_ep_df.loc[resin_ep_df["ev_id"] == "caci:Schemas/Instantiated/cluster_0/Steps/EN_Event_0000052",
                               "ev_reference_id"].iloc[0] == "RES_RES_1011_0001_SEP00030"
        assert resin_ep_df.loc[resin_ep_df["ev_id"] == "caci:Schemas/Instantiated/cluster_0/Steps/EN_Event_0000052",
                               "ev_mapping_id"].iloc[0] == "RES_RES_1011_0001_SEP00030"

    def test_t1_resin_stats(self):
        config = configparser.ConfigParser()
        with open("./config.ini") as configfile:
            config.read_file(configfile)
        ta1_output_directory = config['Test']['ta1_output_directory']
        annotation_directory = config['Test']['annotation_directory']
        ta2_task1_output_directory = config['Test']['ta2_task1_output_directory']
        task1_score_directory = config['Test']['task1_score_directory']
        assessment_directory = config['Test']['assessment_directory']
        annotation_directory = config['Test']['annotation_directory']
        caci_directory = config['Test']['caci_directory']
        # Everything has been pre-extracted
        assessment_directory = config['Test']['assessment_directory']
        test_key_directory = config['Test']['test_key_directory']
        # Everything has been pre-extracted

        file_ev_stats_df, schema_ev_stats_df = scripts.score_task1_event_argument.compute_ke_stats("RESIN", task1_score_directory,
                                                                                                   annotation_directory, 'ev', 20)
        file_ev_score_df, schema_ev_score_df = scripts.score_task1_event_argument.score_file_and_schema_ke(file_ev_stats_df, schema_ev_stats_df, 'ev', 20)

        # 11 match, but 3 are extra relevant and true
        assert file_ev_score_df.loc[0, "distinct_true_ev_count"] == 11
        # 4 events are extra-relevant
        assert file_ev_score_df.loc[0, "true_ev_count"] == 25
        assert file_ev_score_df.loc[0, "ann_ev_count"] == 70
        assert file_ev_score_df.loc[0, "human_assessed_ev_count"] == 54
        assert file_ev_score_df.loc[0, "recall"] == ((file_ev_score_df.loc[0, "distinct_true_ev_count"] + file_ev_score_df.loc[0, "extra_relevant_ev_count"]) / file_ev_score_df.loc[0, "ann_ev_count"])
        assert file_ev_score_df.loc[0, "precision"] == ((file_ev_score_df.loc[0, "true_ev_count"] + file_ev_score_df.loc[0, "extra_relevant_ev_count"])  / file_ev_score_df.loc[0, "human_assessed_ev_count"])
        assert file_ev_score_df.loc[0, "f_measure"] == 2*file_ev_score_df.loc[0, "recall"]*file_ev_score_df.loc[0, "precision"]/ \
               (file_ev_score_df.loc[0, "precision"] + file_ev_score_df.loc[0, "recall"])

        # Check additional stats
        assert file_ev_score_df.loc[0, "match_ev_count"] == 25
        assert file_ev_score_df.loc[0, "match-inexact_ev_count"] == 0
        # Ignores the empty extra-relevant events
        assert file_ev_score_df.loc[0, "extra_relevant_ev_count"] == 3
        assert file_ev_score_df.loc[0, "extra-irrelevant_ev_count"] == 22
        assert file_ev_score_df.loc[0, "unassessed_ev_count"] == 20
        assert file_ev_score_df.loc[0, "non-match_ev_count"] == 0

        file_arg_stats_df, schema_arg_stats_df = scripts.score_task1_event_argument.compute_ke_stats("RESIN", task1_score_directory,
                                                                                                     annotation_directory, 'arg', 20)
        file_arg_score_df, schema_arg_score_df = scripts.score_task1_event_argument.score_file_and_schema_ke(file_arg_stats_df, schema_arg_stats_df,
                                                                                                             'arg', 20)

        assert file_arg_score_df.loc[0, "distinct_true_arg_count"] == 34
        assert file_arg_score_df.loc[0, "true_arg_count"] == 72
        assert file_arg_score_df.loc[0, "ann_arg_count"] == 275
        assert file_arg_score_df.loc[0, "human_assessed_arg_count"] == 101
        assert file_arg_score_df.loc[0, "recall"] == ((file_arg_score_df.loc[0, "distinct_true_arg_count"] + file_arg_score_df.loc[0, "extra_relevant_arg_count"]) / file_arg_score_df.loc[0, "ann_arg_count"])
        assert file_arg_score_df.loc[0, "precision"] == ((file_arg_score_df.loc[0, "true_arg_count"] + file_arg_score_df.loc[0, "extra_relevant_arg_count"]) / file_arg_score_df.loc[0, "human_assessed_arg_count"])
        assert file_arg_score_df.loc[0, "f_measure"] == 2*file_arg_score_df.loc[0, "recall"]*file_arg_score_df.loc[0, "precision"]/ \
               (file_arg_score_df.loc[0, "precision"] + file_arg_score_df.loc[0, "recall"])

        assert file_arg_score_df.loc[0, "match_arg_count"] == 13
        assert file_arg_score_df.loc[0, "match-inexact_arg_count"] == 59
        assert file_arg_score_df.loc[0, "extra_relevant_arg_count"] == 2
        assert file_arg_score_df.loc[0, "extra-irrelevant_arg_count"] == 27
        assert file_arg_score_df.loc[0, "unassessed_arg_count"] == 103
        assert file_arg_score_df.loc[0, "non-match_arg_count"] == 0


    def test_t1_cmu_arg_assessment_link(self):
        config = configparser.ConfigParser()
        with open("./config.ini") as configfile:
            config.read_file(configfile)
        ta1_output_directory = config['Test']['ta1_output_directory']
        annotation_directory = config['Test']['annotation_directory']
        ta2_task1_output_directory = config['Test']['ta2_task1_output_directory']
        task1_score_directory = config['Test']['task1_score_directory']
        assessment_directory = config['Test']['assessment_directory']
        annotation_directory = config['Test']['annotation_directory']
        caci_directory = config['Test']['caci_directory']
        # Everything has been pre-extracted
        assessment_directory = config['Test']['assessment_directory']
        test_key_directory = config['Test']['test_key_directory']
        # Can comment if everything is pre-extracted
        scripts.score_task1_event_argument.score("CMU", "test", True, True)

        # Read extract file
        cmu_ep_fpath = os.path.join(task1_score_directory, "CMU", "Assessed_arguments", "cmu-cmu-ce1011-task1_ev_arg.csv")
        cmu_ep_df = pd.read_csv(cmu_ep_fpath)

        assert cmu_ep_df.loc[cmu_ep_df["arg_id"] == "data:ent-K0C0487JY-text-cmu-r202012071623-29-c0",
                             "arg_match_status"].iloc[0] == "empty"
        assert cmu_ep_df.loc[cmu_ep_df["arg_id"] == "cmu:DOC-ea93fa5f1fcc156c0e12780c247d31b8-s80-evt-144Slots/Movement.Transportation.Unspecified/Slots/Transporter1",
                             "arg_match_status"].iloc[0] == "extra-irrelevant"
        assert cmu_ep_df.loc[cmu_ep_df["arg_id"] == "data:ent-K0C0483JC-text-cmu-r202012071623-179-c1",
                             "arg_match_status"].iloc[0] == "match"
        assert cmu_ep_df.loc[cmu_ep_df["arg_id"] == "data:ent-K0C0483JC-text-cmu-r202012071623-179-c1",
                             "arg_reference_id"].iloc[0] == "AR1011.1090"
        #assert cmu_ep_df.loc[cmu_ep_df["arg_id"] == "data:ent-K0C0483JC-text-cmu-r202012071623-179-c1",
        #                     "arg_mapping_id"].iloc[0] == "CMU_CMU_1011_0002_SKE00191"
        assert cmu_ep_df.loc[cmu_ep_df["arg_id"] == "data:ent-K0C0483JC-text-cmu-r202012071623-181-c0",
                             "arg_match_status"].iloc[0] == "match-inexact"
        assert cmu_ep_df.loc[cmu_ep_df["arg_id"] == "data:ent-K0C0483JC-text-cmu-r202012071623-181-c0",
                             "arg_reference_id"].iloc[0] == "AR1011.1016"
        #assert cmu_ep_df.loc[cmu_ep_df["arg_id"] == "data:ent-K0C0483JC-text-cmu-r202012071623-181-c0",
        #                       "arg_mapping_id"].iloc[0] == "CMU_CMU_1011_0002_SKE00146"

    def test_t1_cmu_stats(self):
        config = configparser.ConfigParser()
        with open("./config.ini") as configfile:
            config.read_file(configfile)
        ta1_output_directory = config['Test']['ta1_output_directory']
        annotation_directory = config['Test']['annotation_directory']
        ta2_task1_output_directory = config['Test']['ta2_task1_output_directory']
        task1_score_directory = config['Test']['task1_score_directory']
        assessment_directory = config['Test']['assessment_directory']
        annotation_directory = config['Test']['annotation_directory']
        caci_directory = config['Test']['caci_directory']
        # Everything has been pre-extracted
        assessment_directory = config['Test']['assessment_directory']
        test_key_directory = config['Test']['test_key_directory']
        # Everything has been pre-extracted

        file_ev_stats_df, schema_ev_stats_df = scripts.score_task1_event_argument.compute_ke_stats("CMU", task1_score_directory,
                                                                                                   annotation_directory, 'ev', 20)
        file_ev_score_df, schema_ev_score_df = scripts.score_task1_event_argument.score_file_and_schema_ke(file_ev_stats_df, schema_ev_stats_df, 'ev', 20)

        assert file_ev_score_df.loc[0, "distinct_true_ev_count"] == 9
        assert file_ev_score_df.loc[0, "true_ev_count"] == 27
        assert file_ev_score_df.loc[0, "ann_ev_count"] == 70
        assert file_ev_score_df.loc[0, "human_assessed_ev_count"] == 52
        assert file_ev_score_df.loc[0, "recall"] == ((file_ev_score_df.loc[0, "distinct_true_ev_count"] + file_ev_score_df.loc[0, "extra_relevant_ev_count"]) / file_ev_score_df.loc[0, "ann_ev_count"])
        assert file_ev_score_df.loc[0, "precision"] == ((file_ev_score_df.loc[0, "true_ev_count"] + file_ev_score_df.loc[0, "extra_relevant_ev_count"]) / file_ev_score_df.loc[0, "human_assessed_ev_count"])
        assert file_ev_score_df.loc[0, "f_measure"] == 2*file_ev_score_df.loc[0, "recall"]*file_ev_score_df.loc[0, "precision"]/ \
               (file_ev_score_df.loc[0, "precision"] + file_ev_score_df.loc[0, "recall"])

        # Check additional stats
        assert file_ev_score_df.loc[0, "match_ev_count"] == 27
        assert file_ev_score_df.loc[0, "match-inexact_ev_count"] == 0
        assert file_ev_score_df.loc[0, "extra_relevant_ev_count"] == 0
        assert file_ev_score_df.loc[0, "extra-irrelevant_ev_count"] == 25
        assert file_ev_score_df.loc[0, "unassessed_ev_count"] == 98
        assert file_ev_score_df.loc[0, "non-match_ev_count"] == 0

        file_arg_stats_df, schema_arg_stats_df = scripts.score_task1_event_argument.compute_ke_stats("CMU", task1_score_directory,
                                                                                                     annotation_directory, 'arg', 20)
        file_arg_score_df, schema_arg_score_df = scripts.score_task1_event_argument.score_file_and_schema_ke(file_arg_stats_df, schema_arg_stats_df,
                                                                                                             'arg', 20)

        assert file_arg_score_df.loc[0, "distinct_true_arg_count"] == 27
        assert file_arg_score_df.loc[0, "true_arg_count"] == 61
        assert file_arg_score_df.loc[0, "ann_arg_count"] == 275
        assert file_arg_score_df.loc[0, "human_assessed_arg_count"] == 77
        assert file_arg_score_df.loc[0, "recall"] == ((file_arg_score_df.loc[0, "distinct_true_arg_count"] + file_arg_score_df.loc[0, "extra_relevant_arg_count"]) / file_arg_score_df.loc[0, "ann_arg_count"])
        assert file_arg_score_df.loc[0, "precision"] == ((file_arg_score_df.loc[0, "true_arg_count"] + file_arg_score_df.loc[0, "extra_relevant_arg_count"]) / file_arg_score_df.loc[0, "human_assessed_arg_count"])
        assert file_arg_score_df.loc[0, "f_measure"] == 2*file_arg_score_df.loc[0, "recall"]*file_arg_score_df.loc[0, "precision"]/ \
               (file_arg_score_df.loc[0, "precision"] + file_arg_score_df.loc[0, "recall"])

        # Check additional stats
        assert file_arg_score_df.loc[0, "match_arg_count"] == 11
        assert file_arg_score_df.loc[0, "match-inexact_arg_count"] == 50
        assert file_arg_score_df.loc[0, "extra_relevant_arg_count"] == 0
        assert file_arg_score_df.loc[0, "extra-irrelevant_arg_count"] == 16
        assert file_arg_score_df.loc[0, "unassessed_arg_count"] == 172
        assert file_arg_score_df.loc[0, "non-match_arg_count"] == 0


    def test_t1_ibm_rel_assessment_link(self):
        config = configparser.ConfigParser()
        with open("./config.ini") as configfile:
            config.read_file(configfile)
        ta1_output_directory = config['Test']['ta1_output_directory']
        annotation_directory = config['Test']['annotation_directory']
        ta2_task1_output_directory = config['Test']['ta2_task1_output_directory']
        task1_score_directory = config['Test']['task1_score_directory']
        assessment_directory = config['Test']['assessment_directory']
        annotation_directory = config['Test']['annotation_directory']
        caci_directory = config['Test']['caci_directory']
        # Everything has been pre-extracted
        assessment_directory = config['Test']['assessment_directory']
        test_key_directory = config['Test']['test_key_directory']
        # Can comment if everything is pre-extracted
        scripts.score_task1_relation.score("IBM", "test", True, True)
        scripts.score_task1_event_argument.score("IBM", "test", True, True)

        # Read extract file
        ibm_ep_fpath = os.path.join(task1_score_directory, "IBM", "Assessed_relations", "ibm-ibm-ce1006-task1_rel.csv")
        ibm_ep_df = pd.read_csv(ibm_ep_fpath)

        assert ibm_ep_df.loc[ibm_ep_df["rel_id"] == "ibm:TA2/ibm:TA2/Task1/Ok4loi9Mrp/EventGraph/Relations/K0C047UIA.rsd.txt.K0C047UIA.rsd.txt-R8",
                             "rel_match_status"].iloc[0] == "extra-relevant"
        assert ibm_ep_df.loc[ibm_ep_df["rel_id"] == "ibm:TA2/ibm:TA2/Task1/Ok4loi9Mrp/EventGraph/Relations/K0C047UIA.rsd.txt.K0C047UIA.rsd.txt-R8",
                             "rel_mapping_id"].iloc[0] == "IBM_IBM_1006_0001_SEP10020"
        assert ibm_ep_df.loc[ibm_ep_df["rel_id"] == "ibm:TA2/ibm:TA2/Task1/Ok4loi9Mrp/EventGraph/Relations/K0C047UIA.rsd.txt.K0C047UIA.rsd.txt-R8",
                             "rel_reference_id"].iloc[0] == "empty"
        assert ibm_ep_df.loc[ibm_ep_df["rel_id"] == "ibm:TA2/ibm:TA2/Task1/Ok4loi9Mrp/EventGraph/Relations/K0C047UIH.rsd.txt.K0C047UIH.rsd.txt-R11",
                             "rel_match_status"].iloc[0] == "extra-relevant"
        assert ibm_ep_df.loc[ibm_ep_df["rel_id"] == "ibm:TA2/ibm:TA2/Task1/Ok4loi9Mrp/EventGraph/Relations/K0C047UIH.rsd.txt.K0C047UIH.rsd.txt-R11",
                             "rel_mapping_id"].iloc[0] == "IBM_IBM_1006_0001_SEP10003"
        assert ibm_ep_df.loc[ibm_ep_df["rel_id"] == "ibm:TA2/ibm:TA2/Task1/Ok4loi9Mrp/EventGraph/Relations/K0C047UIH.rsd.txt.K0C047UIH.rsd.txt-R11",
                             "rel_reference_id"].iloc[0] == "empty"

        assert ibm_ep_df.loc[ibm_ep_df["rel_id"] == "ibm:TA2/ibm:TA2/Task1/Ok4loi9Mrp/EventGraph/Relations/K0C047UID.rsd.txt.K0C047UID.rsd.txt-R20",
                             "rel_match_status"].iloc[0] == "match"
        assert ibm_ep_df.loc[ibm_ep_df["rel_id"] == "ibm:TA2/ibm:TA2/Task1/Ok4loi9Mrp/EventGraph/Relations/K0C047UID.rsd.txt.K0C047UID.rsd.txt-R20",
                             "rel_mapping_id"].iloc[0] == "IBM_IBM_1006_0001_SEP10028"
        assert ibm_ep_df.loc[ibm_ep_df["rel_id"] == "ibm:TA2/ibm:TA2/Task1/Ok4loi9Mrp/EventGraph/Relations/K0C047UID.rsd.txt.K0C047UID.rsd.txt-R20",
                             "rel_reference_id"].iloc[0] == "RR1006.1028"

        assert ibm_ep_df.loc[ibm_ep_df["rel_id"] == "ibm:TA2/ibm:TA2/Task1/Ok4loi9Mrp/EventGraph/Relations/K0C047UI8.rsd.txt.K0C047UI8.rsd.txt-R16",
                             "rel_match_status"].iloc[0] == "extra-irrelevant"
        # There are no empty relations in the file

    def test_t1_ibm_stats(self):
        config = configparser.ConfigParser()
        with open("./config.ini") as configfile:
            config.read_file(configfile)
        ta1_output_directory = config['Test']['ta1_output_directory']
        annotation_directory = config['Test']['annotation_directory']
        ta2_task1_output_directory = config['Test']['ta2_task1_output_directory']
        task1_score_directory = config['Test']['task1_score_directory']
        assessment_directory = config['Test']['assessment_directory']
        annotation_directory = config['Test']['annotation_directory']
        caci_directory = config['Test']['caci_directory']
        # Everything has been pre-extracted
        assessment_directory = config['Test']['assessment_directory']
        test_key_directory = config['Test']['test_key_directory']
        # Everything has been pre-extracted

        file_rel_stats_df, schema_rel_stats_df = scripts.score_task1_event_argument.compute_ke_stats("IBM", task1_score_directory,
                                                                                                   annotation_directory, 'rel', 20)
        file_rel_score_df, schema_rel_score_df = scripts.score_task1_event_argument.score_file_and_schema_ke(file_rel_stats_df, schema_rel_stats_df, 'rel', 20)

        assert file_rel_score_df.loc[0, "distinct_true_rel_count"] == 9
        # This ignores the two with an emtpy reference ID, including IBM_IBM_1006_0001_SEP10020
        assert file_rel_score_df.loc[0, "true_rel_count"] == 27
        assert file_rel_score_df.loc[0, "ann_rel_count"] == 50
        # The 2 extra-relevant relations that are empty are not correct
        assert file_rel_score_df.loc[0, "human_assessed_rel_count"] == 57
        assert file_rel_score_df.loc[0, "recall"] == ((file_rel_score_df.loc[0, "distinct_true_rel_count"] + file_rel_score_df.loc[0, "extra_relevant_rel_count"]) / file_rel_score_df.loc[0, "ann_rel_count"])
        assert file_rel_score_df.loc[0, "precision"] == (file_rel_score_df.loc[0, "true_rel_count"] / file_rel_score_df.loc[0, "human_assessed_rel_count"])
        assert file_rel_score_df.loc[0, "f_measure"] == 2*file_rel_score_df.loc[0, "recall"]*file_rel_score_df.loc[0, "precision"]/ \
               (file_rel_score_df.loc[0, "precision"] + file_rel_score_df.loc[0, "recall"])

        # Check extra stats
        # Check additional stats
        assert file_rel_score_df.loc[0, "match_rel_count"] == 27
        assert file_rel_score_df.loc[0, "match-inexact_rel_count"] == 0
        assert file_rel_score_df.loc[0, "extra_relevant_rel_count"] == 0
        assert file_rel_score_df.loc[0, "extra-irrelevant_rel_count"] == 12
        assert file_rel_score_df.loc[0, "unassessed_rel_count"] == 0
        assert file_rel_score_df.loc[0, "non-match_rel_count"] == 0




class TestScoreTask1_str2bool(TestCase):

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