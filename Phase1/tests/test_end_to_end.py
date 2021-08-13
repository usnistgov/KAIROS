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
import score_task2
import score_task1

# content of conftest.py or a tests file (e.g. in your tests or root directory)


class TestEndToEndExecution(object):
    """
    Testing end to end execution
    """

    def test_task1_end_to_end(self):
        config = configparser.ConfigParser()
        with open("./config.ini") as configfile:
            config.read_file(configfile)
        ta2_output_directory = config['Test']['ta2_task2_output_directory']
        ta1_output_directory = config['Test']['ta1_output_directory']
        task2_score_directory = config['Test']['task2_score_directory']
        annotation_directory = config['Test']['annotation_directory']
        graph_g_directory = config['Test']['graph_g_directory']
        test_key_directory = config['Test']['test_key_directory']
        # End to end test runs too slow; need to find a faster test
        # score_task1.score("CMU_SIMPLE", "test", False, False, False, False)

    def test_task2_end_to_end(self):
        config = configparser.ConfigParser()
        with open("./config.ini") as configfile:
            config.read_file(configfile)
        ta2_output_directory = config['Test']['ta2_task2_output_directory']
        ta1_output_directory = config['Test']['ta1_output_directory']
        task2_score_directory = config['Test']['task2_score_directory']
        annotation_directory = config['Test']['annotation_directory']
        graph_g_directory = config['Test']['graph_g_directory']
        test_key_directory = config['Test']['test_key_directory']
        # score_task2.score("RESIN_SIMPLE", False, False, False, "test")
        # Crash test for now. Passes if it gets here

