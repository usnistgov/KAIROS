#!/usr/bin/env python3

__author__ = "Xiongnan Jin (xiongnan.jin@nist.gov)"
__version__ = "Development: 0.3.0"
__date__ = "09/09/2020"

# This software was developed by employees of the National Institute of
# Standards and Technology (NIST), an agency of the Federal
# Government. Pursuant to title 17 United States Code Section 105, works
# of NIST employees are not subject to copyright protection in the
# United States and are considered to be in the public
# domain. Permission to freely use, copy, modify, and distribute this
# software and its documentation without fee is hereby granted, provided
# that this notice and disclaimer of warranty appears in all copies.

# THE SOFTWARE IS PROVIDED 'AS IS' WITHOUT ANY WARRANTY OF ANY KIND,
# EITHER EXPRESSED, IMPLIED, OR STATUTORY, INCLUDING, BUT NOT LIMITED
# TO, ANY WARRANTY THAT THE SOFTWARE WILL CONFORM TO SPECIFICATIONS, ANY
# IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
# PURPOSE, AND FREEDOM FROM INFRINGEMENT, AND ANY WARRANTY THAT THE
# DOCUMENTATION WILL CONFORM TO THE SOFTWARE, OR ANY WARRANTY THAT THE
# SOFTWARE WILL BE ERROR FREE. IN NO EVENT SHALL NIST BE LIABLE FOR ANY
# DAMAGES, INCLUDING, BUT NOT LIMITED TO, DIRECT, INDIRECT, SPECIAL OR
# CONSEQUENTIAL DAMAGES, ARISING OUT OF, RESULTING FROM, OR IN ANY WAY
# CONNECTED WITH THIS SOFTWARE, WHETHER OR NOT BASED UPON WARRANTY,
# CONTRACT, TORT, OR OTHERWISE, WHETHER OR NOT INJURY WAS SUSTAINED BY
# PERSONS OR PROPERTY OR OTHERWISE, AND WHETHER OR NOT LOSS WAS
# SUSTAINED FROM, OR AROSE OUT OF THE RESULTS OF, OR USE OF, THE
# SOFTWARE OR SERVICES PROVIDED HEREUNDER.

# Distributions of NIST software should also include copyright and
# licensing statements of any third-party software that are legally
# bundled with the code in compliance with the conditions of those
# licenses.

######################################################################################
# load oen json file or json files from a directory
######################################################################################
import json
import os

from tqdm import tqdm


def load_json_file(directory_path: str, file_name: str):
    suffix = file_name[-5:]
    # load only .json files
    if suffix == '.json':
        with open(directory_path + file_name) as json_file:
            json_data = json.load(json_file)
            return json_data
    else:
        return None


def load_json_directory(team_name: str, output_directory: str):
    json_dict = dict()
    directory_path = output_directory + team_name + '/'

    for file_name in os.listdir(directory_path):
        suffix = file_name[-5:]
        # load only .json files
        if suffix == '.json':
            with open(directory_path + file_name) as json_file:
                json_data = json.load(json_file)
                json_dict[file_name] = json_data

    return json_dict


if __name__ == "__main__":
    ta2_team_name = 'JHU'
    ta2_output_directory = '../../../../../Quizlet_3/TA2_outputs/'
    json_dict = load_json_directory(ta2_team_name, ta2_output_directory)
