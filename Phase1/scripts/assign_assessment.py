#!/usr/bin/env python3

__author__ = "Xiongnan Jin (xiongnan.jin@nist.gov)"
__version__ = "Development: 0.3.01"
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
# load data from extracted ep and arg csv files as pandas dataframe,
# add an assessment column and assign random values (true or false)
# if there exists the duplicated row that is already assessed,
# assign the same assessment result.
# temporally used before getting assessment results from LDC
######################################################################################
import pandas as pd
import numpy as np
import os


def get_duplication_index(target_row: pd.Series, df: pd.DataFrame):
    duplication_index = -1

    if len(df) == 0:
        return duplication_index

    df = df.drop(columns=['assessment'])
    target_row = target_row.drop(labels=['assessment'])

    for i, row in df.iterrows():
        if row.equals(target_row):
            duplication_index = i
            break

    return duplication_index


def assign_assessment(ta2_team_name: list, score_directory: str, target_ke: str):
    target_directory = ''
    if target_ke == 'event_primitive':
        target_directory = score_directory + ta2_team_name[0] + '/Event_primitives/'
    elif target_ke == 'argument':
        target_directory = score_directory + ta2_team_name[0] + '/Arguments/'
    elif target_ke == 'entity_relation':
        target_directory = score_directory + ta2_team_name[0] + '/Entity_relations/'

    for file_name in os.listdir(target_directory):
        df = pd.read_csv(target_directory + file_name)
        # add assessment column with default value 'NA'
        if 'assessment' not in df.columns:
            df.insert(0, 'assessment', 'n/a')

        # iterate df and assign True/False randomly
        for i, row in df.iterrows():
            assessed_df = df.loc[df['assessment'] != 'n/a']
            duplication_index = get_duplication_index(row, assessed_df)
            if duplication_index != -1:
                assessment = assessed_df.at[duplication_index, 'assessment']
            else:
                randint = np.random.randint(2, size=1)[0]
                assessment = 'True' if randint > 0 else 'False'
            df.at[i, 'assessment'] = assessment

        df.to_csv(target_directory + file_name, index=0)


if __name__ == "__main__":
    ta2_team_name = ['CMU']
    score_directory = '../../../../Quizlet_4/Score/'
    assign_assessment(ta2_team_name, score_directory)