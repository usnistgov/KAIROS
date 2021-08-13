#!/usr/bin/env python3

__author__ = "Xiongnan Jin (xiongnan.jin@nist.gov)"
__version__ = "Development: 0.5.2"
__date__ = "02/22/2021"

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
# load data from json file, extract order related information
# and save into a pandas table
######################################################################################
import copy
import json
import time

import pandas as pd
import os
import sys
import glob

from tqdm.auto import tqdm

scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../scripts")
sys.path.append(scripts_path)

from scripts import load_data as load


def init_ta2_order_dataframe() -> pd.DataFrame:
    order_df = pd.DataFrame(columns=['file_name', 'task2', 'schema_id', 'schema_super', 'order_id',
                                     'order_before', 'order_before_provenance',
                                     'order_after', 'order_after_provenance',
                                     'order_ta1ref', 'order_confidence', 'order_comment', 'order_flags'])

    return order_df


def init_ta1_order_dataframe() -> pd.DataFrame:
    order_df = pd.DataFrame(columns=['file_name', 'schema_id', 'schema_super', 'order_id',
                                     'order_before', 'order_after', 'order_confidence',
                                     'order_comment', 'order_flags'])

    return order_df


def check_exist_order(input_order_df: pd.DataFrame, schema_id: str, order_before: str, order_after: str):
    matching_df = input_order_df.loc[
        (input_order_df['schema_id'] == schema_id) &
        (input_order_df['order_before'] == order_before) &
        (input_order_df['order_after'] == order_after)
        ]

    if len(matching_df) > 0:
        return True
    else:
        return False


def compute_matching_orders(input_order_df: pd.DataFrame, ti_order_df: pd.DataFrame,
                            first_order_df: pd.DataFrame, second_order_df: pd.DataFrame) -> pd.DataFrame:
    for i, i_row in tqdm(first_order_df.iterrows(), total=first_order_df.shape[0], position=0, leave=True):
        file_name = i_row.get('file_name')
        task2 = i_row.get('task2')
        schema_super = i_row.get('schema_super')
        i_schema_id = i_row.get('schema_id')
        i_order_before = i_row.get('order_before')
        i_order_before_provenance = i_row.get('order_before_provenance')
        i_order_after = i_row.get('order_after')
        matching_order_df = second_order_df.loc[
            (second_order_df['schema_id'] == i_schema_id) &
            (second_order_df['order_before'] == i_order_after)]
        if len(matching_order_df) > 0:
            for j, j_row in matching_order_df.iterrows():
                j_order_after = j_row.get('order_after')
                j_order_after_provenance = j_row.get('order_after_provenance')
                if not check_exist_order(input_order_df, i_schema_id, i_order_before, j_order_after):
                    if not check_exist_order(ti_order_df, i_schema_id, i_order_before, j_order_after):
                        ti_order_row = {'file_name': file_name, 'task2': task2, 'schema_id': i_schema_id,
                                        'schema_super': schema_super, 'order_id': 'nist:scorer_inferred',
                                        'order_before': i_order_before,
                                        'order_before_provenance': i_order_before_provenance,
                                        'order_after': j_order_after,
                                        'order_after_provenance': j_order_after_provenance,
                                        'order_ta1ref': 'nist:scorer_inferred', 'order_confidence': 1,
                                        'order_comment': 'nist:scorer_inferred', 'order_flags': None}
                        ti_order_df = ti_order_df.append(ti_order_row, ignore_index=True)

    return ti_order_df


def transitive_inference(input_order_df: pd.DataFrame):
    exists_new = True
    loop = 0
    ti_order_df = init_ta2_order_dataframe()

    while exists_new:
        # initiate ti_order_df to store inferred
        loop += 1
        time.sleep(0.1)
        if loop == 1:
            pre_ti_order_df = copy.deepcopy(input_order_df)
            print('     loop: ' + str(loop) + ', input_order_df size:' + str(len(input_order_df)) +
                  ', pre_ti_order_df size: 0')
        else:
            pre_ti_order_df = ti_order_df
            print('     loop: ' + str(loop) + ', input_order_df size:' + str(len(input_order_df)) +
                  ', pre_ti_order_df size: ' + str(len(pre_ti_order_df)))

        time.sleep(0.1)
        if len(input_order_df) <= len(pre_ti_order_df):
            ti_order_df = compute_matching_orders(input_order_df, init_ta2_order_dataframe(),
                                                  input_order_df, pre_ti_order_df)
        else:
            ti_order_df = compute_matching_orders(input_order_df, init_ta2_order_dataframe(),
                                                  pre_ti_order_df, input_order_df)

        if len(ti_order_df) == 0:
            exists_new = False
        else:
            input_order_df = input_order_df.append(ti_order_df, ignore_index=True)

    return input_order_df


def extract_ta2_order_from_json(ta2_team_name: str, ta2_output_directory: str,
                                score_directory: str, target_pairs: list) -> pd.DataFrame:
    json_dict = load.load_json_directory(ta2_team_name, ta2_output_directory)

    ta2_csv_output_directory = score_directory + ta2_team_name
    # if the ta2 score folder does not exist, create the folder
    if not os.path.isdir(ta2_csv_output_directory):
        os.makedirs(ta2_csv_output_directory)

    order_csv_output_directory = score_directory + ta2_team_name + '/Orders/'
    # if the order_csv_output_directory does not exist, create the folder
    if not os.path.isdir(order_csv_output_directory):
        os.makedirs(order_csv_output_directory)
    # if the folder exists, clear the contents
    else:
        files = glob.glob(order_csv_output_directory + '*')
        for file in files:
            os.remove(file)

    index = 0
    for file_name in json_dict:
        index += 1
        print('Extracting ' + str(index) + ' of ' + str(len(json_dict)) + ' files...')
        # skip non-target TA1-TA2 pairs for Task1
        if target_pairs:
            fn_hyphen_list = file_name.split('-')
            ta1 = fn_hyphen_list[0].lower()
            ta2 = fn_hyphen_list[1].lower()
            key = ta1 + '-' + ta2
            if key not in target_pairs:
                continue

        order_df = init_ta2_order_dataframe()
        ev_arg_fn = score_directory + ta2_team_name + '/Event_arguments/' + \
                    file_name[:-5] + '_ev_arg.csv'
        if not os.path.isfile(ev_arg_fn):
            print('Cannot find file ' + ev_arg_fn + ' Please run score_task2_event_argument.py first')
            continue
        ev_arg_df = pd.read_csv(ev_arg_fn)
        json_data = json_dict[file_name]
        task_2 = False
        if 'task2' in json_data.keys():
            task_2 = json_data['task2']
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                schema_id = schema_super = None
                if '@id' in schema.keys():
                    schema_id = schema['@id']
                if 'super' in schema.keys():
                    schema_super = schema['super']
                if 'order' in schema.keys():
                    order_list = schema['order']
                    if order_list:
                        for order in order_list:
                            # initiate variables of order
                            order_id = order_after = order_before = order_ta1ref = None
                            order_confidence = order_comment = order_flags = None
                            # get order info
                            if '@id' in order.keys():
                                order_id = str(order['@id'])
                            if 'after' in order.keys():
                                order_after = order['after']
                            if 'before' in order.keys():
                                order_before = order['before']
                            if 'ta1ref' in order.keys():
                                order_ta1ref = str(order['ta1ref'])
                            if 'confidence' in order.keys():
                                order_confidence = str(order['confidence'])
                            if 'comment' in order.keys():
                                order_comment = str(order['comment'])
                            if 'flags' in order.keys():
                                order_flags = str(order['flags'])
                            # iterate all before-after-pairs and add them to order_df
                            order_before_list = []
                            if isinstance(order_before, str):
                                order_before_list.append(order_before)
                            elif isinstance(order_before, list):
                                order_before_list = order_before
                            order_after_list = []
                            if isinstance(order_after, str):
                                order_after_list.append(order_after)
                            elif isinstance(order_after, list):
                                order_after_list = order_after
                            for ob in order_before_list:
                                ob_df = ev_arg_df.loc[ev_arg_df['ev_id'] == ob]
                                ob_prov = ''
                                if len(ob_df) > 0:
                                    ob_prov = ob_df.iloc[0]['ev_provenance']
                                for oa in order_after_list:
                                    oa_df = ev_arg_df.loc[ev_arg_df['ev_id'] == oa]
                                    oa_prov = ''
                                    if len(oa_df) > 0:
                                        oa_prov = oa_df.iloc[0]['ev_provenance']
                                    order_row = {'file_name': file_name, 'task2': task_2,
                                                 'schema_id': schema_id, 'schema_super': schema_super,
                                                 'order_id': order_id,
                                                 'order_before': ob, 'order_before_provenance': ob_prov,
                                                 'order_after': oa, 'order_after_provenance': oa_prov,
                                                 'order_ta1ref': order_ta1ref, 'order_confidence': order_confidence,
                                                 'order_comment': order_comment, 'order_flags': order_flags}
                                    order_df = order_df.append(order_row, ignore_index=True)
        # save non-empty order_df to csv files
        if len(order_df) > 0:
            order_with_ti_df = transitive_inference(order_df)
            order_with_ti_df.to_csv(order_csv_output_directory + file_name[:-5] + '_order.csv', index=False)


def extract_graph_g_order_from_json(graph_g_directory: str):
    for file_name in tqdm(os.listdir(graph_g_directory), position=0, leave=False):
        suffix = file_name[-5:]
        # load only .json files
        if suffix == '.json':
            with open(graph_g_directory + file_name) as json_file:
                json_data = json.load(json_file)
        else:
            continue

        ce = file_name.split('_')[0]
        graph_g_csv_output_directory = graph_g_directory + ce + '/'
        # if the graph_g_csv_output_directory folder does not exist, create the folder
        if not os.path.isdir(graph_g_csv_output_directory):
            os.makedirs(graph_g_csv_output_directory)

        order_df = pd.DataFrame(columns=['file_name', 'order_after', 'order_before'])
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                if 'order' in schema.keys():
                    order_list = schema['order']
                    if order_list:
                        for order in order_list:
                            # initiate variables of order
                            order_after = order_before = None
                            # get order info
                            if 'before' in order.keys():
                                order_before = order['before']
                            if 'after' in order.keys():
                                order_after = order['after']
                            # iterate all before-after-pairs and add them to order_df
                            order_before_list = []
                            if isinstance(order_before, str):
                                order_before_list.append(order_before)
                            elif isinstance(order_before, list):
                                order_before_list = order_before
                            order_after_list = []
                            if isinstance(order_after, str):
                                order_after_list.append(order_after)
                            elif isinstance(order_after, list):
                                order_after_list = order_after
                            for ob in order_before_list:
                                for oa in order_after_list:
                                    order_row = {'file_name': file_name, 'order_before': ob, 'order_after': oa}
                                    order_df = order_df.append(order_row, ignore_index=True)
        # save non-empty order_df to csv files
        if len(order_df) > 0:
            order_df.to_csv(graph_g_csv_output_directory + file_name[:-5] + '_order.csv', index=False)


def extract_ta1_order_from_json(ta1_team_name: str, ta1_output_directory: str, score_directory: str):
    json_dict = load.load_json_directory(ta1_team_name, ta1_output_directory)

    ta1_library_directory = score_directory + 'TA1_library/'
    # if the ta1 library folder does not exist, create the folder
    if not os.path.isdir(ta1_library_directory):
        os.makedirs(ta1_library_directory)

    ta1_csv_output_directory = ta1_library_directory + ta1_team_name
    # if the ta1 csv output folder does not exist, create the folder
    if not os.path.isdir(ta1_csv_output_directory):
        os.makedirs(ta1_csv_output_directory)

    order_csv_output_directory = ta1_csv_output_directory + '/Orders/'
    # if the order_csv_output_directory does not exist, create the folder
    if not os.path.isdir(order_csv_output_directory):
        os.makedirs(order_csv_output_directory)
    # if the folder exists, clear the contents
    else:
        files = glob.glob(order_csv_output_directory + '*')
        for file in files:
            os.remove(file)

    for file_name in tqdm(json_dict):
        order_df = init_ta1_order_dataframe()
        json_data = json_dict[file_name]
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                schema_id = schema_super = None
                if '@id' in schema.keys():
                    schema_id = schema['@id']
                if 'super' in schema.keys():
                    schema_super = schema['super']
                if 'order' in schema.keys():
                    order_list = schema['order']
                    if order_list:
                        for order in order_list:
                            # initiate variables of order
                            order_id = order_before = order_after = None
                            order_confidence = order_comment = order_flags = None
                            # get order info
                            if '@id' in order.keys():
                                order_id = str(order['@id'])
                            if 'before' in order.keys():
                                order_before = order['before']
                            if 'after' in order.keys():
                                order_after = order['after']
                            if 'confidence' in order.keys():
                                order_confidence = str(order['confidence'])
                            if 'comment' in order.keys():
                                order_comment = str(order['comment'])
                            if 'flags' in order.keys():
                                order_flags = str(order['flags'])
                            # iterate all before-after-pairs and add them to order_df
                            order_before_list = []
                            if isinstance(order_before, str):
                                order_before_list.append(order_before)
                            elif isinstance(order_before, list):
                                order_before_list = order_before
                            order_after_list = []
                            if isinstance(order_after, str):
                                order_after_list.append(order_after)
                            elif isinstance(order_after, list):
                                order_after_list = order_after
                            for ob in order_before_list:
                                for oa in order_after_list:
                                    order_row = {'file_name': file_name, 'schema_id': schema_id,
                                                 'schema_super': schema_super,
                                                 'order_id': order_id, 'order_after': oa,
                                                 'order_before': ob, 'order_confidence': order_confidence,
                                                 'order_comment': order_comment, 'order_flags': order_flags}
                                    order_df = order_df.append(order_row, ignore_index=True)
        # save non-empty order_df to csv files
        if len(order_df) > 0:
            order_df.to_csv(order_csv_output_directory + file_name[:-5] + '_order.csv', index=False)


def read_temporal_annotation(annotation_directory: str, target_ce: str) -> pd.DataFrame:
    temporal_fn = annotation_directory + target_ce + '/' + target_ce + '_temporal.xlsx'
    if not os.path.isfile(temporal_fn):
        sys.exit('File not found: ' + temporal_fn)
    temporal_df = pd.read_excel(temporal_fn)

    return temporal_df


def assess_order_pair(order_before_1_type: str, order_before_1_value: str,
                      order_before_2_type: str, order_before_2_value: str,
                      order_after_1_type: str, order_after_1_value: str,
                      order_after_2_type: str, order_after_2_value: str) -> bool:
    # assume that no 2_type_after is assigned with 1_type_before
    if order_before_1_type == 'before' and order_before_2_type == 'after':
        print('2_type_after is assigned with 1_type_before')
    elif order_after_1_type == 'before' and order_after_2_type == 'after':
        print('2_type_after is assigned with 1_type_before')

    # both are exactly
    if order_before_1_type == 'exactly' and order_after_1_type == 'exactly':
        if int(order_before_1_value) < int(order_after_1_value):
            return True
        else:
            return False
    # one of them is exactly
    # before is exactly
    elif order_before_1_type == 'exactly' and order_after_1_type == 'before':
        return False
    elif order_before_1_type == 'exactly' and order_after_1_type == 'after':
        if int(order_before_1_value) <= int(order_after_1_value):
            return True
        else:
            return False
    # after is exactly
    elif order_before_1_type == 'before' and order_after_1_type == 'exactly':
        if int(order_before_1_value) <= int(order_after_1_value):
            return True
        else:
            return False
    elif order_before_1_type == 'after' and order_after_1_type == 'exactly':
        if order_before_2_type == 'EMPTY_NA':
            return False
        elif order_before_2_type == 'before':
            if int(order_before_2_value) <= int(order_after_1_value):
                return True
            else:
                return False
    # neither is exactly
    elif order_before_1_type == 'before' and order_after_1_type == 'before':
        return False
    elif order_before_1_type == 'before' and order_after_1_type == 'after':
        if int(order_before_1_value) <= int(order_after_1_value):
            return True
        else:
            return False
    elif order_before_1_type == 'after' and order_after_1_type == 'before':
        return False
    elif order_before_1_type == 'after' and order_after_1_type == 'after':
        if order_before_2_type == 'unknown':
            return False
        elif order_before_2_type == 'before':
            if int(order_before_2_value) <= int(order_after_1_value):
                return True
            else:
                return False


def compute_reference_order_from_single_temporal_annotation(temporal_df: pd.DataFrame) -> pd.DataFrame:
    order_df = pd.DataFrame(columns=['before', 'after'])

    if len(temporal_df) == 0:
        return order_df

    for i, i_row in temporal_df.iterrows():
        order_before = i_row.get('eventprimitive_id')
        i_order1_type = i_row.get('order1_type')
        i_order1_value = i_row.get('order1_value')
        i_order2_type = i_row.get('order2_type')
        i_order2_value = i_row.get('order2_value')
        j_list = []
        for j, j_row in temporal_df.iterrows():
            if i != j:
                # if event i is before event j, add j to j_list
                j_order1_type = j_row.get('order1_type')
                j_order1_value = j_row.get('order1_value')
                j_order2_type = j_row.get('order2_type')
                j_order2_value = j_row.get('order2_value')
                if assess_order_pair(i_order1_type, i_order1_value, i_order2_type, i_order2_value,
                                     j_order1_type, j_order1_value, j_order2_type, j_order2_value):
                    order_after = j_row.get('eventprimitive_id')
                    j_list.append(order_after)
        if len(j_list) > 0:
            for order_after in j_list:
                order_row = {'before': order_before, 'after': order_after}
                order_df = order_df.append(order_row, ignore_index=True)

    return order_df


def extract_reference_order(annotation_directory: str) -> None:
    for target_ce in tqdm(os.listdir(annotation_directory), position=0, leave=False):
        # skip non-target_ce folder, e.g., .DS_store
        if '.' in target_ce:
            continue
        temporal_df = read_temporal_annotation(annotation_directory, target_ce)
        # remove the rows with order1_type as unknown, which are meaningless to compute reference orders
        temporal_df = temporal_df.loc[temporal_df['order1_type'] != 'unknown']
        # get reference order
        order_df = compute_reference_order_from_single_temporal_annotation(temporal_df)
        # write file
        order_fn = annotation_directory + target_ce + '/' + target_ce + '_order.xlsx'
        if len(order_df) > 0:
            order_df.to_excel(order_fn, index=False)


if __name__ == "__main__":
    ta1_team_name = 'CMU'
    ta1_output_directory = '/Users/xnj1/OneDrive - National Institute of Standards and Technology (NIST)/KAIROS/Quizlet_4/TA1_outputs/'
    score_directory = '/Users/xnj1/OneDrive - National Institute of Standards and Technology (NIST)/KAIROS/Quizlet_4/Score/'
    extract_ta1_order_from_json(ta1_team_name, ta1_output_directory, score_directory)
