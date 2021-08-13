#!/usr/bin/env python3

__author__ = "Xiongnan Jin (xiongnan.jin@nist.gov)"
__version__ = "Development: 0.5.2"
__date__ = "03/26/2021"

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
# load data from TA2 system output json files, extract step related information
# using pandas dataframe and save as csv files
######################################################################################

import pandas as pd
import os
import sys
import glob
import json
from tqdm import tqdm

scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../scripts")
sys.path.append(scripts_path)

from scripts import load_data as load


def init_ta2_ev_arg_dataframe(task2: bool) -> pd.DataFrame:
    if task2:
        ev_arg_df = pd.DataFrame(columns=['file_name', 'task2', 'schema_id', 'schema_super', 'ev_id',
                                          'ev_type', 'ev_name', 'ev_ta1ref', 'ev_confidence', 'ev_comment',
                                          'ev_provenance',
                                          'ev_modality', 'ev_earliestStartTime', 'ev_latestStartTime',
                                          'ev_earliestEndTime', 'ev_latestEndTime', 'ev_requires', 'ev_achieves',
                                          'arg_name', 'arg_id', 'arg_role', 'arg_type', 'arg_refvar', 'arg_comment',
                                          'arg_aka', 'arg_reference', 'arg_provenance', 'val_name', 'val_confidence',
                                          'val_provenance', 'val_type'])
    else:
        ev_arg_df = pd.DataFrame(columns=['file_name', 'task2', 'schema_id', 'schema_super', 'ev_id',
                                          'ev_type', 'ev_name', 'ev_ta1ref', 'ev_confidence', 'ev_comment',
                                          'ev_provenance', 'ev_prov_childID', 'ev_prov_mediaType',
                                          'ev_modality', 'ev_earliestStartTime', 'ev_latestStartTime',
                                          'ev_earliestEndTime', 'ev_latestEndTime', 'ev_requires', 'ev_achieves',
                                          'arg_name', 'arg_id', 'arg_role', 'arg_type', 'arg_refvar', 'arg_comment',
                                          'arg_aka', 'arg_reference', 'arg_provenance', 'val_name', 'val_confidence',
                                          'val_type', 'val_provenance', 'val_prov_childID', 'val_prov_mediaType'])

    return ev_arg_df


def init_ta1_ev_arg_dataframe() -> pd.DataFrame:
    ev_arg_df = pd.DataFrame(columns=['file_name', 'schema_id', 'schema_super', 'schema_version',
                                      'ev_id', 'ev_name', 'ev_type',
                                      'ev_confidence', 'ev_achieves', 'ev_requires', 'ev_modality',
                                      'ev_aka', 'ev_reference', 'ev_comment',
                                      'arg_id', 'arg_name', 'arg_role', 'arg_refvar', 'arg_type',
                                      'arg_reference', 'arg_aka', 'arg_comment'])

    return ev_arg_df


def get_provenance_data(schema_provenance_list: list) -> dict:
    provenance_data = {}
    for provenance_dict in schema_provenance_list:
        provenance = provenance_dict['provenance']
        provenance_data[provenance] = provenance_dict

    return provenance_data


def get_first_provenance(ke_provenance) -> str:
    if isinstance(ke_provenance, str):
        ke_provenance = ke_provenance.replace('[', '')
        ke_provenance = ke_provenance.replace(']', '')
        ke_provenance = ke_provenance.replace('\'', '')
        ke_provenance = ke_provenance.replace(' ', '')

        if ',' not in ke_provenance:
            return ke_provenance
        else:
            ke_provenance_list = ke_provenance.split(',')
            return ke_provenance_list[0]
    elif isinstance(ke_provenance, list):
        return ke_provenance[0]


def extract_ta2_ev_arg_from_json(ta2_team_name: str, ta2_output_directory: str,
                                 score_directory: str, target_pairs: list, annotation_directory: str) -> None:
    json_dict = load.load_json_directory(ta2_team_name, ta2_output_directory)

    ta2_csv_output_directory = score_directory + ta2_team_name + '/'
    # if the ta2 score folder does not exist, create the folder
    if not os.path.isdir(ta2_csv_output_directory):
        os.makedirs(ta2_csv_output_directory)

    ev_arg_csv_output_directory = ta2_csv_output_directory + 'Event_arguments/'
    # if the ev arg scv output does not exist, create the folder
    if not os.path.isdir(ev_arg_csv_output_directory):
        os.makedirs(ev_arg_csv_output_directory)
    # if the folder exists, clear the contents
    else:
        files = glob.glob(ev_arg_csv_output_directory + '*')
        for file in files:
            os.remove(file)

    language_mapping_fp = annotation_directory + 'document_profile.tab'
    if not os.path.isfile(language_mapping_fp):
        sys.exit('File not found: ' + language_mapping_fp)
    language_mapping_df = pd.read_table(language_mapping_fp)

    parent_children_mapping_fp = annotation_directory + 'parent_children.tab'
    if not os.path.isfile(parent_children_mapping_fp):
        sys.exit('File not found: ' + parent_children_mapping_fp)
    parent_children_mapping_df = pd.read_table(parent_children_mapping_fp)

    for file_name in tqdm(json_dict, position=0, leave=False):
        # skip non-target TA1-TA2 pairs for Task1
        if target_pairs:
            fn_hyphen_list = file_name.split('-')
            ta1 = fn_hyphen_list[0].lower()
            ta2 = fn_hyphen_list[1].lower()
            key = ta1 + '-' + ta2
            if key not in target_pairs:
                continue
        json_data = json_dict[file_name]
        task2 = False
        if 'task2' in json_data.keys():
            task2 = json_data['task2']
        ev_arg_df = init_ta2_ev_arg_dataframe(task2)

        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                provenance_data = {}
                if not task2:
                    provenance_data = get_provenance_data(schema['provenanceData'])
                # initiate schema elements
                schema_id = schema_super = None
                # get schema elements
                if '@id' in schema.keys():
                    schema_id = schema['@id']
                if 'super' in schema.keys():
                    schema_super = schema['super']
                if 'steps' in schema.keys():
                    step_list = schema['steps']
                    if step_list:
                        for step in step_list:
                            # initiate event elements
                            ev_id = ev_type = ev_name = ev_ta1ref = ev_confidence = ev_comment = None
                            ev_provenance = ev_modality = ev_earliestStartTime = ev_latestStartTime = None
                            ev_earliestEndTime = ev_latestEndTime = ev_requires = ev_achieves = None
                            if not task2:
                                ev_prov_childID = ev_prov_mediaType = None
                            # get event elements
                            if '@id' in step.keys():
                                ev_id = str(step['@id'])
                            if '@type' in step.keys():
                                ev_type = str(step['@type'])
                            if 'name' in step.keys():
                                ev_name = str(step['name'])
                            if 'ta1ref' in step.keys():
                                ev_ta1ref = str(step['ta1ref'])
                            if 'confidence' in step.keys():
                                ev_confidence = str(step['confidence'])
                            if 'comment' in step.keys():
                                ev_comment = str(step['comment'])
                            if 'provenance' in step.keys():
                                ev_provenance = str(step['provenance'])
                                first_ev_provenance = get_first_provenance(ev_provenance)
                                if not task2 and first_ev_provenance in provenance_data.keys():
                                    ev_prov_childID = provenance_data[first_ev_provenance]['childID']
                                    ev_prov_mediaType = provenance_data[first_ev_provenance]['mediaType']
                            if 'modality' in step.keys():
                                ev_modality = str(step['modality'])
                            if 'requires' in step.keys():
                                ev_requires = str(step['requires'])
                            if 'achieves' in step.keys():
                                ev_achieves = str(step['achieves'])
                            # 4-tuple of temporal information
                            if 'temporal' in step.keys():
                                temporal_list = step['temporal']
                                if temporal_list:
                                    for temporal in temporal_list:
                                        # no ev temporal types, e.g., start_datetime_type
                                        if 'earliestStartTime' in temporal.keys():
                                            ev_earliestStartTime = str(temporal['earliestStartTime'])
                                        if 'latestStartTime' in temporal.keys():
                                            ev_latestStartTime = str(temporal['latestStartTime'])
                                        if 'earliestEndTime' in temporal.keys():
                                            ev_earliestEndTime = str(temporal['earliestEndTime'])
                                        if 'latestEndTime' in temporal.keys():
                                            ev_latestEndTime = str(temporal['latestEndTime'])

                            if 'participants' in step.keys():
                                participant_list = step['participants']
                                # if there is no participant (argument), add event with an empty argument
                                if not participant_list:
                                    ev_arg_row = {'file_name': file_name, 'task2': task2,
                                                  'schema_id': schema_id,
                                                  'schema_super': schema_super,
                                                  'ev_id': ev_id, 'ev_type': ev_type,
                                                  'ev_name': ev_name, 'ev_ta1ref': ev_ta1ref,
                                                  'ev_confidence': ev_confidence,
                                                  'ev_comment': ev_comment,
                                                  'ev_provenance': ev_provenance}
                                    if not task2:
                                        ev_arg_row.update({'ev_prov_childID': ev_prov_childID,
                                                           'ev_prov_mediaType': ev_prov_mediaType})
                                    ev_arg_row.update({'ev_modality': ev_modality,
                                                       'ev_earliestStartTime': ev_earliestStartTime,
                                                       'ev_latestStartTime': ev_latestStartTime,
                                                       'ev_earliestEndTime': ev_earliestEndTime,
                                                       'ev_latestEndTime': ev_latestEndTime,
                                                       'ev_requires': ev_requires,
                                                       'ev_achieves': ev_achieves,
                                                       'arg_name': 'empty', 'arg_id': 'empty',
                                                       'arg_role': 'empty', 'arg_type': 'empty',
                                                       'arg_refvar': 'empty', 'arg_comment': 'empty',
                                                       'arg_aka': 'empty', 'arg_reference': 'empty',
                                                       'arg_provenance': 'empty',
                                                       'val_name': 'empty',
                                                       'val_confidence': 'empty',
                                                       'val_type': 'empty', 'val_provenance': 'empty'
                                                       })
                                    if not task2:
                                        ev_arg_row.update({'val_prov_childID': 'empty',
                                                           'val_prov_mediaType': 'empty'})
                                    ev_arg_df = ev_arg_df.append(ev_arg_row, ignore_index=True)
                                else:
                                    val_count = 0
                                    for participant in participant_list:
                                        # initiate argument elements
                                        arg_name = arg_id = arg_role = arg_type = arg_refvar = None
                                        arg_comment = arg_aka = arg_reference = arg_provenance = None
                                        if not task2:
                                            val_prov_childID = val_prov_mediaType = None
                                        # get argument elements
                                        if 'name' in participant.keys():
                                            arg_name = str(participant['name'])
                                        if '@id' in participant.keys():
                                            arg_id = str(participant['@id'])
                                        if 'role' in participant.keys():
                                            arg_role = str(participant['role'])
                                        if 'entityTypes' in participant.keys():
                                            arg_type = str(participant['entityTypes'])
                                        if 'refvar' in participant.keys():
                                            arg_refvar = str(participant['refvar'])
                                        if 'comment' in participant.keys():
                                            arg_comment = str(participant['comment'])
                                        if 'aka' in participant.keys():
                                            arg_aka = str(participant['aka'])
                                        if 'reference' in participant.keys():
                                            arg_reference = str(participant['reference'])
                                        if 'provenance' in participant.keys():
                                            arg_provenance = str(participant['provenance'])
                                        if 'values' in participant.keys():
                                            value_list = participant['values']
                                            # only add values if value list is not empty
                                            if value_list:
                                                for value in value_list:
                                                    val_count += 1
                                                    # initiate value elements
                                                    val_name = val_confidence = val_provenance = val_type = None
                                                    if 'name' in value.keys():
                                                        val_name = str(value['name'])
                                                    if 'confidence' in value.keys():
                                                        val_confidence = str(value['confidence'])
                                                    if 'provenance' in value.keys():
                                                        val_provenance = str(value['provenance'])
                                                        first_val_provenance = get_first_provenance(val_provenance)
                                                        if not task2 and first_val_provenance in provenance_data.keys():
                                                            val_prov_childID = provenance_data[first_val_provenance][
                                                                'childID']
                                                            val_prov_mediaType = provenance_data[first_val_provenance][
                                                                'mediaType']
                                                    if 'entityTypes' in value.keys():
                                                        val_type = str(value['entityTypes'])
                                                    # add a row to arg_df
                                                    ev_arg_row = {'file_name': file_name, 'task2': task2,
                                                                  'schema_id': schema_id,
                                                                  'schema_super': schema_super,
                                                                  'ev_id': ev_id, 'ev_type': ev_type,
                                                                  'ev_name': ev_name, 'ev_ta1ref': ev_ta1ref,
                                                                  'ev_confidence': ev_confidence,
                                                                  'ev_comment': ev_comment,
                                                                  'ev_provenance': ev_provenance}
                                                    if not task2:
                                                        ev_arg_row.update({'ev_prov_childID': ev_prov_childID,
                                                                           'ev_prov_mediaType': ev_prov_mediaType})
                                                    ev_arg_row.update({'ev_modality': ev_modality,
                                                                       'ev_earliestStartTime': ev_earliestStartTime,
                                                                       'ev_latestStartTime': ev_latestStartTime,
                                                                       'ev_earliestEndTime': ev_earliestEndTime,
                                                                       'ev_latestEndTime': ev_latestEndTime,
                                                                       'ev_requires': ev_requires,
                                                                       'ev_achieves': ev_achieves,
                                                                       'arg_name': arg_name, 'arg_id': arg_id,
                                                                       'arg_role': arg_role, 'arg_type': arg_type,
                                                                       'arg_refvar': arg_refvar,
                                                                       'arg_comment': arg_comment,
                                                                       'arg_aka': arg_aka,
                                                                       'arg_reference': arg_reference,
                                                                       'arg_provenance': arg_provenance,
                                                                       'val_name': val_name,
                                                                       'val_confidence': val_confidence,
                                                                       'val_type': val_type,
                                                                       'val_provenance': val_provenance})
                                                    if not task2:
                                                        ev_arg_row.update({'val_prov_childID': val_prov_childID,
                                                                           'val_prov_mediaType': val_prov_mediaType})
                                                    ev_arg_df = ev_arg_df.append(ev_arg_row, ignore_index=True)
                                    # if there is no instantiated argument, add event with an empty argument
                                    if val_count == 0:
                                        ev_arg_row = {'file_name': file_name, 'task2': task2,
                                                      'schema_id': schema_id,
                                                      'schema_super': schema_super,
                                                      'ev_id': ev_id, 'ev_type': ev_type,
                                                      'ev_name': ev_name, 'ev_ta1ref': ev_ta1ref,
                                                      'ev_confidence': ev_confidence,
                                                      'ev_comment': ev_comment,
                                                      'ev_provenance': ev_provenance}
                                        if not task2:
                                            ev_arg_row['ev_prov_childID'] = ev_prov_childID
                                            ev_arg_row['ev_prov_mediaType'] = ev_prov_mediaType
                                        ev_arg_row.update({'ev_modality': ev_modality,
                                                           'ev_earliestStartTime': ev_earliestStartTime,
                                                           'ev_latestStartTime': ev_latestStartTime,
                                                           'ev_earliestEndTime': ev_earliestEndTime,
                                                           'ev_latestEndTime': ev_latestEndTime,
                                                           'ev_requires': ev_requires,
                                                           'ev_achieves': ev_achieves,
                                                           'arg_name': 'empty', 'arg_id': 'empty',
                                                           'arg_role': 'empty', 'arg_type': 'empty',
                                                           'arg_refvar': 'empty', 'arg_comment': 'empty',
                                                           'arg_aka': 'empty', 'arg_reference': 'empty',
                                                           'arg_provenance': 'empty',
                                                           'val_name': 'empty',
                                                           'val_confidence': 'empty',
                                                           'val_type': 'empty', 'val_provenance': 'empty'
                                                           })
                                        if not task2:
                                            ev_arg_row.update({'val_prov_childID': 'empty',
                                                               'val_prov_mediaType': 'empty'})
                                        ev_arg_df = ev_arg_df.append(ev_arg_row, ignore_index=True)

        # save ev_arg_df to csv files
        if len(ev_arg_df) > 0:
            if not task2:
                ev_arg_df.insert(13, 'ev_prov_parentID', None)
                ev_arg_df.insert(14, 'ev_prov_language', None)
                ev_arg_df.insert(len(ev_arg_df.columns), 'val_prov_parentID', None)
                ev_arg_df.insert(len(ev_arg_df.columns), 'val_prov_language', None)
                for i, row in ev_arg_df.iterrows():
                    # add event prov parentID and prov language
                    ev_prov_childID = row.ev_prov_childID
                    # handle IBM format issue, e.g., K0C047UHI.rsd.txt
                    if isinstance(ev_prov_childID, str) and '.' in ev_prov_childID:
                        ev_prov_childID = ev_prov_childID.split('.')[0]
                    ev_pc_mapping_df = parent_children_mapping_df.loc[
                        parent_children_mapping_df['child_uid'] == ev_prov_childID]
                    if len(ev_pc_mapping_df) > 0:
                        ev_prov_parentID = ev_pc_mapping_df.iloc[0]['parent_uid']
                        ev_arg_df.at[i, 'ev_prov_parentID'] = ev_prov_parentID
                        ev_lang_mapping_df = language_mapping_df.loc[
                            language_mapping_df['parent_uid'] == ev_prov_parentID]
                        if len(ev_lang_mapping_df) > 0:
                            ev_prov_language = ev_lang_mapping_df.iloc[0]['language']
                            ev_arg_df.at[i, 'ev_prov_language'] = ev_prov_language
                    # add value prov parentID and prov language
                    val_prov_childID = row.val_prov_childID
                    # handle IBM format issue, e.g., K0C047UHI.rsd.txt
                    if isinstance(val_prov_childID, str) and '.' in val_prov_childID:
                        val_prov_childID = val_prov_childID.split('.')[0]
                    val_pc_mapping_df = parent_children_mapping_df.loc[
                        parent_children_mapping_df['child_uid'] == val_prov_childID]
                    if len(val_pc_mapping_df) > 0:
                        val_prov_parentID = val_pc_mapping_df.iloc[0]['parent_uid']
                        ev_arg_df.at[i, 'val_prov_parentID'] = val_prov_parentID
                        val_lang_mapping_df = language_mapping_df.loc[
                            language_mapping_df['parent_uid'] == val_prov_parentID]
                        if len(val_lang_mapping_df) > 0:
                            val_prov_language = val_lang_mapping_df.iloc[0]['language']
                            ev_arg_df.at[i, 'val_prov_language'] = val_prov_language

            # fix the surrogates error existed in ta2 outputs
            ev_arg_df = ev_arg_df.applymap(lambda x: str(x).encode("utf-8", errors="ignore").
                                           decode("utf-8", errors="ignore"))
            ev_arg_df.to_csv(ev_arg_csv_output_directory + file_name[:-5] + '_ev_arg.csv', index=False)


def extract_graph_g_ev_arg_from_json(graph_g_directory: str):
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

        ev_df = pd.DataFrame(columns=[
            'eventprimitive_id', 'description', 'type', 'subtype', 'subsubtype',
            'attribute'
        ])
        arg_df = pd.DataFrame(columns=[
            'eventprimitive_id', 'arg_id', 'entity_id',
            'slot_type', 'type', 'subtype', 'subsubtype'
        ])

        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                if 'steps' in schema.keys():
                    step_list = schema['steps']
                    if step_list:
                        for step in step_list:
                            # initialize ev elements
                            ev_id = ev_description = ev_type = ev_subtype = ev_subsubtype = None
                            ev_attribute = 'EMPTY_OPT'
                            # get event elements
                            if '@id' in step.keys():
                                ev_id = str(step['@id'])
                            if 'name' in step.keys():
                                ev_description = str(step['name'])
                            if '@type' in step.keys():
                                full_type = str(step['@type'])
                                slash_list = full_type.split('/')
                                types = slash_list[len(slash_list) - 1]
                                type_list = types.split('.')
                                ev_type = type_list[0].lower()
                                ev_subtype = type_list[1].lower()
                                ev_subsubtype = type_list[2].lower()
                            if 'modality' in step.keys():
                                ev_attribute = str(step['modality'])

                            ev_row = {'eventprimitive_id': ev_id, 'description': ev_description,
                                      'type': ev_type, 'subtype': ev_subtype, 'subsubtype': ev_subsubtype,
                                      'attribute': ev_attribute}

                            ev_df = ev_df.append(ev_row, ignore_index=True)

                            if 'participants' in step.keys():
                                participant_list = step['participants']
                                if participant_list:
                                    for participant in participant_list:
                                        if 'values' not in participant.keys():
                                            continue
                                        # initiate argument elements
                                        arg_id = entity_id = slot_type = arg_type = arg_subtype = arg_subsubtype = None
                                        # get argument elements
                                        if 'name' in participant.keys():
                                            slot_type = 'xxxxxxxxxxx' + str(participant['name']).lower()
                                        if 'entityTypes' in participant.keys():
                                            slash_list = str(participant['entityTypes']).split('/')
                                            arg_type = slash_list[len(slash_list) - 1].lower()
                                            if '.' in arg_type:
                                                dot_list = arg_type.split('.')
                                                arg_type = dot_list[0]
                                                arg_subtype = dot_list[1]
                                                arg_subsubtype = dot_list[2]
                                        value_list = participant['values']
                                        # only add values if value list is not empty
                                        for value in value_list:
                                            if 'provenance' in value.keys():
                                                arg_id = str(value['provenance'])
                                            if 'entity' in value.keys():
                                                entity_id = str(value['entity'])
                                            # add a row to arg_df
                                            arg_row = {
                                                'eventprimitive_id': ev_id, 'arg_id': arg_id, 'entity_id': entity_id,
                                                'slot_type': slot_type, 'type': arg_type, 'subtype': arg_subtype,
                                                'subsubtype': arg_subsubtype
                                            }
                                            arg_df = arg_df.append(arg_row, ignore_index=True)

        # save ev_arg_df to csv files
        if len(ev_df) > 0:
            ev_df.to_csv(graph_g_csv_output_directory + file_name[:-5] + '_ev.csv', index=False)
        if len(arg_df) > 0:
            arg_df.to_csv(graph_g_csv_output_directory + file_name[:-5] + '_arg.csv', index=False)


def extract_ta1_ev_arg_from_json(ta1_team_name: str, ta1_output_directory: str, score_directory: str):
    json_dict = load.load_json_directory(ta1_team_name, ta1_output_directory)

    ta1_library_directory = score_directory + 'TA1_library/'
    # if the ta1 library folder does not exist, create the folder
    if not os.path.isdir(ta1_library_directory):
        os.makedirs(ta1_library_directory)

    ta1_csv_output_directory = ta1_library_directory + ta1_team_name + '/'
    # if the ta1 csv output folder does not exist, create the folder
    if not os.path.isdir(ta1_csv_output_directory):
        os.makedirs(ta1_csv_output_directory)

    ev_arg_csv_output_directory = ta1_csv_output_directory + 'Event_arguments/'
    # if the ev_arg_csv_output_directory does not exist, create the folder
    if not os.path.isdir(ev_arg_csv_output_directory):
        os.makedirs(ev_arg_csv_output_directory)
    # if the folder exists, clear the contents
    else:
        files = glob.glob(ev_arg_csv_output_directory + '*')
        for file in files:
            os.remove(file)

    for file_name in json_dict:
        ev_arg_df = init_ta1_ev_arg_dataframe()
        json_data = json_dict[file_name]
        schema_list = json_data['schemas']
        if schema_list:
            for schema in tqdm(schema_list, position=0):
                # initiate schema elements
                schema_id = schema_super = schema_version = None
                # get schema elements
                if '@id' in schema.keys():
                    schema_id = schema['@id']
                if 'super' in schema.keys():
                    schema_super = schema['super']
                if 'version' in schema.keys():
                    schema_version = schema['version']
                if 'steps' in schema.keys():
                    step_list = schema['steps']
                    if step_list:
                        for step in step_list:
                            # initiate event elements
                            ev_id = ev_name = ev_type = ev_confidence = ev_achieves = ev_requires = None
                            ev_modality = ev_aka = ev_reference = ev_comment = None
                            # get event elements
                            if '@id' in step.keys():
                                ev_id = str(step['@id'])
                            if 'name' in step.keys():
                                ev_name = str(step['name'])
                            if '@type' in step.keys():
                                ev_type = str(step['@type'])
                            if 'confidence' in step.keys():
                                ev_confidence = str(step['confidence'])
                            if 'achieves' in step.keys():
                                ev_achieves = str(step['achieves'])
                            if 'requires' in step.keys():
                                ev_requires = str(step['requires'])
                            if 'modality' in step.keys():
                                ev_modality = str(step['modality'])
                            if 'aka' in step.keys():
                                ev_aka = str(step['aka'])
                            if 'reference' in step.keys():
                                ev_reference = str(step['reference'])
                            if 'comment' in step.keys():
                                ev_comment = str(step['comment'])
                            if 'participants' in step.keys():
                                participant_list = step['participants']
                                if participant_list:
                                    for participant in participant_list:
                                        # initiate argument elements
                                        arg_id = arg_name = arg_role = arg_refvar = arg_type = None
                                        arg_reference = arg_aka = arg_comment = None
                                        # get argument elements
                                        if '@id' in participant.keys():
                                            arg_id = str(participant['@id'])
                                        if 'name' in participant.keys():
                                            arg_name = str(participant['name'])
                                        if 'role' in participant.keys():
                                            arg_role = str(participant['role'])
                                        if 'refvar' in participant.keys():
                                            arg_refvar = str(participant['refvar'])
                                        if 'entityTypes' in participant.keys():
                                            arg_type = participant['entityTypes']
                                        if 'reference' in participant.keys():
                                            arg_reference = str(participant['reference'])
                                        if 'aka' in participant.keys():
                                            arg_aka = str(participant['aka'])
                                        if 'comment' in participant.keys():
                                            arg_comment = str(participant['comment'])
                                        # add a row to ev_arg_df
                                        ev_arg_row = {'file_name': file_name, 'schema_id': schema_id,
                                                      'schema_super': schema_super,
                                                      'schema_version': schema_version,
                                                      'ev_id': ev_id, 'ev_name': ev_name, 'ev_type': ev_type,
                                                      'ev_confidence': ev_confidence, 'ev_achieves': ev_achieves,
                                                      'ev_requires': ev_requires, 'ev_modality': ev_modality,
                                                      'ev_aka': ev_aka, 'ev_reference': ev_reference,
                                                      'ev_comment': ev_comment,
                                                      'arg_id': arg_id, 'arg_name': arg_name, 'arg_role': arg_role,
                                                      'arg_refvar': arg_refvar, 'arg_type': arg_type,
                                                      'arg_reference': arg_reference, 'arg_aka': arg_aka,
                                                      'arg_comment': arg_comment}
                                        ev_arg_df = ev_arg_df.append(ev_arg_row, ignore_index=True)

        # save ev_arg_df to csv files
        if len(ev_arg_df) > 0:
            ev_arg_df.to_csv(ev_arg_csv_output_directory + file_name[:-5] + '_ev_arg.csv', index=False)


if __name__ == "__main__":
    ta2_team_name = 'CMU'
    ta2_output_directory = '../../../../../Quizlet_4/TA2_outputs/'
    ta1_team_name = 'CMU'
    ta1_output_directory = '../../../../../Quizlet_4/TA1_outputs/'
    score_directory = '../../../../../Quizlet_4/Score/'
    extract_ta1_ev_arg_from_json(ta1_team_name, ta1_output_directory, score_directory)
