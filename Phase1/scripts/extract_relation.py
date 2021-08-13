#!/usr/bin/env python3

__author__ = "Xiongnan Jin (xiongnan.jin@nist.gov)"
__version__ = "Development: 0.5.1"
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
# load data from json file, extract entity relation related information
# and save into a pandas dataframe
######################################################################################
import json

import pandas as pd
import os
import sys
import glob

from tqdm import tqdm

scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../scripts")
sys.path.append(scripts_path)

from scripts import load_data as load
from scripts import extract_event_argument


def init_ta2_relation_dataframe(task2: bool) -> pd.DataFrame:
    if task2:
        rel_df = pd.DataFrame(columns=['file_name', 'task2', 'schema_id', 'schema_super', 'rel_subject',
                                       'rel_comment', 'rel_subject_provenance', 'rel_id', 'rel_name', 'rel_predicate',
                                       'rel_object', 'rel_ta1ref', 'rel_confidence', 'rel_object_provenance',
                                       'rel_provenance'])
    else:
        rel_df = pd.DataFrame(columns=['file_name', 'task2', 'schema_id', 'schema_super', 'rel_subject',
                                       'rel_comment', 'rel_subject_provenance', 'rel_id', 'rel_name', 'rel_predicate',
                                       'rel_object', 'rel_ta1ref', 'rel_confidence', 'rel_object_provenance',
                                       'rel_provenance', 'rel_prov_childID', 'rel_prov_mediaType'])

    return rel_df


def init_ta2_relation_entity_dataframe() -> pd.DataFrame:
    rel_df = pd.DataFrame(columns=['file_name', 'task2', 'schema_id', 'schema_super', 'ent_id',
                                   'ent_provenance', 'ent_prov_childID', 'ent_prov_mediaType',
                                   'rel_comment', 'rel_id', 'rel_name', 'rel_predicate',
                                   'rel_ta1ref', 'rel_confidence', 'rel_provenance'])

    return rel_df


def init_ta1_relation_dataframe() -> pd.DataFrame:
    rel_df = pd.DataFrame(columns=['file_name', 'schema_id', 'schema_super', 'rel_subject',
                                   'rel_comment', 'rel_id', 'rel_name', 'rel_predicate',
                                   'rel_object', 'rel_confidence', 'rel_object_provenance'])

    return rel_df


def extract_ta2_relation_from_json(ta2_team_name: str, ta2_output_directory: str,
                                   score_directory: str, target_pairs: list, annotation_directory: str) -> None:
    json_dict = load.load_json_directory(ta2_team_name, ta2_output_directory)

    ta2_csv_output_directory = score_directory + ta2_team_name
    # if the ta2 score folder does not exist, create the folder
    if not os.path.isdir(ta2_csv_output_directory):
        os.makedirs(ta2_csv_output_directory)

    rel_csv_output_directory = score_directory + ta2_team_name + '/Relations/'
    # if the rel_csv_output_directory does not exist, create the folder
    if not os.path.isdir(rel_csv_output_directory):
        os.makedirs(rel_csv_output_directory)
    # if the folder exists, clear the contents
    else:
        files = glob.glob(rel_csv_output_directory + '*')
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
        rel_df = init_ta2_relation_dataframe(task2)
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                provenance_data = {}
                if not task2:
                    provenance_data = extract_event_argument.get_provenance_data(schema['provenanceData'])
                schema_id = schema_super = None
                if '@id' in schema.keys():
                    schema_id = schema['@id']
                if 'super' in schema.keys():
                    schema_super = schema['super']
                if 'entityRelations' in schema.keys():
                    rel_list = schema['entityRelations']
                    if rel_list:
                        for rel in rel_list:
                            # initiate variables of entity relations
                            rel_subject = rel_comment = rel_subject_provenance = rel_id = None
                            rel_name = rel_predicate = rel_object = rel_ta1ref = None
                            rel_confidence = rel_object_provenance = rel_provenance = None
                            if not task2:
                                rel_prov_childID = rel_prov_mediaType = None
                            # get entity relation info
                            if 'relationSubject' in rel.keys():
                                rel_subject = str(rel['relationSubject'])
                            if 'comment' in rel.keys():
                                rel_comment = rel['comment']
                            # subject provenance
                            if 'provenance' in rel.keys():
                                rel_subject_provenance = rel['provenance']
                            if 'relations' in rel.keys():
                                relation_list = rel['relations']
                                if isinstance(relation_list, list):
                                    if relation_list:
                                        for relation in relation_list:
                                            if '@id' in relation.keys():
                                                rel_id = relation['@id']
                                            if 'name' in relation.keys():
                                                rel_name = relation['name']
                                            if 'relationPredicate' in relation.keys():
                                                rel_predicate = relation['relationPredicate']
                                            if 'relationObject' in relation.keys():
                                                rel_object = relation['relationObject']
                                            if 'ta1ref' in relation.keys():
                                                rel_ta1ref = relation['ta1ref']
                                            if 'confidence' in relation.keys():
                                                rel_confidence = relation['confidence']
                                            # object provenance
                                            if 'provenance' in relation.keys():
                                                rel_object_provenance = relation['provenance']
                                            # relation provenance
                                            if 'relationProvenance' in relation.keys():
                                                rel_provenance = relation['relationProvenance']
                                                first_rel_provenance = \
                                                    extract_event_argument.get_first_provenance(rel_provenance)
                                                if not task2 and first_rel_provenance in provenance_data.keys():
                                                    rel_prov_childID = provenance_data[first_rel_provenance]['childID']
                                                    rel_prov_mediaType = \
                                                        provenance_data[first_rel_provenance]['mediaType']
                                            rel_row = {'file_name': file_name, 'task2': task2,
                                                       'schema_id': schema_id,
                                                       'schema_super': schema_super,
                                                       'rel_subject': rel_subject,
                                                       'rel_comment': rel_comment,
                                                       'rel_subject_provenance': rel_subject_provenance,
                                                       'rel_id': rel_id, 'rel_name': rel_name,
                                                       'rel_predicate': rel_predicate,
                                                       'rel_object': rel_object,
                                                       'rel_ta1ref': rel_ta1ref, 'rel_confidence': rel_confidence,
                                                       'rel_object_provenance': rel_object_provenance,
                                                       'rel_provenance': rel_provenance}
                                            if not task2:
                                                rel_row['rel_prov_childID'] = rel_prov_childID
                                                rel_row['rel_prov_mediaType'] = rel_prov_mediaType
                                            rel_df = rel_df.append(rel_row, ignore_index=True)
                                else:
                                    relation = relation_list
                                    if '@id' in relation.keys():
                                        rel_id = relation['@id']
                                    if 'name' in relation.keys():
                                        rel_name = relation['name']
                                    if 'relationPredicate' in relation.keys():
                                        rel_predicate = relation['relationPredicate']
                                    if 'relationObject' in relation.keys():
                                        rel_object = relation['relationObject']
                                    if 'ta1ref' in relation.keys():
                                        rel_ta1ref = relation['ta1ref']
                                    if 'confidence' in relation.keys():
                                        rel_confidence = relation['confidence']
                                    # object provenance
                                    if 'provenance' in relation.keys():
                                        rel_object_provenance = relation['provenance']
                                    # relation provenance
                                    if 'relationProvenance' in relation.keys():
                                        rel_provenance = relation['relationProvenance']
                                        first_rel_provenance = \
                                            extract_event_argument.get_first_provenance(rel_provenance)
                                        if not task2 and first_rel_provenance in provenance_data.keys():
                                            rel_prov_childID = provenance_data[first_rel_provenance]['childID']
                                            rel_prov_mediaType = \
                                                provenance_data[first_rel_provenance]['mediaType']
                                    rel_row = {'file_name': file_name, 'task2': task2,
                                               'schema_id': schema_id,
                                               'schema_super': schema_super,
                                               'rel_subject': rel_subject,
                                               'rel_comment': rel_comment,
                                               'rel_subject_provenance': rel_subject_provenance,
                                               'rel_id': rel_id, 'rel_name': rel_name,
                                               'rel_predicate': rel_predicate,
                                               'rel_object': rel_object,
                                               'rel_ta1ref': rel_ta1ref, 'rel_confidence': rel_confidence,
                                               'rel_object_provenance': rel_object_provenance,
                                               'rel_provenance': rel_provenance}
                                    if not task2:
                                        rel_row['rel_prov_childID'] = rel_prov_childID
                                        rel_row['rel_prov_mediaType'] = rel_prov_mediaType
                                    rel_df = rel_df.append(rel_row, ignore_index=True)
        # save non-empty rel_df to csv files
        if len(rel_df) > 0:
            if not task2:
                rel_df.insert(len(rel_df.columns), 'rel_prov_parentID', None)
                rel_df.insert(len(rel_df.columns), 'rel_prov_language', None)
                for i, row in rel_df.iterrows():
                    # add relation prov parentID and prov language
                    rel_prov_childID = row.rel_prov_childID
                    # handle IBM format issue, e.g., K0C047UHI.rsd.txt
                    if isinstance(rel_prov_childID, str) and '.' in rel_prov_childID:
                        rel_prov_childID = rel_prov_childID.split('.')[0]
                    rel_pc_mapping_df = parent_children_mapping_df.loc[
                        parent_children_mapping_df['child_uid'] == rel_prov_childID]
                    if len(rel_pc_mapping_df) > 0:
                        rel_prov_parentID = rel_pc_mapping_df.iloc[0]['parent_uid']
                        rel_df.at[i, 'rel_prov_parentID'] = rel_prov_parentID
                        rel_lang_mapping_df = language_mapping_df.loc[
                            language_mapping_df['parent_uid'] == rel_prov_parentID]
                        if len(rel_lang_mapping_df) > 0:
                            rel_prov_language = rel_lang_mapping_df.iloc[0]['language']
                            rel_df.at[i, 'rel_prov_language'] = rel_prov_language
            # fix the surrogates error existed in ta2 outputs
            rel_df = rel_df.applymap(lambda x: str(x).encode("utf-8", errors="ignore").
                                     decode("utf-8", errors="ignore"))
            rel_df.to_csv(rel_csv_output_directory + file_name[:-5] + '_rel.csv', index=False)


def extract_ta2_relation_entity_from_json(ta2_team_name: str, ta2_output_directory: str,
                                          score_directory: str, target_pairs: list, annotation_directory: str) -> None:
    json_dict = load.load_json_directory(ta2_team_name, ta2_output_directory)

    ta2_csv_output_directory = score_directory + ta2_team_name
    # if the ta2 score folder does not exist, create the folder
    if not os.path.isdir(ta2_csv_output_directory):
        os.makedirs(ta2_csv_output_directory)

    rel_csv_output_directory = score_directory + ta2_team_name + '/Relation_entities/'
    # if the rel_csv_output_directory does not exist, create the folder
    if not os.path.isdir(rel_csv_output_directory):
        os.makedirs(rel_csv_output_directory)
    # if the folder exists, clear the contents
    else:
        files = glob.glob(rel_csv_output_directory + '*')
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

        rel_ent_df = init_ta2_relation_entity_dataframe()

        json_data = json_dict[file_name]
        task2 = False
        if 'task2' in json_data.keys():
            task2 = json_data['task2']
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                provenance_data = {}
                if not task2:
                    provenance_data = extract_event_argument.get_provenance_data(schema['provenanceData'])
                schema_id = schema_super = None
                if '@id' in schema.keys():
                    schema_id = schema['@id']
                if 'super' in schema.keys():
                    schema_super = schema['super']
                if 'entityRelations' in schema.keys():
                    rel_list = schema['entityRelations']
                    if rel_list:
                        for rel in rel_list:
                            # initiate variables of entity relations
                            rel_subject = rel_comment = rel_subject_provenance = rel_id = None
                            rel_name = rel_predicate = rel_object = rel_ta1ref = None
                            rel_confidence = rel_object_provenance = rel_provenance = None
                            if not task2:
                                rel_subject_prov_childID = rel_subject_prov_mediaType = None
                                rel_object_prov_childID = rel_object_prov_mediaType = None
                            # get entity relation info
                            if 'relationSubject' in rel.keys():
                                rel_subject = str(rel['relationSubject'])
                            if 'comment' in rel.keys():
                                rel_comment = rel['comment']
                            # subject provenance
                            if 'provenance' in rel.keys():
                                rel_subject_provenance = rel['provenance']
                                first_rel_subject_prov = \
                                    extract_event_argument.get_first_provenance(rel_subject_provenance)
                                if not task2 and first_rel_subject_prov in provenance_data.keys():
                                    rel_subject_prov_childID = provenance_data[first_rel_subject_prov]['childID']
                                    rel_subject_prov_mediaType = \
                                        provenance_data[first_rel_subject_prov]['mediaType']
                            if 'relations' in rel.keys():
                                relation_list = rel['relations']
                                if isinstance(relation_list, list):
                                    if relation_list:
                                        for relation in relation_list:
                                            if '@id' in relation.keys():
                                                rel_id = relation['@id']
                                            if 'name' in relation.keys():
                                                rel_name = relation['name']
                                            if 'relationPredicate' in relation.keys():
                                                rel_predicate = relation['relationPredicate']
                                            if 'relationObject' in relation.keys():
                                                rel_object = relation['relationObject']
                                            if 'ta1ref' in relation.keys():
                                                rel_ta1ref = relation['ta1ref']
                                            if 'confidence' in relation.keys():
                                                rel_confidence = relation['confidence']
                                            # object provenance
                                            if 'provenance' in relation.keys():
                                                rel_object_provenance = relation['provenance']
                                                first_rel_object_prov = \
                                                    extract_event_argument.get_first_provenance(rel_object_provenance)
                                                if not task2 and first_rel_object_prov in provenance_data.keys():
                                                    rel_object_prov_childID = provenance_data[first_rel_object_prov][
                                                        'childID']
                                                    rel_object_prov_mediaType = \
                                                        provenance_data[first_rel_object_prov]['mediaType']
                                            # relation provenance
                                            if 'relationProvenance' in relation.keys():
                                                rel_provenance = relation['relationProvenance']
                                            # relation subject row
                                            rel_ent_row1 = {'file_name': file_name, 'task2': task2,
                                                            'schema_id': schema_id,
                                                            'schema_super': schema_super,
                                                            'ent_id': rel_subject,
                                                            'ent_provenance': rel_subject_provenance}
                                            if not task2:
                                                rel_ent_row1.update({'ent_prov_childID': rel_subject_prov_childID,
                                                                     'ent_prov_mediaType': rel_subject_prov_mediaType})
                                            rel_ent_row1.update({'rel_comment': rel_comment,
                                                                 'rel_id': rel_id, 'rel_name': rel_name,
                                                                 'rel_predicate': rel_predicate,
                                                                 'rel_ta1ref': rel_ta1ref,
                                                                 'rel_confidence': rel_confidence,
                                                                 'rel_provenance': rel_provenance})
                                            # relation object row
                                            rel_ent_row2 = {'file_name': file_name, 'task2': task2,
                                                            'schema_id': schema_id,
                                                            'schema_super': schema_super,
                                                            'ent_id': rel_object,
                                                            'ent_provenance': rel_object_provenance}
                                            if not task2:
                                                rel_ent_row2.update({'ent_prov_childID': rel_object_prov_childID,
                                                                     'ent_prov_mediaType': rel_object_prov_mediaType})
                                            rel_ent_row2.update({'rel_comment': rel_comment,
                                                                 'rel_id': rel_id, 'rel_name': rel_name,
                                                                 'rel_predicate': rel_predicate,
                                                                 'rel_ta1ref': rel_ta1ref,
                                                                 'rel_confidence': rel_confidence,
                                                                 'rel_provenance': rel_provenance})
                                            rel_ent_df = rel_ent_df.append(rel_ent_row1, ignore_index=True)
                                            rel_ent_df = rel_ent_df.append(rel_ent_row2, ignore_index=True)
                                else:
                                    relation = relation_list
                                    if '@id' in relation.keys():
                                        rel_id = relation['@id']
                                    if 'name' in relation.keys():
                                        rel_name = relation['name']
                                    if 'relationPredicate' in relation.keys():
                                        rel_predicate = relation['relationPredicate']
                                    if 'relationObject' in relation.keys():
                                        rel_object = relation['relationObject']
                                    if 'ta1ref' in relation.keys():
                                        rel_ta1ref = relation['ta1ref']
                                    if 'confidence' in relation.keys():
                                        rel_confidence = relation['confidence']
                                    # object provenance
                                    if 'provenance' in relation.keys():
                                        rel_object_provenance = relation['provenance']
                                        first_rel_object_prov = \
                                            extract_event_argument.get_first_provenance(rel_object_provenance)
                                        if not task2 and first_rel_object_prov in provenance_data.keys():
                                            rel_object_prov_childID = provenance_data[first_rel_object_prov][
                                                'childID']
                                            rel_object_prov_mediaType = \
                                                provenance_data[first_rel_object_prov]['mediaType']
                                    # relation provenance
                                    if 'relationProvenance' in relation.keys():
                                        rel_provenance = relation['relationProvenance']
                                    # relation subject row
                                    rel_ent_row1 = {'file_name': file_name, 'task2': task2,
                                                    'schema_id': schema_id,
                                                    'schema_super': schema_super,
                                                    'ent_id': rel_subject,
                                                    'ent_provenance': rel_subject_provenance}
                                    if not task2:
                                        rel_ent_row1.update({'ent_prov_childID': rel_subject_prov_childID,
                                                             'ent_prov_mediaType': rel_subject_prov_mediaType})
                                    rel_ent_row1.update({'rel_comment': rel_comment,
                                                         'rel_id': rel_id, 'rel_name': rel_name,
                                                         'rel_predicate': rel_predicate,
                                                         'rel_ta1ref': rel_ta1ref,
                                                         'rel_confidence': rel_confidence,
                                                         'rel_provenance': rel_provenance})
                                    # relation object row
                                    rel_ent_row2 = {'file_name': file_name, 'task2': task2,
                                                    'schema_id': schema_id,
                                                    'schema_super': schema_super,
                                                    'ent_id': rel_object,
                                                    'ent_provenance': rel_object_provenance}
                                    if not task2:
                                        rel_ent_row2.update({'ent_prov_childID': rel_object_prov_childID,
                                                             'ent_prov_mediaType': rel_object_prov_mediaType})
                                    rel_ent_row2.update({'rel_comment': rel_comment,
                                                         'rel_id': rel_id, 'rel_name': rel_name,
                                                         'rel_predicate': rel_predicate,
                                                         'rel_ta1ref': rel_ta1ref,
                                                         'rel_confidence': rel_confidence,
                                                         'rel_provenance': rel_provenance})
                                    rel_ent_df = rel_ent_df.append(rel_ent_row1, ignore_index=True)
                                    rel_ent_df = rel_ent_df.append(rel_ent_row2, ignore_index=True)
        # save non-empty rel_ent_df to csv files
        if len(rel_ent_df) > 0:
            if not task2:
                rel_ent_df.insert(8, 'ent_prov_parentID', None)
                rel_ent_df.insert(9, 'ent_prov_language', None)
                for i, row in rel_ent_df.iterrows():
                    # add entity prov parentID and prov language
                    ent_prov_childID = row.ent_prov_childID
                    # handle IBM format issue, e.g., K0C047UHI.rsd.txt
                    if isinstance(ent_prov_childID, str) and '.' in ent_prov_childID:
                        ent_prov_childID = ent_prov_childID.split('.')[0]
                    ent_pc_mapping_df = parent_children_mapping_df.loc[
                        parent_children_mapping_df['child_uid'] == ent_prov_childID]
                    if len(ent_pc_mapping_df) > 0:
                        ent_prov_parentID = ent_pc_mapping_df.iloc[0]['parent_uid']
                        rel_ent_df.at[i, 'ent_prov_parentID'] = ent_prov_parentID
                        ent_lang_mapping_df = language_mapping_df.loc[
                            language_mapping_df['parent_uid'] == ent_prov_parentID]
                        if len(ent_lang_mapping_df) > 0:
                            ent_prov_language = ent_lang_mapping_df.iloc[0]['language']
                            rel_ent_df.at[i, 'ent_prov_language'] = ent_prov_language
            # fix the surrogates error existed in ta2 outputs
            rel_ent_df = rel_ent_df.applymap(lambda x: str(x).encode("utf-8", errors="ignore").
                                             decode("utf-8", errors="ignore"))
            rel_ent_df.to_csv(rel_csv_output_directory + file_name[:-5] + '_rel.csv', index=False)


def extract_graph_g_relation_from_json(graph_g_directory: str):
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

        rel_df = pd.DataFrame(columns=[
            'file_name', 'rel_subject', 'rel_subject_provenance', 'rel_id', 'rel_ta1ref', 'rel_name',
            'rel_predicate', 'rel_object', 'rel_object_provenance', 'rel_provenance'
        ])
        task_2 = False
        if 'task2' in json_data.keys():
            task_2 = json_data['task2']
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                if 'entityRelations' in schema.keys():
                    rel_list = schema['entityRelations']
                    if rel_list:
                        for rel in rel_list:
                            # initiate variables of entity relations
                            rel_subject = rel_subject_provenance = None
                            # get relation info
                            if 'relationSubject' in rel.keys():
                                rel_subject = rel['relationSubject']
                            if 'provenance' in rel.keys():
                                rel_subject_provenance = rel['provenance']
                            if 'relations' in rel.keys():
                                relation_list = rel['relations']
                                if relation_list:
                                    for relation in relation_list:
                                        rel_id = rel_ta1ref = rel_name = rel_predicate = rel_object = None
                                        rel_object_provenance = rel_provenance = None
                                        if '@id' in relation.keys():
                                            rel_id = relation['@id']
                                        if 'ta1ref' in relation.keys():
                                            rel_ta1ref = relation['ta1ref']
                                        if 'name' in relation.keys():
                                            rel_name = relation['name']
                                        if 'relationPredicate' in relation.keys():
                                            rel_predicate = relation['relationPredicate']
                                        if 'relationObject' in relation.keys():
                                            rel_object = relation['relationObject']
                                        if 'provenance' in relation.keys():
                                            rel_object_provenance = relation['provenance']
                                        # relation provenance
                                        if 'relationProvenance' in relation.keys():
                                            rel_provenance = relation['relationProvenance']
                                        rel_row = {'file_name': file_name, 'rel_subject': rel_subject,
                                                   'rel_subject_provenance': rel_subject_provenance,
                                                   'rel_id': rel_id, 'rel_ta1ref': rel_ta1ref,
                                                   'rel_name': rel_name, 'rel_predicate': rel_predicate,
                                                   'rel_object': rel_object,
                                                   'rel_object_provenance': rel_object_provenance,
                                                   'rel_provenance': rel_provenance}
                                        rel_df = rel_df.append(rel_row, ignore_index=True)
        # save non-empty ent_rel_df to csv files
        if len(rel_df) > 0:
            rel_df.to_csv(graph_g_csv_output_directory + file_name[:-5] + '_rel.csv', index=False)


def extract_ta1_relation_from_json(ta1_team_name: str, ta1_output_directory: str, score_directory: str) -> None:
    json_dict = load.load_json_directory(ta1_team_name, ta1_output_directory)

    ta1_library_directory = score_directory + 'TA1_library/'
    # if the ta1 library folder does not exist, create the folder
    if not os.path.isdir(ta1_library_directory):
        os.makedirs(ta1_library_directory)

    ta1_csv_output_directory = ta1_library_directory + ta1_team_name
    # if the ta1 csv output folder does not exist, create the folder
    if not os.path.isdir(ta1_csv_output_directory):
        os.makedirs(ta1_csv_output_directory)

    rel_csv_output_directory = ta1_csv_output_directory + '/Relations/'
    # if the rel_csv_output_directory does not exist, create the folder
    if not os.path.isdir(rel_csv_output_directory):
        os.makedirs(rel_csv_output_directory)
    # if the folder exists, clear the contents
    else:
        files = glob.glob(rel_csv_output_directory + '*')
        for file in files:
            os.remove(file)

    for file_name in tqdm(json_dict, position=0, leave=False):
        rel_df = init_ta1_relation_dataframe()
        json_data = json_dict[file_name]
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                schema_id = schema_super = None
                if '@id' in schema.keys():
                    schema_id = schema['@id']
                if 'super' in schema.keys():
                    schema_super = schema['super']
                if 'entityRelations' in schema.keys():
                    rel_list = schema['entityRelations']
                    if rel_list:
                        for rel in rel_list:
                            # initiate variables of entity relations
                            rel_subject = rel_comment = rel_id = None
                            rel_name = rel_predicate = rel_object = None
                            rel_confidence = rel_object_provenance = rel_comment = None
                            # get entity relation info
                            if 'relationSubject' in rel.keys():
                                rel_subject = str(rel['relationSubject'])
                            if 'comment' in rel.keys():
                                rel_comment = rel['comment']
                            if 'relations' in rel.keys():
                                relation_list = rel['relations']
                                if relation_list:
                                    for relation in relation_list:
                                        if '@id' in relation.keys():
                                            rel_id = relation['@id']
                                        if 'name' in relation.keys():
                                            rel_name = relation['name']
                                        if 'relationPredicate' in relation.keys():
                                            rel_predicate = relation['relationPredicate']
                                        if 'relationObject' in relation.keys():
                                            rel_object = relation['relationObject']
                                        if 'confidence' in relation.keys():
                                            rel_confidence = relation['confidence']
                                        # object provenance
                                        if 'provenance' in relation.keys():
                                            rel_object_provenance = relation['provenance']
                                        rel_row = {'file_name': file_name, 'schema_id': schema_id,
                                                   'schema_super': schema_super,
                                                   'rel_subject': rel_subject,
                                                   'rel_comment': rel_comment, 'rel_id': rel_id,
                                                   'rel_name': rel_name,
                                                   'rel_predicate': rel_predicate,
                                                   'rel_object': rel_object,
                                                   'rel_confidence': rel_confidence,
                                                   'rel_object_provenance': rel_object_provenance}
                                        rel_df = rel_df.append(rel_row, ignore_index=True)
        # save non-empty rel_df to csv files
        if len(rel_df) > 0:
            rel_df.to_csv(rel_csv_output_directory + file_name[:-5] + '_rel.csv', index=False)


if __name__ == "__main__":
    # ta2_team_name = 'RESIN'
    # ta2_output_directory = '../../../../../Quizlet_4/TA2_outputs/'
    score_directory = '../../../../../Quizlet_4/Score/'

    ta1_team_name = 'IBM'
    ta1_output_directory = '../../../../../Quizlet_4/TA1_outputs/'
    extract_ta1_relation_from_json(ta1_team_name, ta1_output_directory, score_directory)
