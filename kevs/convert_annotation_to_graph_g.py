import os
import sys
import configparser
import copy
import dateutil
import dateutil.parser

import pandas
import numpy as np
import pandas as pd
import json
import urllib.request


def get_qnode_label(qnode_name: str, qnode_df_fp: str) -> str:
    """
    Finds qlabel if in table. If not, queries api and writes it to table for a cache

    Args:
        qnode_name (str):
        qnode_df_fp (str):

    Returns:

    """
    qnode_df = pd.read_csv(qnode_df_fp)
    qlabel = ""
    write_wikidata = False
    qlabel_list = qnode_df.loc[qnode_df['qnode'] == qnode_name, 'qlabel'].tolist()
    if len(qlabel_list) >= 1:
        qlabel = str(qlabel_list[0])
    else:
        # Find in api
        api_url = "https://kgtk.isi.edu/api?q={}&language=en&extra_info=true".format(qnode_name)
        with urllib.request.urlopen(api_url) as url:
            qnode_data = json.loads(url.read().decode())
        qlabel = qnode_data[0]['label'][0]
        if qlabel != '':
            qnode_df = pd.concat([qnode_df,
                                  pd.DataFrame([{'qnode': qnode_name, 'qlabel': qlabel}])],
                                 ignore_index=True)
            write_wikidata = True

    if write_wikidata:
        qnode_df.to_csv(qnode_df_fp, index=False, header=True)
    return qlabel


def add_temporal_info(ep_dict: dict, start_datetime_type: str, start_datetime: str,
                      end_datetime_type: str, end_datetime: str) -> dict:

    # Handle common unknown cases
    start_early_datetime = start_datetime.replace("Txx:xx:xx", "T00:00:00")
    start_late_datetime = start_datetime.replace("Txx:xx:xx", "T23:59:59")
    end_early_datetime = end_datetime.replace("Txx:xx:xx", "T00:00:00")
    end_late_datetime = end_datetime.replace("Txx:xx:xx", "T23:59:59")

    # add start datetime
    if start_datetime_type == 'starton':
        ep_dict['temporal']['earliestStartTime'] = start_early_datetime
        ep_dict['temporal']['latestStartTime'] = start_late_datetime
    elif start_datetime_type == 'startbefore':
        ep_dict['temporal']['latestStartTime'] = start_late_datetime

    elif start_datetime_type == 'startafter':
        ep_dict['temporal']['earliestStartTime'] = start_early_datetime
    # add end datetime
    if end_datetime_type == 'endon':
        ep_dict['temporal']['earliestEndTime'] = end_early_datetime
        ep_dict['temporal']['latestEndTime'] = end_late_datetime
    elif end_datetime_type == 'endbefore':
        ep_dict['temporal']['latestEndTime'] = end_late_datetime
    elif end_datetime_type == 'endafter':
        ep_dict['temporal']['earliestEndTime'] = end_early_datetime

    return ep_dict


def add_relations():
    pass


def add_arguments(schema_dict: dict, ep_id: str, ep_sdf_id: str, ep_dict: dict, ep_id_set: set,
                  arg_df: pd.DataFrame, arg_ep_id_set: set,
                  qnode_df_fp: str,
                  entity_qnode_df: pd.DataFrame, ep_df: pd.DataFrame,
                  er_df: pd.DataFrame) -> dict:
    '''
    Adds arguments into graph G. This changes the "selected" row of "er_df" to new for any
    new identified relations
    Args:
        schema_dict:
        ep_id:
        ep_sdf_id:
        ep_dict:
        ep_id_set:
        arg_df:
        arg_ep_id_set:
        qnode_df_fp:
        entity_qnode_df:
        ep_df:
        er_df:

    Returns:

    '''
    arg_df_filtered = arg_df[arg_df['eventprimitive_id'] == ep_id]

    for i, arg_row in arg_df_filtered.iterrows():
        arg_description = str(arg_row.get('description'))
        val_id = arg_row.get('arg_id')
        arg_sdf_id = arg_row.get('sdf_id')
        entity_id = arg_row.get('entity_id')
        entity_sdf_id = ""
        if entity_id != "EMPTY_NA":
            entity_sdf_id = entity_qnode_df.loc[entity_qnode_df['entity_id'] == entity_id,
                                                'sdf_id'].tolist()[0]
        entity_id = entity_id
        modality = arg_row.get('attribute')
        gen_slot_type = arg_row.get('general_slot_type')

        # arg_role = ep_type + '/Slots/' + val_role
        # arg_id = ep_id + '/' + val_role
        arg_id = ep_id + '/' + val_id
        # Replace Entity ID with pointer to event
        if entity_id == "EMPTY_NA":
            entity_id = val_id
            # We might have a relation as an argument
            if "RR" in val_id:
                # Look in the entire er set to add the argument, and add the relation if selected
                # is not true
                entity_sdf_id = \
                    er_df.loc[er_df['relation_id'] == val_id, 'sdf_id'].tolist()[0]
                # Add to dictionary if not a selected relation
                if er_df.loc[er_df['relation_id'] == val_id, 'selected'].tolist()[0] == "no":
                    # We flag this as a new relation to add
                    er_df.loc[er_df['relation_id'] == val_id, 'selected'] = "new"
                    print("Relation {} Used as Argument ID {} in Event {}".
                          format(val_id, arg_row.get('arg_id'), ep_id))
            else:
                entity_sdf_id = \
                    ep_df.loc[ep_df['eventprimitive_id'] == val_id, 'sdf_id'].tolist()[0]
        participants_dict = {'@id': arg_sdf_id, 'roleName': gen_slot_type, 'entity': "kairos:NULL",
                             'values': {'ta2entity': entity_sdf_id, 'provenance': "LDC Annotation"}}
        # modality check: if modality != 'none':
        # modality check:   participants_dict['modality'] = modality
        arg_id = ep_id + '/' + val_id
        # if arg is an entity
        if val_id[:2] == 'AR':
            # participants_dict['entityTypes'] = arg_type
            qnode_name = entity_qnode_df.loc[entity_qnode_df['entity_id'] == entity_id,
                                             'qnode_kb_id_identity'].tolist()[0]

            if qnode_name == "EMPTY_NA" or qnode_name == "NIL":
                qnode_name = entity_qnode_df.loc[entity_qnode_df['entity_id'] == entity_id,
                                                 'qnode_kb_id_type'].tolist()[0]
            qlabel = ''
            if qnode_name != "EMPTY_TBD" and qnode_name != "EMPTY_NA" and qnode_name != "NIL":
                qlabel = get_qnode_label(qnode_name, qnode_df_fp)
            if '|' in qnode_name:
                qnode_name = ["wd:{}".format(str.strip()) for str in qnode_name.split('|')]
            else:
                qnode_name = "wd:{}".format(qnode_name)
            # Only add entity if not already in entity dictionary
            entity_dict = {'@id': entity_sdf_id, 'name': arg_description,
                           'ta2qnode': qnode_name,
                           'ta2qlabel': qlabel}
            add_entity(schema_dict, entity_dict)
        # if arg is an event

        val_dict = {'provenance': val_id}
        # in case the argument is an event
        if val_id[:2] == 'VP':
            val_dict['entity'] = val_id
            if val_id not in ep_id_set:
                if val_id not in arg_ep_id_set:
                    arg_ep_id_set.add(val_id)
        # in case the argument is an entity
        else:
            if entity_id != 'EMPTY_NA':
                val_dict['entity'] = entity_id
            else:
                val_dict['name'] = arg_description
                # val_dict['entityTypes'] = arg_type
        if modality != 'EMPTY_OPT' and modality != "none":
            if ',' in modality:
                modality = modality.split(',')
            val_dict['modality'] = modality
        participant_list = ep_dict['participants']
        existing_participant = False
        for participant in participant_list:
            if participant['@id'] == arg_id:
                existing_participant = True
                participants_dict = participant
        # participants_dict['values'].append(val_dict)
        if not existing_participant:
            ep_dict['participants'].append(participants_dict)

    return ep_dict


def add_ontology_participant(ep_dict: dict, ep_role_list: list, role: str, role_types: str) -> list:
    role_types = role_types.replace(' ', '')
    role_type_list = role_types.split(',')
    entityTypes = []
    for role_type in role_type_list:
        entityTypes.append('kairos:Primitives/Entities/' + role_type.upper())
    ep_role_list.append(role)

    return ep_role_list


def complete_ep_dict(ep_dict: dict):
    ep_role_list = []
    participant_list = ep_dict['participants']
    if participant_list:
        for participant_dict in participant_list:
            arg_role = participant_dict['roleName']
            slash_list = arg_role.split('/')
            arg_role_name = slash_list[len(slash_list) - 1]
            ep_role_list.append(arg_role_name)


def get_correct_temporal_element(input_element: str):
    if input_element == 'x':
        return 'xx'
    else:
        return '0' + input_element


def get_correct_datetime(input_datetime_str: str) -> str:
    """
    Produces datetime string in xsd:dateTime definition (CCYY-MM-DDThh:mm:ss.sss),
    which is currently an SDF requirement.
    For example "2018-07-26T00:00:00".'' is a valid time stame
    Args:
        input_datetime_str:

    Returns:

    """
    # Use datetime object to handle a variety of formats
    # Produce datetime in

    year = "xx"
    month = "xx"
    day = "xx"
    hour = "xx"
    minute = "xx"
    second = "xx"

    try:
        input_datetime = pd.to_datetime(input_datetime_str)
        year = str(input_datetime.year)
        month = str(input_datetime.month).rjust(2, "0")
        day = str(input_datetime.day).rjust(2, "0")
        hour = str(input_datetime.hour).rjust(2, "0")
        minute = str(input_datetime.minute).rjust(2, "0")
        second = str(input_datetime.second).rjust(2, "0")
    except dateutil.parser.ParserError:
        date_str = input_datetime_str.split('-')
        year = date_str[0]
        month = date_str[1]
        day = date_str[2].split('T')[0]

    correct_datetime = year + '-' + month + '-' + day + 'T' + hour + ':' + minute + ':' + second

    return correct_datetime


def add_ep(schema_dict: dict, ep_row: pd.Series, ep_id_set: set,
           arg_df: pandas.DataFrame, arg_ep_id_set: set,
           qnode_df_fp: str,
           entity_qnode_df: pandas.DataFrame, ep_df: pd.DataFrame, er_df: pd.DataFrame):
    ep_id = ep_row.get('eventprimitive_id')
    ep_sdf_id = ep_row.get('sdf_id')
    ep_name = str(ep_row.get('description'))
    qnode_name = ep_row.get('qnode_type_id')
    qlabel = ''
    if qnode_name != "EMPTY_TBD":
        qlabel = get_qnode_label(qnode_name, qnode_df_fp)
    if '|' in qnode_name:
        qnode_name = ["wd:{}".format(str.strip()) for str in qnode_name.split('|')]
    else:
        qnode_name = "wd:{}".format(qnode_name)
    ep_significance = ep_row.get('significance')
    ep_attribute = ep_row.get('attribute')
    if ep_attribute.__contains__(','):
        ep_attribute = ep_attribute.replace(' ', '')
        ep_dict = {'@id': ep_sdf_id, 'name': qlabel, 'description': ep_name,
                   'ta2qnode': qnode_name, 'ta2qlabel': qlabel,
                   'ta1ref': 'kairos:NULL', "ta1explanation": "From Graph G.",
                   'provenance': ep_id,
                   # 'significance': ep_significance,
                   'temporal': {}, 'participants': []}
        if ep_attribute != 'none':
            if ',' in ep_attribute:
                ep_attribute = ep_attribute.split(',')
            ep_dict['modality'] = ep_attribute
    else:
        if ep_attribute != 'EMPTY_OPT' and ep_attribute != "none":
            if ',' in ep_attribute:
                ep_attribute = ep_attribute.split(',')
            ep_dict = {'@id': ep_sdf_id, 'name': qlabel, 'description': ep_name,
                       'ta2qnode': qnode_name, 'ta2qlabel': qlabel,
                       'ta1ref': 'kairos:NULL', "ta1explanation": "From Graph G.",
                       'provenance': ep_id,
                       # 'significance': ep_significance,
                       'modality': ep_attribute,
                       'temporal': {}, 'participants': []}
        else:
            ep_dict = {'@id': ep_sdf_id, 'name': qlabel, 'description': ep_name,
                       'ta2qnode': qnode_name, 'ta2qlabel': qlabel,
                       'ta1ref': 'kairos:NULL', "ta1explanation": "From Graph G.",
                       'provenance': ep_id,
                       # 'significance': ep_significance,
                       'temporal': {}, 'participants': []}
    # add temporal info to ep
    start_datetime_type = ep_row.get('start_datetime_type')
    start_datetime = ep_row.get('start_datetime')
    if start_datetime != 'EMPTY_NA':
        start_datetime = get_correct_datetime(start_datetime)
    end_datetime_type = ep_row.get('end_datetime_type')
    end_datetime = ep_row.get('end_datetime')
    if end_datetime != 'EMPTY_NA':
        end_datetime = get_correct_datetime(end_datetime)
    ep_dict = add_temporal_info(ep_dict, start_datetime_type, start_datetime,
                                end_datetime_type, end_datetime)
    ep_dict = add_arguments(schema_dict, ep_id, ep_sdf_id, ep_dict, ep_id_set,
                            arg_df, arg_ep_id_set,
                            qnode_df_fp, entity_qnode_df, ep_df, er_df)
    complete_ep_dict(ep_dict)
    # add ep to schema
    schema_dict['events'].append(ep_dict)
    # Add event to children of events
    ep_significance_str = "false"
    if ep_significance == "critical":
        ep_significance_str = 'true'
    child_event = {'child': ep_sdf_id,
                   'optional': ep_significance_str, 'importance': 1}
    schema_dict['events'][0]['children'].append(child_event)
    ep_id_set.add(ep_id)


def add_entity(schema_dict: dict, entity_dict: dict):
    # annotation typo fix
    if entity_dict['name'] == 'Mr Justice Jeremy Baker':
        entity_dict['name'] = 'Mr. Justice Jeremy Baker'

    target_entity_id = entity_dict['@id']
    flag_exist = False
    for i in range(len(schema_dict['entities'])):
        cur_entity_id = schema_dict['entities'][i]['@id']
        if (cur_entity_id == target_entity_id):
            flag_exist = True
    if not flag_exist:
        schema_dict['entities'].append(entity_dict)


def assess_order_pair(order_before_1_type: str, order_before_1_value: str,
                      order_before_2_type: str, order_before_2_value: str,
                      order_after_1_type: str, order_after_1_value: str,
                      order_after_2_type: str, order_after_2_value: str,
                      l2_order_before_1_type: str, l2_order_before_1_value: str,
                      l2_order_before_2_type: str, l2_order_before_2_value: str,
                      l2_order_after_1_type: str, l2_order_after_1_value: str,
                      l2_order_after_2_type: str, l2_order_after_2_value: str) -> bool:

    # Handle special case where both types and layers match
    if order_before_1_type == order_after_1_type and order_before_2_type == order_after_2_type and \
            order_before_1_value == order_after_1_value and \
            order_before_2_value == order_after_2_value:
        if l2_order_before_1_type == 'exactly' and l2_order_after_1_type == "exactly" and \
                l2_order_before_1_value < l2_order_after_1_value:
            return True

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


def generate_single_graphG(complex_event: str, output_graph_g_directory: str,
                           annotation_directory: str,
                           qnode_directory: str) -> None:

    print('generating Graph G for: ' + complex_event)

    ep_selr_df = pd.read_excel(os.path.join(annotation_directory, complex_event,
                                            complex_event + '_events_selected.xlsx'))
    ep_df = pd.read_excel(os.path.join(annotation_directory, complex_event,
                                       complex_event + '_events.xlsx'))
    arg_df = pd.read_excel(os.path.join(annotation_directory, complex_event,
                                        complex_event + '_arguments.xlsx'))
    temporal_df = pd.read_excel(os.path.join(annotation_directory, complex_event,
                                             complex_event + '_temporal.xlsx'))
    entity_qnode_df = pd.read_excel(os.path.join(annotation_directory, complex_event,
                                                 complex_event + '_kb_linking.xlsx'))

    # We need to add 'sdf_id' column that gives an sdf-friendly
    # 1 is reserved for the instance number
    curr_sel_num = 3
    ep_df['row_num'] = np.arange(len(ep_df))
    ep_df['unique_num'] = curr_sel_num + ep_df['row_num']
    ep_df['unique_str'] = ep_df['unique_num'].apply(lambda x: str(x).zfill(5))
    ep_df['sdf_id'] = 'nist:Events/' + ep_df['unique_str'] + '/' + ep_df['eventprimitive_id']
    curr_sel_num = max(ep_df['unique_num']) + 1
    # We only care about the "eventprimitive_id" column of the selected events
    ep_sel_df = ep_selr_df.loc[:, ['eventprimitive_id']].\
        merge(ep_df, on="eventprimitive_id", how="left")

    # Do this for the other data frames
    arg_df['row_num'] = np.arange(len(arg_df))
    arg_df['unique_num'] = curr_sel_num + arg_df['row_num']
    arg_df['unique_str'] = arg_df['unique_num'].apply(lambda x: str(x).zfill(5))
    arg_df['sdf_id'] = 'nist:Participants/' + arg_df['unique_str'] + '/' + arg_df['arg_id']
    curr_sel_num = max(arg_df['unique_num']) + 1

    entity_qnode_df['row_num'] = np.arange(len(entity_qnode_df))
    entity_qnode_df['unique_num'] = curr_sel_num + entity_qnode_df['row_num']
    entity_qnode_df['unique_str'] = entity_qnode_df['unique_num'].apply(lambda x: str(x).zfill(5))
    entity_qnode_df['sdf_id'] = 'nist:Entities/' + entity_qnode_df['unique_str'] + \
                                '/' + entity_qnode_df['entity_id']
    curr_sel_num = max(entity_qnode_df['unique_num']) + 1

    er_df = pd.read_excel(os.path.join(annotation_directory, complex_event,
                                       complex_event + '_relations.xlsx'))
    er_sel_df = pd.read_excel(os.path.join(annotation_directory, complex_event,
                                           complex_event + '_relations_selected.xlsx'))

    er_df['row_num'] = np.arange(len(er_df))
    er_df['unique_num'] = curr_sel_num + er_df['row_num']
    er_df['unique_str'] = er_df['unique_num'].apply(lambda x: str(x).zfill(5))
    er_df['sdf_id'] = 'nist:Relations/' + er_df['unique_str'] + \
        '/' + er_df['relation_id']
    curr_sel_num = max(er_df['unique_num']) + er_df.shape[0]

    er_df['selected'] = "no"
    if er_sel_df.shape[0] > 0:
        er_df.loc[er_df['relation_id'].isin(er_sel_df['relation_id']), 'selected'] = "yes"

    er_sel_df = er_df.loc[er_df['selected'] == "yes", :]

    qnode_df_fp = os.path.join(qnode_directory, 'dwd_qnode_table_v3.csv')

    # initiate sdf format
    sdf_dict = {'@context': [], '@id': 'nist:Quizlet9/GraphG/' + complex_event,
                'sdfVersion': '2.0',
                'version': 'nist:Quizlet9GraphG:v2022.03.18',
                'ta2': True, 'task2': True, 'ceID': complex_event, 'instances': []}
    sdf_dict['@context'].append('https://kairos-sdf.s3.amazonaws.com/context/kairos-v2.0.jsonld')
    sdf_dict['@context'].append({'nist': 'https://nist.gov/kairos/'})
    # initiate a schema
    root_ep_id = 'nist:Events/00002/nist-GraphG-' + complex_event + '-root'
    schema_dict = {'@id': 'nist:Instances/00001/nistQuizlet9GraphG',
                   'name': 'nist:Quizlet9/GraphG' + complex_event,
                   'confidence': 1, 'ta1ref': 'kairos::NULL',
                   'events': [], 'entities': [],
                   'relations': []}
    sdf_dict['instances'].append(schema_dict)

    ep_id_set = set()
    arg_ep_id_set = set()

    # Add higher event as root so that we can indicate order with children and outlinks

    root_ep_name = 'nist:GraphG/' + complex_event + '/root'
    root_ep_description = 'Root Event for Graph G Complex Event for ' + complex_event
    root_ep_dict = {'@id': root_ep_id, 'name': root_ep_name, 'description': root_ep_description,
                    'ta1ref': 'kairos:NULL', "ta1explanation": "NIST-Constructed Root Node.",
                    'provenance': 'NIST-Constructed root node',
                    'ta2qnode': 'wd:Q3241045', 'ta2qlabel': 'disease_outbreak',
                    'children_gate': "and",
                    'children': []}
    schema_dict['events'].append(root_ep_dict)

    # add event primitives and arguments
    for i, ep_row in ep_sel_df.iterrows():
        add_ep(schema_dict, ep_row, ep_id_set, arg_df, arg_ep_id_set,
               qnode_df_fp, entity_qnode_df, ep_df, er_df)

    # recursively add argument events
    while len(arg_ep_id_set) > 0:
        temp = copy.deepcopy(arg_ep_id_set)
        # print(temp)
        # add events in arg_ep_id_set
        arg_ep_id_set_loop = arg_ep_id_set.copy()
        for arg_ep_id in arg_ep_id_set_loop:
            if arg_ep_id not in ep_id_set:
                ep_id_set.add(arg_ep_id)
                arg_ep_df = ep_df.loc[ep_df['eventprimitive_id'] == arg_ep_id]
                arg_ep_row = arg_ep_df.iloc[0]
                add_ep(schema_dict, arg_ep_row, ep_id_set, arg_df, arg_ep_id_set,
                       qnode_df_fp, entity_qnode_df, ep_df, er_df)
        # remove already added events from arg_ep_id_set
        for arg_ep_id in temp:
            arg_ep_id_set.remove(arg_ep_id)

    # if er_sel_df exists
    er_sel_df = er_df.loc[(er_df['selected'] == "yes") | (er_df['selected'] == "new"), :]
    if len(er_sel_df) > 0:
        # add entity relations
        # add an empty event primitive for relation only arguments (without ep_id)
        for i, er_row in er_sel_df.iterrows():
            er_id = er_row.get('relation_id')
            er_sdf_id = er_row.get('sdf_id')
            er_name = str(er_row.get('description'))
            er_modality = str(er_row.get('attribute'))
            arg_pair_df = arg_df.loc[arg_df['relation_id'] == er_id]
            # get subject and object index in the arg_pair_df
            sub_index = obj_index = -1
            arg_num_0 = arg_pair_df.iloc[0].get('arg_num')
            if arg_num_0 == 'arg1':
                sub_index = 0
                obj_index = 1
            elif arg_num_0 == 'arg2':
                sub_index = 1
                obj_index = 0
            er_subject_arg_id = arg_pair_df.iloc[sub_index].get('arg_id')
            er_subject_ent_id = None
            # if it is an argument subject
            if er_subject_arg_id[:2] == 'AR':
                # add relationSubject entity
                er_subject_ent_id = arg_pair_df.iloc[sub_index].get('entity_id')
                er_subject_ent_sdf_id = ""
                if er_subject_ent_id != "EMPTY_NA":
                    er_subject_ent_sdf_id = \
                        entity_qnode_df.loc[entity_qnode_df['entity_id'] ==
                                            er_subject_ent_id, 'sdf_id'].tolist()[0]
                er_subject_ent_name = str(arg_pair_df.iloc[sub_index].get('description'))
                qnode_name = entity_qnode_df.loc[entity_qnode_df['entity_id'] == er_subject_ent_id,
                                                 'qnode_kb_id_identity'].tolist()[0]
                if qnode_name == "EMPTY_NA" or qnode_name == "NIL":
                    qnode_name = \
                        entity_qnode_df.loc[entity_qnode_df['entity_id'] == er_subject_ent_id,
                                            'qnode_kb_id_type'].tolist()[0]
                qlabel = ''
                if qnode_name != "EMPTY_TBD" and qnode_name != "EMPTY_NA":
                    qlabel = get_qnode_label(qnode_name, qnode_df_fp)
                if '|' in qnode_name:
                    qnode_name = ["wd:{}".format(str.strip()) for str in qnode_name.split('|')]
                else:
                    qnode_name = "wd:{}".format(qnode_name)
                # Only add entity if not already in entity dictionary
                er_subject_ent_dict = {'@id': er_subject_ent_sdf_id, 'name': er_subject_ent_name,
                                       'ta2qnode': qnode_name,
                                       'ta2qlabel': qlabel}

                add_entity(schema_dict, er_subject_ent_dict)
            # if it is an event argument
            elif er_subject_arg_id[:2] == 'VP':
                er_subject_ent_id = er_subject_arg_id
                # if the event is not in ep_list, add the event to schema
                if er_subject_arg_id not in ep_id_set:
                    # print(er_subject_arg_id)
                    subject_ep_df = ep_df.loc[ep_df['eventprimitive_id'] == er_subject_arg_id]
                    ep_row = subject_ep_df.iloc[0]
                    add_ep(schema_dict, ep_row, ep_id_set, arg_df, arg_ep_id_set,
                           qnode_df_fp, entity_qnode_df, ep_df, er_df)
                    er_subject_ent_sdf_id = ep_df.loc[ep_df['eventprimitive_id'] ==
                                                      er_subject_ent_id, 'sdf_id'].tolist()[0]
                else:
                    er_subject_ent_sdf_id = ep_df.loc[ep_df['eventprimitive_id'] ==
                                                      er_subject_ent_id, 'sdf_id'].tolist()[0]

            # add relationObject
            er_object_arg_id = arg_pair_df.iloc[obj_index].get('arg_id')
            er_object_ent_id = None
            # if it is an argument subject
            if er_object_arg_id[:2] == 'AR':
                # add relationSubject entity
                er_object_ent_id = arg_pair_df.iloc[obj_index].get('entity_id')
                er_object_ent_sdf_id = ""
                if er_object_ent_id != "EMPTY_NA":
                    er_object_ent_sdf_id = \
                        entity_qnode_df.loc[entity_qnode_df['entity_id'] ==
                                            er_object_ent_id, 'sdf_id'].tolist()[0]
                er_object_ent_name = str(arg_pair_df.iloc[obj_index].get('description'))
                qnode_name = entity_qnode_df.loc[entity_qnode_df['entity_id'] == er_object_ent_id,
                                                 'qnode_kb_id_identity'].tolist()[0]
                if qnode_name == "EMPTY_NA" or qnode_name == "NIL":
                    qnode_name = \
                        entity_qnode_df.loc[entity_qnode_df['entity_id'] == er_object_ent_id,
                                            'qnode_kb_id_type'].tolist()[0]
                qlabel = ''
                if qnode_name != "EMPTY_TBD" and qnode_name != "EMPTY_NA":
                    qlabel = get_qnode_label(qnode_name, qnode_df_fp)
                if '|' in qnode_name:
                    qnode_name = ["wd:{}".format(str.strip()) for str in qnode_name.split('|')]
                else:
                    qnode_name = "wd:{}".format(qnode_name)
                er_object_ent_dict = {'@id': er_object_ent_sdf_id, 'name': er_object_ent_name,
                                      'ta2qnode': qnode_name,
                                      'ta2qlabel': qlabel}
                add_entity(schema_dict, er_object_ent_dict)
            # if it is an event subject
            if er_object_arg_id[:2] == 'VP':
                er_object_ent_id = er_object_arg_id

                # if the event is not in ep_list, add the event to schema
                if er_object_arg_id not in ep_id_set:
                    # print(er_object_arg_id)
                    object_ep_dict = ep_df.loc[ep_df['eventprimitive_id'] == er_object_arg_id]
                    ep_row = object_ep_dict.iloc[0]
                    add_ep(schema_dict, ep_row, ep_id_set, arg_df, arg_ep_id_set,
                           qnode_df_fp, entity_qnode_df, ep_df, er_df)
                    er_object_ent_sdf_id = ep_df.loc[ep_df['eventprimitive_id'] ==
                                                     er_object_ent_id, 'sdf_id'].tolist()[0]
                else:
                    er_object_ent_sdf_id = ep_df.loc[ep_df['eventprimitive_id'] ==
                                                     er_object_ent_id, 'sdf_id'].tolist()[0]

            qnode_name = er_row.get('qnode_type_id')
            qlabel = ''
            if qnode_name != "EMPTY_TBD":
                qlabel = get_qnode_label(qnode_name, qnode_df_fp)
            # qnodes are mostly missing; set to relation predicate for now
            er_dict = {'@id': er_sdf_id, 'name': er_name,
                       'relationSubject': er_subject_ent_sdf_id,
                       'relationSubject_prov': er_subject_arg_id,
                       'relationPredicate': qnode_name,
                       'relationObject': er_object_ent_sdf_id,
                       'relationObject_prov': er_object_arg_id,
                       'relationProvenance': 'LDC Annotation',
                       'ta1ref': 'kairos:NULL'}
            if er_modality != "none" and er_modality != "generic":
                # Check for a split and add if a list
                if ',' in er_modality:
                    er_modality = er_modality.split(',')
                er_dict['modality'] = er_modality
            schema_dict['relations'].append(er_dict)

    # add matching eps
    temporal_df = pd.merge(temporal_df, ep_sel_df, how='inner', on='eventprimitive_id')
    temporal_df = temporal_df[['eventprimitive_id', 'layer1_order1_type', 'layer1_order1_value',
                               'layer1_order2_type', 'layer1_order2_value', 'sdf_id']]
    temporal_df = temporal_df.loc[temporal_df['layer1_order1_type'] != 'unknown']
    # add orders
    cur_rel_sel_num = 50000
    for i, i_row in temporal_df.iterrows():
        # We do not use the row ide with i_id = i_row.get('eventprimitive_id')
        i_sdf_id = i_row.get('sdf_id')
        j_list = []
        for j, j_row in temporal_df.iterrows():
            if i != j:
                if assess_order_pair(i_row.get('layer1_order1_type'),
                                     i_row.get('layer1_order1_value'),
                                     i_row.get('layer1_order2_type'),
                                     i_row.get('layer1_order2_value'),
                                     j_row.get('layer1_order1_type'),
                                     j_row.get('layer1_order1_value'),
                                     j_row.get('layer1_order2_type'),
                                     j_row.get('layer1_order2_value'),
                                     i_row.get('layer2_order1_type'),
                                     i_row.get('layer2_order1_value'),
                                     i_row.get('layer2_order2_type'),
                                     i_row.get('layer2_order2_value'),
                                     j_row.get('layer2_order1_type'),
                                     j_row.get('layer2_order1_value'),
                                     j_row.get('layer2_order2_type'),
                                     j_row.get('layer2_order2_value')):
                    j_list.append(j_row.get('sdf_id'))

        for j_sdf_id in j_list:
            er_dict = {'@id': 'nist:Relations/{}/GraphG-{}-Bef-{}'.format(cur_rel_sel_num,
                                                                          i_sdf_id, j_sdf_id),
                       'name': '{} happens before {}'.format(i_sdf_id, j_sdf_id),
                       'relationSubject': i_sdf_id,
                       'relationSubject_prov': 'LDC Temporal Annotation',
                       'relationPredicate': "wd:Q79030196",
                       'relationObject': j_sdf_id,
                       'relationObject_prov': 'LDC Temporal Annotation',
                       'relationProvenance': 'LDC Annotation',
                       'ta1ref': 'kairos:NULL'}
            cur_rel_sel_num += 1
            # There are no modalities for this relation
            schema_dict['relations'].append(er_dict)
        # Return the updated version for future increments

    # Make output directory if it does not exist
    if not os.path.isdir(output_graph_g_directory):
        os.makedirs(output_graph_g_directory)
    # transform to json and write file
    json_file_name = os.path.join(output_graph_g_directory,
                                  'graphg-graphg-task2-' + complex_event + '.json')

    with open(json_file_name, 'w') as outfile:
        json.dump(sdf_dict, outfile, indent=4)

    orig_events = ep_sel_df['eventprimitive_id']
    new_events = [ev['@id'].split('/')[2] for ev in schema_dict['events']]
    new_events = [ev for ev in new_events if not ('nist' in ev)]
    extra_events = list(set(new_events) - set(orig_events))
    extra_rels = er_df.loc[er_df['selected'] == "new", 'relation_id'].tolist()
    print("Extra Annotation Events used in Graph G are {}".format(extra_events))
    print("Extra Annotation Relations used in Graph G are {}".format(extra_rels))
    print('Graph G: ' + complex_event + ' generated as file ' +
          'graphg-graphg-task2-' + complex_event + '.json')


def generate_graphg(config_filepath, config_mode) -> None:
    try:
        config = configparser.ConfigParser()
        with open(config_filepath) as configfile:
            config.read_file(configfile)
    except ImportError:
        sys.exit('CANNOT OPEN CONFIG FILE: ' + config_filepath)

    root_dir = config[config_mode]["root_dir"]
    input_subdir = config[config_mode]["input_subdir"]
    output_subdir = config[config_mode]["output_subdir"]
    eval_phase_subdir = config[config_mode]["eval_phase_subdir"]
    input_dir_prefix = os.path.join(root_dir, input_subdir, eval_phase_subdir)
    output_dir_prefix = os.path.join(root_dir, output_subdir, eval_phase_subdir)

    # Subfolders of root_directory/input_subdir/eval_phase_subdir
    task2_annotation_dir = os.path.join(input_dir_prefix,
                                        config[config_mode]["task2_annotation_subdir"])

    # subfolders of root_directory/output_subdir/eval_phase_subdir
    output_graph_g_dir = os.path.join(output_dir_prefix,
                                      config[config_mode]["output_graph_g_subdir"])
    qnode_dir = os.path.join(output_dir_prefix,
                             config[config_mode]["qnode_subdir"])

    complex_event_list = os.listdir(task2_annotation_dir)
    complex_event_list.remove('.DS_Store')

    for complex_event in complex_event_list:
        generate_single_graphG(complex_event, output_graph_g_dir, task2_annotation_dir,
                               qnode_dir)


def convert_owg_to_qnode(wikidata_fp):

    wikidata_fp = '/Users/pcf/soma_Documents/KAIROS/KAIROS_TE_Inputs/FY2021/xpo_v3.json'
    output_fp = '/Users/pcf/soma_Documents/KAIROS/KAIROS_NIST_Outputs/dwd_qnode_table_v3.csv'
    wikidata_df = pd.DataFrame(columns=['qnode', 'qlabel'])
    with open(wikidata_fp) as json_file:
        json_data = json.load(json_file)

    for event_key in json_data['events'].keys():
        event = json_data['events'][event_key]
        qnode = event['wd_node']
        qlabel = event['name']
        if not (qnode in wikidata_df.qnode.unique()):
            wikidata_df = pd.concat([wikidata_df,
                                     pd.DataFrame([{'qnode': qnode, 'qlabel': qlabel}])],
                                    ignore_index=True)
        for arg in event['arguments']:
            for constraint in arg['constraints']:
                qnode = constraint['wd_node']
                qlabel = constraint['name']
                if not (qnode in wikidata_df.qnode.unique()):
                    wikidata_df = pd.concat([wikidata_df,
                                             pd.DataFrame([{'qnode': qnode, 'qlabel': qlabel}])],
                                            ignore_index=True)

    for key in json_data['entities'].keys():
        entity = json_data['entities'][key]

        qnode = entity['wd_node']
        qlabel = entity['name']
        if not (qnode in wikidata_df.qnode.unique()):
            wikidata_df = pd.concat([wikidata_df,
                                     pd.DataFrame([{'qnode': qnode, 'qlabel': qlabel}])],
                                    ignore_index=True)
        for overlay in entity['overlay_parents']:
            qnode = overlay['wd_node']
            qlabel = overlay['name']
            if not (qnode in wikidata_df.qnode.unique()):
                wikidata_df = pd.concat([wikidata_df,
                                         pd.DataFrame([{'qnode': qnode, 'qlabel': qlabel}])],
                                        ignore_index=True)

    for rel_key in json_data['relations'].keys():
        rel = json_data['relations'][rel_key]
        qnode = rel['wd_node']
        qlabel = rel['name']
        if not (qnode in wikidata_df.qnode.unique()):
            wikidata_df = pd.concat([wikidata_df,
                                     pd.DataFrame([{'qnode': qnode, 'qlabel': qlabel}])],
                                    ignore_index=True)
        for arg in rel['arguments']:
            for constraint in arg['constraints']:
                qnode = constraint['wd_node']
                qlabel = constraint['name']
                if not (qnode in wikidata_df.qnode.unique()):
                    wikidata_df = pd.concat([wikidata_df,
                                             pd.DataFrame([{'qnode': qnode, 'qlabel': qlabel}])],
                                            ignore_index=True)

    # Add extra qnodes
    wikidata_df = pd.concat([wikidata_df,
                             pd.DataFrame([{'qnode': 'Q79030196', 'qlabel': 'before'}])],
                            ignore_index=True)
    wikidata_df = pd.concat([wikidata_df,
                             pd.DataFrame([{'qnode': 'Q42240', 'qlabel': 'research'}])],
                            ignore_index=True)
    wikidata_df = pd.concat([wikidata_df,
                             pd.DataFrame([{'qnode': 'Q12722854', 'qlabel': 'sick_person'}])],
                            ignore_index=True)
    wikidata_df = pd.concat([wikidata_df,
                             pd.DataFrame([{'qnode': 'Q1274115', 'qlabel': 'responsibility'}])],
                            ignore_index=True)
    wikidata_df = pd.concat([wikidata_df, pd.DataFrame([{'qnode': 'Q621695', 'qlabel': 'blame'}])],
                            ignore_index=True)
    wikidata_df.to_csv(output_fp, index=False, header=True)
