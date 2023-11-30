import os
import numpy as np
import pandas as pd
import json

# Global variable for temporal qnodes as a reference
temporal_qnodes = ["Q79030196", "Q65560376", "P4330",
                   "wd:Q79030196", "wd:Q65560376", "wd:P4330",
                   "wiki:Q79030196", "wiki:Q65560376", "wiki:P4330"]


def create_single_extraction_directory(score_directory: str, team_name: str, element: str) -> None:
    extraction_output_directory = os.path.join(score_directory, team_name, element)
    # if the extraction_output_directory does not exist, create the folder
    if not os.path.isdir(extraction_output_directory):
        os.makedirs(extraction_output_directory)
    # if the folder exists, clear the contents
    else:
        pass
        # files = glob.glob(os.path.join(extraction_output_directory, '*'))
        # for file in files:
        #    os.remove(file)


def create_extraction_directories(score_directory: str, team_name: str) -> None:
    # schema directory
    create_single_extraction_directory(score_directory, team_name, 'Extracted_Schemas')
    # event directory
    create_single_extraction_directory(score_directory, team_name, 'Extracted_Events')
    # argument directory
    create_single_extraction_directory(score_directory, team_name, 'Extracted_Arguments')
    # children directory
    create_single_extraction_directory(score_directory, team_name, 'Extracted_Children')
    # entity directory
    create_single_extraction_directory(score_directory, team_name, 'Extracted_Entities')
    # relation directory
    create_single_extraction_directory(score_directory, team_name, 'Extracted_Relations')
    create_single_extraction_directory(score_directory, team_name, 'System_Level_Extractions')


# TA1 Methods
def get_ta1_children(children_df: pd.DataFrame, event: json, ev_child_list: list, schema_id: str,
                     ev_id: str) -> pd.DataFrame:
    if 'children' in event.keys():
        children = event['children']
        if children:
            for child in children:
                # initiate child elements
                child_id = child_children_gate = child_optional = child_comment = None
                child_importance = child_outlinks = None
                if 'child' in child.keys():
                    child_id = child['child']
                if 'children_gate' in child.keys():
                    child_children_gate = child['children_gate']
                if 'optional' in child.keys():
                    child_optional = child['optional']
                if 'comment' in child.keys():
                    child_comment = child['comment']
                if 'importance' in child.keys():
                    child_importance = child['importance']
                if 'outlinks' in child.keys():
                    child_outlinks = child['outlinks']
                # if the child_id is not none, add child_id to ev_child_list
                # and add the children_row to children_df
                if child_id is not None:
                    ev_child_list.append(child_id)
                    children_row = {
                        'schema_id': schema_id, 'ev_id': ev_id, 'child_id': child_id,
                        'child_children_gate': child_children_gate,
                        'child_optional': child_optional,
                        'child_comment': child_comment, 'child_importance': child_importance,
                        'child_outlinks': child_outlinks
                    }
                    children_df = pd.concat([children_df, pd.DataFrame([children_row])],
                                            ignore_index=True)

    return children_df


def get_ta1_arguments(arg_df: pd.DataFrame, event: json, ev_arg_list: list,
                      schema_id: str, ev_id: str) -> pd.DataFrame:
    if 'participants' in event.keys():
        participants = event['participants']
        if participants:
            for participant in participants:
                # initiate argument elements
                arg_id = arg_role_name = arg_entity = None
                if '@id' in participant.keys():
                    arg_id = participant['@id']
                if 'roleName' in participant.keys():
                    arg_role_name = participant['roleName']
                if 'entity' in participant.keys():
                    arg_entity = participant['entity']
                # add arg_id to ev_arg_list and add the arg_row to arg_df
                if arg_id is not None:
                    ev_arg_list.append(arg_id)
                # IBM has no argument @id keyword
                else:
                    ev_arg_list.append(arg_role_name)
                arg_row = {
                    'schema_id': schema_id, 'ev_id': ev_id, 'arg_id': arg_id,
                    'arg_role_name': arg_role_name, 'arg_entity': arg_entity
                }
                arg_df = pd.concat([arg_df, pd.DataFrame([arg_row])], ignore_index=True)

    return arg_df


def get_ta1_entities(ent_df: pd.DataFrame, schema: json, entity_list: list,
                     schema_id: str) -> pd.DataFrame:
    if 'entities' in schema.keys():
        ent_list = schema['entities']
        if ent_list:
            for entity in ent_list:
                # initiate entity elements
                ent_id = ent_name = ent_qnode = ent_qlabel = ent_comment = None
                if '@id' in entity.keys():
                    ent_id = entity['@id']
                if 'name' in entity.keys():
                    ent_name = entity['name']
                if 'qnode' in entity.keys():
                    ent_qnode = entity['qnode']
                if 'qlabel' in entity.keys():
                    ent_qlabel = entity['qlabel']
                if 'comment' in entity.keys():
                    ent_comment = entity['comment']
                # add ent_id to entity_list and add ent_row to ent_df
                if ent_id is not None:
                    entity_list.append(ent_id)
                else:
                    entity_list.append(ent_name)
                ent_row = {
                    'schema_id': schema_id, 'ent_id': ent_id, 'ent_name': ent_name,
                    'ent_qnode': ent_qnode,
                    'ent_qlabel': ent_qlabel, 'ent_comment': ent_comment
                }
                ent_df = pd.concat([ent_df, pd.DataFrame([ent_row])], ignore_index=True)

    return ent_df


def get_ta1_relations(rel_df: pd.DataFrame, schema: json, relation_list: list, schema_id: str,
                      rel_inEvent: str) -> pd.DataFrame:
    if 'relations' in schema.keys():
        rel_list = schema['relations']
        if rel_list:
            for relation in rel_list:
                # initiate relation elements
                rel_id = rel_name = rel_subject = rel_predicate = rel_object = None
                rel_relationSubject = rel_relationPredicate = rel_relationObject = None
                if '@id' in relation.keys():
                    rel_id = relation['@id']
                if 'name' in relation.keys():
                    rel_name = relation['name']
                if 'subject' in relation.keys():
                    rel_subject = relation['subject']
                if 'predicate' in relation.keys():
                    rel_predicate = relation['predicate']
                if 'object' in relation.keys():
                    rel_object = relation['object']
                if 'relationSubject' in relation.keys():
                    rel_relationSubject = relation['relationSubject']
                if 'relationPredicate' in relation.keys():
                    rel_relationPredicate = relation['relationPredicate']
                if 'relationObject' in relation.keys():
                    rel_relationObject = relation['relationObject']
                # add rel_id to relation_list and add rel_row to rel_df
                if rel_id is not None:
                    relation_list.append(rel_id)
                else:
                    relation_list.append(rel_name)
                rel_row = {
                    'schema_id': schema_id, 'rel_id': rel_id, 'rel_name': rel_name,
                    'rel_subject': rel_subject,
                    'rel_predicate': rel_predicate, 'rel_object': rel_object,
                    'rel_relationSubject': rel_relationSubject,
                    'rel_relationPredicate': rel_relationPredicate,
                    'rel_relationObject': rel_relationObject, 'rel_inEvent': rel_inEvent
                }
                rel_df = pd.concat([rel_df, pd.DataFrame([rel_row])], ignore_index=True)

    return rel_df


def get_ta1_events(ta1_libraryfile, schema: json, schema_id: str,
                   event_list: list, relation_list: list):
    if 'events' in schema.keys():
        ev_list = schema['events']
        if ev_list:
            for event in ev_list:
                # initiate event elements
                ev_id = ev_name = ev_type = ev_wd_node = ev_wd_label = None
                ev_children_gate = ev_description = ev_goal = None
                ev_minDuration = ev_maxDuration = ev_ta1explanation = None
                ev_privateData = ev_comment = None
                ev_aka = ev_template = ev_repeatable = None
                ev_child_list = []
                ev_arg_list = []
                # get event elements
                if '@id' in event.keys():
                    ev_id = event['@id']
                if 'name' in event.keys():
                    ev_name = event['name']
                if '@type' in event.keys():
                    ev_type = event['@type']
                if 'wd_node' in event.keys():
                    ev_wd_node = event['qnode']
                if 'wd_label' in event.keys():
                    ev_wd_label = event['qlabel']
                if 'children_gate' in event.keys():
                    ev_children_gate = event['children_gate']
                if 'description' in event.keys():
                    ev_description = event['description']
                if 'goal' in event.keys():
                    ev_goal = event['goal']
                if 'minDuration' in event.keys():
                    ev_minDuration = event['minDuration']
                if 'maxDuration' in event.keys():
                    ev_maxDuration = event['maxDuration']
                if 'ta1explanation' in event.keys():
                    ev_ta1explanation = event['ta1explanation']
                if 'privateData' in event.keys():
                    ev_privateData = event['privateData']
                if 'comment' in event.keys():
                    ev_comment = event['comment']
                if 'aka' in event.keys():
                    ev_aka = event['aka']
                if 'template' in event.keys():
                    ev_template = event['template']
                if 'repeatable' in event.keys():
                    ev_repeatable = event['repeatable']
                # get children information
                ta1_libraryfile.children_df = get_ta1_children(ta1_libraryfile.children_df, event,
                                                               ev_child_list, schema_id, ev_id)
                # get argument information
                ta1_libraryfile.arg_df = get_ta1_arguments(ta1_libraryfile.arg_df, event,
                                                           ev_arg_list, schema_id, ev_id)
                # if the ev_id is not none, add ev_id to ev_arg_list
                # and add the ev_row to ev_df
                if ev_id is not None:
                    event_list.append(ev_id)
                    ev_row = {
                        'schema_id': schema_id, 'ev_id': ev_id, 'ev_name': ev_name,
                        'ev_type': ev_type,
                        'ev_wd_node': ev_wd_node, 'ev_wd_label': ev_wd_label,
                        'ev_children_gate': ev_children_gate,
                        'ev_description': ev_description, 'ev_goal': ev_goal,
                        'ev_minDuration': ev_minDuration,
                        'ev_maxDuration': ev_maxDuration, 'ev_ta1explanation': ev_ta1explanation,
                        'ev_privateData': ev_privateData, 'ev_comment': ev_comment,
                        'ev_aka': ev_aka,
                        'ev_template': ev_template, 'ev_repeatable': ev_repeatable,
                        'ev_child_list': ev_child_list,
                        'ev_arg_list': ev_arg_list
                    }
                    ta1_libraryfile.ev_df = pd.concat([ta1_libraryfile.ev_df,
                                                       pd.DataFrame([ev_row])],
                                                      ignore_index=True)
                if 'relations' in event.keys():
                    ta1_libraryfile.rel_df = get_ta1_relations(ta1_libraryfile.rel_df, event,
                                                               relation_list, schema_id, ev_id)
    # The object is updated so there is nothing to return
    return


"""
to do: update TA1 part later
"""
# extract Phase 2 TA1 event, argument, children, entity, and relation information from json files
# and save as csv files


def extract_ta1_elements_from_json_file(ta1_team_name: str, base_file_name: str,
                                        ta1_libraryfile,
                                        json_dict: dict, file_name: str):

    # initiate schema, event, argument, and children dataframes
    schema = json_dict[file_name]
    # initiate schema elements
    sdfVersion = schema_id = schema_name = schema_version = None
    schema_description = schema_template = None
    schema_comment = None
    schema_primitives = []
    event_list = []
    entity_list = []
    relation_list = []
    # get schema elements
    if 'sdfVersion' in schema.keys():
        sdfVersion = schema['sdfVersion']
    if '@id' in schema.keys():
        schema_id = schema['@id']
        ta1_libraryfile.schema_id = schema_id
    if 'name' in schema.keys():
        schema_name = schema['name']
    if 'version' in schema.keys():
        schema_version = schema['version']
    if 'description' in schema.keys():
        schema_description = schema['description']
    if 'template' in schema.keys():
        schema_template = schema['template']
    if 'comment' in schema.keys():
        schema_comment = schema['comment']
    if 'primitives' in schema.keys():
        schema_primitives = schema['primitives']
    # get event, argument, and children information
    # The ta1_events updates events, args, entities, and relations
    get_ta1_events(ta1_libraryfile, schema, schema_id, event_list, relation_list)
    # get entity information
    ta1_libraryfile.ent_df = get_ta1_entities(ta1_libraryfile.ent_df, schema, entity_list,
                                              schema_id)
    # get relation information
    ta1_libraryfile.rel_df = get_ta1_relations(ta1_libraryfile.rel_df, schema, relation_list,
                                               schema_id, None)

    # Separate order relations from relations into order df, remove
    # order relations from rel_df.
    ta1_libraryfile.temporalrel_df = \
        ta1_libraryfile.rel_df.loc[ta1_libraryfile.rel_df['rel_relationPredicate'].isin(
            temporal_qnodes), :]
    ta1_libraryfile.rel_df = \
        ta1_libraryfile.rel_df.loc[(
            ~ta1_libraryfile.rel_df['rel_relationPredicate'].isin(temporal_qnodes)), :]

    # add a schema_row to schema_df
    schema_row = {
        'file_name': file_name, 'ta1_team_name': ta1_team_name, 'sdfVersion': sdfVersion,
        'schema_id': schema_id,
        'schema_name': schema_name, 'schema_version': schema_version,
        'schema_description': schema_description,
        'schema_template': schema_template, 'schema_comment': schema_comment,
        'schema_primitives': schema_primitives, 'event_list': event_list,
        'entity_list': entity_list,
        'relation_list': relation_list
    }
    ta1_libraryfile.schema_df = pd.concat([ta1_libraryfile.schema_df,
                                           pd.DataFrame([schema_row])],
                                          ignore_index=True)

    # Add ta1_team_name to each data frame
    ta1_libraryfile.ev_df['ta1_team_name'] = ta1_team_name
    ta1_libraryfile.arg_df['ta1_team_name'] = ta1_team_name
    ta1_libraryfile.ent_df['ta1_team_name'] = ta1_team_name
    ta1_libraryfile.rel_df['ta1_team_name'] = ta1_team_name
    ta1_libraryfile.temporalrel_df['ta1_team_name'] = ta1_team_name
    ta1_libraryfile.children_df['ta1_team_name'] = ta1_team_name
    ta1_libraryfile.schema_df['ta1_team_name'] = ta1_team_name

    return ta1_libraryfile


"""
updated
"""
# TA2 Methods


def get_ta2_children(children_df: pd.DataFrame, event: json, ev_child_list: list, schema_id: str,
                     ev_id: str, instance_id: str, file_name: str) -> pd.DataFrame:
    instance_id_short = str(instance_id.split('/')[1])
    if 'subgroup_events' in event.keys():
        children = event['subgroup_events']
        if len(children) > 0:
            for child in children:
                ev_child_list.append(child)
                children_row = {
                    'schema_instance_id': file_name + '_' + instance_id_short,
                    'schema_id': schema_id, 'instance_id': instance_id, 'ev_id': ev_id,
                    'child_ev_id': child
                }
                children_df = pd.concat([children_df,
                                        pd.DataFrame([children_row])],
                                        ignore_index=True)
    return children_df


"""
updated
"""


def get_ta2_arguments(arg_df: pd.DataFrame, event: json, ev_arg_list: list,
                      schema_id: str, ev_id: str,
                      instance_id: str, file_name: str) -> pd.DataFrame:
    instance_id_short = str(instance_id.split('/')[1])
    if 'participants' in event.keys():
        participants = event['participants']
        if participants:
            for participant in participants:
                # initiate argument elements
                arg_id = arg_role_name = arg_entity = arg_value_id = None
                arg_ta2entity = arg_ta2confidence = arg_ta2provenance = None
                if '@id' in participant.keys():
                    arg_id = participant['@id']
                if 'roleName' in participant.keys():
                    arg_role_name = participant['roleName']
                if 'entity' in participant.keys():
                    arg_entity = participant['entity']
                # add arg_id to ev_arg_list and add the arg_row to arg_df
                if arg_id is not None:
                    ev_arg_list.append(arg_id)
                # IBM has no argument @id keyword
                else:
                    ev_arg_list.append(arg_role_name)
                if 'values' in participant.keys():
                    if isinstance(participant['values'], list):
                        for value in participant['values']:
                            if '@id' in value.keys():
                                arg_value_id = value['@id']
                            if 'ta2entity' in value.keys():
                                arg_ta2entity = value['ta2entity']
                            if 'confidence' in value.keys():
                                arg_ta2confidence = value['confidence']
                            if 'provenance' in value.keys():
                                arg_ta2provenance = value['provenance']
                            arg_row = {
                                'schema_instance_id':
                                    file_name + '_' + instance_id_short,
                                'schema_id': schema_id, 'instance_id': instance_id,
                                'ev_id': ev_id, 'arg_id': arg_id,
                                'arg_role_name': arg_role_name, 'arg_entity': arg_entity,
                                'arg_value_id': arg_value_id,
                                'arg_ta2entity': arg_ta2entity,
                                'arg_ta2confidence': arg_ta2confidence,
                                'arg_ta2provenance': arg_ta2provenance
                            }
                            arg_df = pd.concat([arg_df, pd.DataFrame([arg_row])], ignore_index=True)
                    else:
                        if '@id' in participant['values'].keys():
                            arg_value_id = participant['values']['@id']
                        if 'ta2entity' in participant['values'].keys():
                            arg_ta2entity = participant['values']['ta2entity']
                        if 'confidence' in participant['values'].keys():
                            arg_ta2confidence = participant['values']['confidence']
                        if 'provenance' in participant['values'].keys():
                            arg_ta2provenance = participant['values']['provenance']
                        schema_instance_id_str = \
                            file_name + '_' + instance_id_short
                        arg_row = {
                            'schema_instance_id': schema_instance_id_str,
                            'schema_id': schema_id, 'instance_id': instance_id,
                            'ev_id': ev_id, 'arg_id': arg_id,
                            'arg_role_name': arg_role_name, 'arg_entity': arg_entity,
                            'arg_value_id': arg_value_id,
                            'arg_ta2entity': arg_ta2entity, 'arg_ta2confidence': arg_ta2confidence,
                            'arg_ta2provenance': arg_ta2provenance
                        }
                        arg_df = pd.concat([arg_df, pd.DataFrame([arg_row])], ignore_index=True)
                else:
                    # If there is no "values", we add a stub holder with no ta2 entity link
                    arg_row = {
                        'schema_instance_id': file_name + '_' + instance_id_short,
                        'schema_id': schema_id, 'instance_id': instance_id,
                        'ev_id': ev_id, 'arg_id': arg_id,
                        'arg_role_name': arg_role_name, 'arg_entity': arg_entity,
                        'arg_value_id': "kairos:NULL",
                        'arg_ta2entity': "kairos:NULL", 'arg_ta2confidence': '0',
                        'arg_ta2provenance': "kairos:NULL"
                    }
                    arg_df = pd.concat([arg_df, pd.DataFrame([arg_row])], ignore_index=True)

    return arg_df


"""
updated
"""


def get_ta2_entities(ent_df: pd.DataFrame, schema: json, entity_list: list, schema_id: str,
                     instance_id: str, file_name: str) -> pd.DataFrame:
    instance_id_short = str(instance_id.split('/')[1])
    if 'entities' in schema.keys():
        ent_list = schema['entities']
        if ent_list:
            for entity in ent_list:
                # initiate entity elements
                ent_id = ent_name = ent_wd_node = ent_wd_label = None
                ent_ta2wd_node = ent_ta2wd_label = None
                ent_wd_description = ent_ta2wd_description = None
                if '@id' in entity.keys():
                    ent_id = entity['@id']
                if 'name' in entity.keys():
                    ent_name = entity['name']
                if 'wd_node' in entity.keys():
                    ent_wd_node = entity['wd_node']
                if 'wd_label' in entity.keys():
                    ent_wd_label = entity['wd_label']
                if 'wd_description' in entity.keys():
                    ent_wd_description = entity['wd_description']
                if 'ta2wd_node' in entity.keys():
                    ent_ta2wd_node = entity['ta2wd_node']
                if 'ta2wd_label' in entity.keys():
                    ent_ta2wd_label = entity['ta2wd_label']
                if 'ta2wd_description' in entity.keys():
                    ent_ta2wd_description = entity['ta2wd_description']

                # add ent_id to entity_list and add ent_row to ent_df
                if ent_id is not None:
                    entity_list.append(ent_id)
                else:
                    entity_list.append(ent_name)
                ent_row = {
                    'schema_instance_id': file_name + '_' + instance_id_short,
                    'schema_id': schema_id, 'instance_id': instance_id, 'ent_id': ent_id,
                    'ent_name': ent_name, 'ent_wd_node': ent_wd_node,
                    'ent_wd_label': ent_wd_label,
                    'ent_wd_description': ent_wd_description,
                    'ent_ta2wd_node': ent_ta2wd_node,
                    'ent_ta2wd_label': ent_ta2wd_label,
                    'ent_ta2wd_description': ent_ta2wd_description
                }
                ent_df = pd.concat([ent_df, pd.DataFrame([ent_row])], ignore_index=True)

    return ent_df


"""
updated
"""


def get_ta2_relations(rel_df: pd.DataFrame, schema: json, relation_list: list, schema_id: str,
                      instance_id: str, file_name: str, rel_inEvent: str) -> pd.DataFrame:
    instance_id_short = str(instance_id.split('/')[1])
    if 'relations' in schema.keys():
        rel_list = schema['relations']
        if rel_list:
            for relation in rel_list:
                # initiate relation elements
                rel_id = rel_name = rel_subject = rel_predicate = rel_object = None
                rel_relationSubject = rel_relationPredicate = rel_relationObject = None
                rel_relationProvenance = rel_relationSubject_prov = None
                rel_relationObject_prov = rel_confidence = None
                rel_ta1ref = None
                if '@id' in relation.keys():
                    rel_id = relation['@id']
                if 'name' in relation.keys():
                    rel_name = relation['name']
                if 'subject' in relation.keys():
                    rel_subject = relation['subject']
                if 'predicate' in relation.keys():
                    rel_predicate = relation['predicate']
                if 'object' in relation.keys():
                    rel_object = relation['object']
                if 'relationSubject' in relation.keys():
                    rel_relationSubject = relation['relationSubject']
                if 'relationPredicate' in relation.keys():
                    rel_relationPredicate = relation['relationPredicate']
                if 'relationObject' in relation.keys():
                    rel_relationObject = relation['relationObject']
                if 'relationProvenance' in relation.keys():
                    rel_relationProvenance = relation['relationProvenance']
                if 'relationSubject_prov' in relation.keys():
                    rel_relationSubject_prov = relation['relationSubject_prov']
                if 'relationObject_prov' in relation.keys():
                    rel_relationObject_prov = relation['relationObject_prov']
                if 'confidence' in relation.keys():
                    rel_confidence = relation['confidence']
                if 'ta1ref' in relation.keys():
                    rel_ta1ref = relation['ta1ref']
                # add rel_id to relation_list and add rel_row to rel_df
                if rel_id is not None:
                    relation_list.append(rel_id)
                else:
                    relation_list.append(rel_name)
                rel_row = {
                    'schema_instance_id': file_name + '_' + instance_id_short,
                    'schema_id': schema_id, 'instance_id': instance_id,
                    'rel_id': rel_id, 'rel_name': rel_name, 'rel_subject': rel_subject,
                    'rel_predicate': rel_predicate, 'rel_object': rel_object,
                    'rel_relationSubject': rel_relationSubject,
                    'rel_relationPredicate': rel_relationPredicate,
                    'rel_relationObject': rel_relationObject,
                    'rel_relationProvenance': rel_relationProvenance,
                    'rel_relationSubject_prov': rel_relationSubject_prov,
                    'rel_relationObject_prov': rel_relationObject_prov,
                    'rel_ta1ref': rel_ta1ref,
                    'rel_confidence': rel_confidence,
                    'rel_inEvent': rel_inEvent
                }
                rel_df = pd.concat([rel_df, pd.DataFrame([rel_row])], ignore_index=True)

    return rel_df


"""
updated
"""


def get_ta2_events(ta2_ceinstance, schema: json, schema_id: str,
                   event_list: list, instance_name: str, instance_id: str, file_name: str,
                   relation_list: list):
    instance_id_short = str(instance_id.split('/')[1])
    ce_id = ta2_ceinstance.ce_name
    task_str = "task_1"
    if ta2_ceinstance.is_task2:
        task_str = "task_2"
    if 'events' in schema.keys():
        ev_list = schema['events']
        if ev_list:
            for event in ev_list:
                # initiate event elements
                ev_id = ev_name = ev_type = ev_wd_node = ev_wd_label = ev_children_gate = None
                ev_wd_description = ev_description = ev_goal = ev_ta1ref = None
                ev_ta2wd_node = ev_ta2wd_label = ev_ta2wd_description = None
                ev_ta1explanation = None
                ev_privateData = ev_comment = None
                ev_aka = ev_template = ev_repeatable = None
                ev_provenance = ev_prediction_provenance = None
                ev_confidence = ev_confidence_val = ev_modality = None
                ev_duration = ev_earliestStartTime = ev_latestStartTime = None
                ev_earliestEndTime = ev_latestEndTime = ev_absoluteTime = None
                ev_outlinks = None
                ev_parent = None
                ev_child_list = []
                ev_arg_list = []
                # get event elements
                if '@id' in event.keys():
                    ev_id = event['@id']
                if 'name' in event.keys():
                    ev_name = event['name']
                if '@type' in event.keys():
                    ev_type = event['@type']
                if 'wd_node' in event.keys():
                    ev_wd_node = event['wd_node']
                if 'wd_node' in event.keys():
                    ev_wd_label = event['wd_label']
                if 'wd_description' in event.keys():
                    ev_wd_description = event['wd_description']
                if 'ta2wd_node' in event.keys():
                    ev_ta2wd_node = event['ta2wd_node']
                if 'ta2wd_label' in event.keys():
                    ev_ta2wd_label = event['ta2wd_label']
                if 'ta2wd_description' in event.keys():
                    ev_wd_description = event['ta2wd_description']
                if 'ta1ref' in event.keys():
                    ev_ta1ref = event['ta1ref']
                if 'parent' in event.keys():
                    ev_parent = event['parent']
                if 'children_gate' in event.keys():
                    ev_children_gate = event['children_gate']
                if 'description' in event.keys():
                    ev_description = event['description']
                if 'goal' in event.keys():
                    ev_goal = event['goal']
                if 'ta1explanation' in event.keys():
                    ev_ta1explanation = event['ta1explanation']
                if 'privateData' in event.keys():
                    ev_privateData = event['privateData']
                if 'comment' in event.keys():
                    ev_comment = event['comment']
                if 'aka' in event.keys():
                    ev_aka = event['aka']
                if 'template' in event.keys():
                    ev_template = event['template']
                if 'repeatable' in event.keys():
                    ev_repeatable = event['repeatable']
                if 'provenance' in event.keys():
                    if ta2_ceinstance.ta2_team_name == "RESIN":
                        ev_provenance = event['provenance'][0]
                    else:
                        ev_provenance = event['provenance']
                if 'predictionProvenance' in event.keys():
                    ev_prediction_provenance = event['predictionProvenance']
                if 'confidence' in event.keys():
                    ev_confidence = event['confidence']
                    ev_confidence_val = np.nanmax(event['confidence'])
                if 'modality' in event.keys():
                    ev_modality = event['modality']
                if 'temporal' in event.keys():
                    if 'duration' in event['temporal'].keys():
                        ev_duration = event['temporal']['duration']
                    if 'earliestStartTime' in event['temporal'].keys():
                        ev_earliestStartTime = event['temporal']['earliestStartTime']
                    if 'latestStartTime' in event['temporal'].keys():
                        ev_latestStartTime = event['temporal']['latestStartTime']
                    if 'earliestEndTime' in event['temporal'].keys():
                        ev_earliestEndTime = event['temporal']['earliestEndTime']
                    if 'latestEndTime' in event['temporal'].keys():
                        ev_latestEndTime = event['temporal']['latestEndTime']
                    if 'absoluteTime' in event['temporal'].keys():
                        ev_absoluteTime = event['temporal']['latestEndTime']
                if 'outlinks' in event.keys():
                    ev_outlinks = event['outlinks']
                # get children information
                ta2_ceinstance.children_df = get_ta2_children(ta2_ceinstance.children_df, event,
                                                              ev_child_list, schema_id, ev_id,
                                                              instance_id, file_name)
                # get argument information
                ta2_ceinstance.arg_df = get_ta2_arguments(ta2_ceinstance.arg_df, event,
                                                          ev_arg_list, schema_id, ev_id,
                                                          instance_id, file_name)
                # if the ev_id is not none, add ev_id to ev_arg_list
                # and add the ev_row to ev_df
                if ev_id is not None:
                    event_list.append(ev_id)
                    ev_row = {
                        'schema_instance_id': file_name + '_' + instance_id_short,
                        'schema_id': schema_id, 'instance_name': instance_name, 'ce_id': ce_id,
                        'task': task_str,
                        'instance_id': instance_id, 'instance_id_short': instance_id_short,
                        'ev_id': ev_id, 'ev_name': ev_name, 'ev_type': ev_type,
                        'ev_wd_node': ev_wd_node, 'ev_wd_label': ev_wd_label,
                        'ev_wd_description': ev_wd_description,
                        'ev_ta2wd_node': ev_ta2wd_node, 'ev_ta2wd_label': ev_ta2wd_label,
                        'ev_ta2wd_description': ev_ta2wd_description,
                        'ev_ta1ref': ev_ta1ref,
                        'ev_children_gate': ev_children_gate,
                        'ev_description': ev_description, 'ev_goal': ev_goal,
                        'ev_ta1explanation': ev_ta1explanation,
                        'ev_privateData': ev_privateData, 'ev_comment': ev_comment,
                        'ev_aka': ev_aka,
                        'ev_template': ev_template, 'ev_repeatable': ev_repeatable,
                        'ev_parent': ev_parent,
                        'ev_child_list': ev_child_list,
                        'ev_arg_list': ev_arg_list, 'ev_provenance': ev_provenance,
                        'ev_confidence': ev_confidence,
                        'ev_confidence_val': ev_confidence_val,
                        'ev_prediction_provenance': ev_prediction_provenance,
                        'ev_modality': ev_modality,
                        'ev_outlinks': ev_outlinks,
                        'ev_duration': ev_duration,
                        'ev_earliestStartTime': ev_earliestStartTime,
                        'ev_latestStartTime': ev_latestStartTime,
                        'ev_earliestEndTime': ev_earliestEndTime,
                        'ev_latestEndTime': ev_latestEndTime,
                        'ev_absoluteTime': ev_absoluteTime
                    }
                    ta2_ceinstance.ev_df = pd.concat([ta2_ceinstance.ev_df,
                                                      pd.DataFrame([ev_row])],
                                                     ignore_index=True)
                if 'relations' in event.keys():
                    ta2_ceinstance.rel_df = get_ta2_relations(ta2_ceinstance.rel_df, event,
                                                              relation_list, schema_id,
                                                              instance_id, file_name, ev_id)

    # Since the object is updated, nothing is returned
    return


"""
change/add field names
"""


def extract_ta2_elements_from_json_instance(ta2_team_name: str,
                                            ta2_ceinstance,
                                            instance_schema: dict,
                                            json_dict: dict, file_name: str, instance_id: str,
                                            instance_name: str):

    schema = json_dict[file_name]
    # initiate schema elements
    instance_id_short = str(instance_id.split('/')[1])
    instance_confidence = None
    instance_description = instance_ta1ref = None
    sdfVersion = ceId = schema_id = schema_name = schema_version = None
    schema_description = schema_template = None
    schema_comment = None
    schema_primitives = []
    event_list = []
    entity_list = []
    relation_list = []
    provenance_list = []
    # get schema elements
    if 'sdfVersion' in schema.keys():
        sdfVersion = schema['sdfVersion']
    if 'ceID' in schema.keys():
        ceId = schema['ceID']
    if '@id' in schema.keys():
        schema_id = schema['@id']
    if 'name' in schema.keys():
        schema_name = schema['name']
    if 'version' in schema.keys():
        schema_version = schema['version']
    if 'description' in schema.keys():
        schema_description = schema['description']
    if 'template' in schema.keys():
        schema_template = schema['template']
    if 'comment' in schema.keys():
        schema_comment = schema['comment']
    if 'primitives' in schema.keys():
        schema_primitives = schema['primitives']
    if 'provenanceData' in schema.keys():
        provenance_list = schema['provenanceData']
    # Get the Instance Information
    if 'confidence' in instance_schema.keys():
        instance_confidence = instance_schema['confidence']
    if 'description' in instance_schema.keys():
        instance_description = instance_schema['description']
    if 'ta1ref' in instance_schema.keys():
        instance_ta1ref = instance_schema['ta1ref']
    # Now get the specific information
    get_ta2_events(ta2_ceinstance, instance_schema, schema_id, event_list, instance_name,
                   instance_id, file_name,
                   relation_list)
    # get entity information
    ta2_ceinstance.ent_df = get_ta2_entities(ta2_ceinstance.ent_df, instance_schema, entity_list,
                                             schema_id, instance_id, file_name)
    # get relation information
    ta2_ceinstance.rel_df = get_ta2_relations(ta2_ceinstance.rel_df, instance_schema,
                                              relation_list, schema_id, instance_id,
                                              file_name, None)
    # Separate order relations from relations into order df, remove
    # order relations from rel_df.
    ta2_ceinstance.temporalrel_df = \
        ta2_ceinstance.rel_df.loc[ta2_ceinstance.rel_df['rel_relationPredicate'].isin(
            temporal_qnodes), :]
    ta2_ceinstance.rel_df = \
        ta2_ceinstance.rel_df.loc[(
            ~ta2_ceinstance.rel_df['rel_relationPredicate'].isin(temporal_qnodes)), :]

    # add a schema_row to schema_df
    # All TA2 File Submissions follow <ta1>-<ta2>-<task>-<ce>
    file_split = file_name.split('.')[0].split('-')
    ta1_team_name = file_split[0].upper()
    ta2_team_name = file_split[1].upper()
    ta2_ceinstance.ta1_team_name = ta1_team_name
    schema_row = {
        'file_name': file_name, 'ta2_team_name': ta2_team_name, 'ta1_team_name': ta1_team_name,
        'sdfVersion': sdfVersion,
        'schema_instance_id': ta2_ceinstance.schema_instance_id,
        'schema_id': schema_id,
        'schema_name': schema_name, 'schema_version': schema_version,
        'schema_description': schema_description,
        'schema_template': schema_template, 'schema_comment': schema_comment,
        'instance_name': instance_name, 'instance_description': instance_description,
        'instance_ta1ref': instance_ta1ref,
        'instance_id': instance_id, 'instance_id_short': instance_id_short,
        'instance_confidence': instance_confidence,
        'schema_primitives': schema_primitives, 'event_list': event_list,
        'entity_list': entity_list,
        'relation_list': relation_list, 'ce_id': ceId, 'provenance_list': provenance_list
    }
    ta2_ceinstance.schema_df = pd.concat([ta2_ceinstance.schema_df,
                                          pd.DataFrame([schema_row])],
                                         ignore_index=True)
    # Add ta1_team_name and ta2_team_name to each data frame
    ta2_ceinstance.ev_df['ta1_team_name'] = ta1_team_name
    ta2_ceinstance.arg_df['ta1_team_name'] = ta1_team_name
    ta2_ceinstance.ent_df['ta1_team_name'] = ta1_team_name
    ta2_ceinstance.rel_df['ta1_team_name'] = ta1_team_name
    ta2_ceinstance.temporalrel_df['ta1_team_name'] = ta1_team_name
    ta2_ceinstance.children_df['ta1_team_name'] = ta1_team_name
    ta2_ceinstance.schema_df['ta1_team_name'] = ta1_team_name
    ta2_ceinstance.ev_df['ta2_team_name'] = ta2_team_name
    ta2_ceinstance.arg_df['ta2_team_name'] = ta2_team_name
    ta2_ceinstance.ent_df['ta2_team_name'] = ta2_team_name
    ta2_ceinstance.rel_df['ta2_team_name'] = ta2_team_name
    ta2_ceinstance.temporalrel_df['ta2_team_name'] = ta2_team_name
    ta2_ceinstance.children_df['ta2_team_name'] = ta2_team_name
    ta2_ceinstance.schema_df['ta2_team_name'] = ta2_team_name

    return ta2_ceinstance
