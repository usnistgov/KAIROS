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
# analyze the step related information, e.g., size and keywords
######################################################################################
from scripts import load_data as load, duplication as dup


# get top level keywords under schemas
def get_schema_keywords(json_dict: dict):
    keyword_list = []
    for file_name in json_dict:
        json_data = json_dict[file_name]
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                for keyword in schema:
                    if keyword not in keyword_list:
                        keyword_list.append(keyword)

    return keyword_list


# get steps keywords
def get_steps_keywords(json_dict: dict):
    keyword_list = []

    for file_name in json_dict:
        json_data = json_dict[file_name]
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                if 'steps' in schema.keys():
                    step_list = schema['steps']
                    if step_list:
                        for step in step_list:
                            for step_key in step:
                                if step_key not in keyword_list:
                                    keyword_list.append(step_key)

    return keyword_list


# get temporal keywords
def get_temporal_keywords(json_dict: dict):
    keyword_list = []

    for file_name in json_dict:
        json_data = json_dict[file_name]
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                if 'steps' in schema.keys():
                    step_list = schema['steps']
                    if step_list:
                        for step in step_list:
                            if 'temporal' in step.keys():
                                temporal_list = step['temporal']
                                if temporal_list:
                                    for temporal in temporal_list:
                                        for temporal_key in temporal:
                                            if temporal_key not in keyword_list:
                                                keyword_list.append(temporal_key)

    return keyword_list


# get slots keywords
def get_ep_and_arg_prov(json_dict: dict):
    ep_prov_list = []
    arg_prov_list = []

    for file_name in json_dict:
        json_data = json_dict[file_name]
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                if 'steps' in schema.keys():
                    ep_list = schema['steps']
                    if ep_list:
                        for ep in ep_list:
                            if 'provenance' in ep.keys():
                                ep_prov = ep['provenance']
                                if len(ep_prov) > 0:
                                    if ep_prov not in ep_prov_list:
                                        ep_prov_list.append(ep_prov)
                            if 'participants' in ep.keys():
                                arg_list = ep['participants']
                                if arg_list:
                                    for arg in arg_list:
                                        if 'values' in arg.keys():
                                            value_list = arg['values']
                                            if value_list:
                                                for value in value_list:
                                                    if 'provenance' in value.keys():
                                                        value_prov = value['provenance']
                                                        if len(value_prov) > 0:
                                                            if value_prov not in arg_prov_list:
                                                                arg_prov_list.append(value_prov)

    return ep_prov_list, arg_prov_list


# get slots keywords
def get_participants_keywords(json_dict: dict):
    keyword_list = []

    for file_name in json_dict:
        json_data = json_dict[file_name]
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                if 'steps' in schema.keys():
                    step_list = schema['steps']
                    if step_list:
                        for step in step_list:
                            if 'participants' in step.keys():
                                participant_list = step['participants']
                                if participant_list:
                                    for participant in participant_list:
                                        for participant_key in participant:
                                            if participant_key not in keyword_list:
                                                keyword_list.append(participant_key)

    return keyword_list


# get slots keywords
def get_participants_values_keywords(json_dict: dict):
    keyword_list = []

    for file_name in json_dict:
        json_data = json_dict[file_name]
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                if 'steps' in schema.keys():
                    step_list = schema['steps']
                    if step_list:
                        for step in step_list:
                            if 'participants' in step.keys():
                                participant_list = step['participants']
                                if participant_list:
                                    for participant in participant_list:
                                        if 'values' in participant.keys():
                                            value_list = participant['values']
                                            if value_list:
                                                for value in value_list:
                                                    for value_key in value:
                                                        if value_key not in keyword_list:
                                                            keyword_list.append(value_key)

    return keyword_list


# check whether a step has confidence in temporal or slots
def is_step_with_confidence(step: dict):
    if 'temporal' in step.keys():
        temporal_list = step['temporal']
        if temporal_list:
            for temporal in temporal_list:
                if 'confidence' in temporal.keys():
                    confidence = float(temporal['confidence'])
                    if confidence > 0:
                        return True

    if 'slots' in step.keys():
        slot_list = step['slots']
        if slot_list:
            for slot in slot_list:
                if 'values' in slot.keys():
                    value_list = slot['values']
                    if value_list:
                        for value in value_list:
                            if 'confidence' in value.keys():
                                confidence = float(value['confidence'])
                                if confidence > 0:
                                    return True

    return False


# get all steps without checking duplication and confidence
def get_all_steps(json_dict: dict):
    result = {}

    for file_name in json_dict:
        json_data = json_dict[file_name]
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                if 'steps' in schema.keys():
                    step_list = schema['steps']
                    for step in step_list:
                        step_id = file_name + '_' + step['@id']
                        temp_dict = {step_id: step}
                        result.update(temp_dict)

    return result


# get all steps with hard-duplication check
def get_unique_steps(json_dict: dict):
    result = {}

    for file_name in json_dict:
        json_data = json_dict[file_name]
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                if 'steps' in schema.keys():
                    step_list = schema['steps']
                    for step in step_list:
                        if not dup.is_hard_duplicated(file_name, step, result):
                            step_id = file_name + '_' + step['@id']
                            temp_dict = {step_id: step}
                            result.update(temp_dict)

    return result


# get all unique steps with confidence, confidence 0 is excluded
def get_unique_steps_with_confidence(json_dict: dict):
    result = {}

    for file_name in json_dict:
        json_data = json_dict[file_name]
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                if 'steps' in schema.keys():
                    step_list = schema['steps']
                    for step in step_list:
                        if is_step_with_confidence(step):
                            if not dup.is_hard_duplicated(file_name, step, result):
                                step_id = file_name + '_' + step['@id']
                                temp_dict = {step_id: step}
                                result.update(temp_dict)

    return result


if __name__ == "__main__":
    # directory_path = '../../../../Quizlet_3/TA2_outputs/ALL/'
    ta2_team_name = ['CMU']
    ta2_output_directory = '../../../../../Quizlet_4/TA2_outputs/'
    ta1_team_name = ['ALL']
    ta1_output_directory = '../../../../../Quizlet_4/TA1_outputs/'
    json_dict = load.load_json_directory(ta1_team_name, ta1_output_directory)
    print('keywords of schema are: ' + str(get_schema_keywords(json_dict)))
    print('keywords of steps are: ' + str(get_steps_keywords(json_dict)))
    print('keywords of temporal are: ' + str(get_temporal_keywords(json_dict)))
    print('keywords of participants are: ' + str(get_participants_keywords(json_dict)))
    print('keywords of participant_values are: ' + str(get_participants_values_keywords(json_dict)))
    print('size of all steps: ' + str(len(get_all_steps(json_dict))))
    print('size of unique steps: ' + str(len(get_unique_steps(json_dict))))
    print('size of unique steps with confidence: ' + str(len(get_unique_steps_with_confidence(json_dict))))
    # ep_prov_list, arg_prov_list = get_ep_and_arg_prov(json_dict)
    # print('provenance of ep: ' + str(ep_prov_list))
    # print('provenance of arg: ' + str(arg_prov_list))
