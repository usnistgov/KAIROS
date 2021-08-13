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
# analyze the entityRelation related information, e.g., size and keywords
######################################################################################
from scripts import load_data as load, duplication as dup


# get order keywords
def get_entityRelation_keywords(json_dict: dict):
    keyword_list = []

    for file_name in json_dict:
        json_data = json_dict[file_name]
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                if 'entityRelations' in schema.keys():
                    entityRelation_list = schema['entityRelations']
                    if entityRelation_list:
                        for entityRelation in entityRelation_list:
                            for keyword in entityRelation:
                                if keyword not in keyword_list:
                                    keyword_list.append(keyword)

    return keyword_list


# get entityRelations_relations keywords
def get_entityRelations_relations_relationPredicate_keywords(json_dict: dict):
    keyword_list = []

    for file_name in json_dict:
        json_data = json_dict[file_name]
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                if 'entityRelations' in schema.keys():
                    entityRelation_list = schema['entityRelations']
                    if entityRelation_list:
                        for entityRelation in entityRelation_list:
                            if 'relations' in entityRelation.keys():
                                relations_list = entityRelation['relations']
                                if relations_list:
                                    for relations in relations_list:
                                        if 'relationPredicate' in relations.keys():
                                            relationPredicate = relations['relationPredicate']
                                            if relationPredicate not in keyword_list:
                                                keyword_list.append(relationPredicate)

    return keyword_list


# get entityRelations_relations keywords
def get_entityRelations_relations_keywords(json_dict: dict):
    keyword_list = []

    for file_name in json_dict:
        json_data = json_dict[file_name]
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                if 'entityRelations' in schema.keys():
                    entityRelation_list = schema['entityRelations']
                    if entityRelation_list:
                        for entityRelation in entityRelation_list:
                            if 'relations' in entityRelation.keys():
                                relations_list = entityRelation['relations']
                                if relations_list:
                                    for relation in relations_list:
                                        for keyword in relation.keys():
                                            if keyword not in keyword_list:
                                                keyword_list.append(keyword)

    return keyword_list


# check whether an entityRelation has confidence
def is_entityRelations_with_confidence(entityRelation: dict):
    if 'relations' in entityRelation.keys():
        relation_list = entityRelation['relations']
        if relation_list:
            for relation in relation_list:
                if 'confidence' in relation.keys():
                    confidence = relation['confidence']
                    if confidence == 'unknown':
                        return False
                    if float(confidence) > 0:
                        return True

    return False


# get all entityRelations without checking duplication and confidence
def get_all_entityRelations(json_dict: dict):
    result = {}

    for file_name in json_dict:
        json_data = json_dict[file_name]
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                if 'entityRelations' in schema.keys():
                    entityRelation_list = schema['entityRelations']
                    if entityRelation_list:
                        for i in range(0, len(entityRelation_list)):
                            entityRelation = entityRelation_list[i]
                            entityRelation_id = file_name + '_entityRelation_' + str(i)
                            temp_dict = {entityRelation_id: entityRelation}
                            result.update(temp_dict)

    return result


# get all entityRelations with hard-duplication check
def get_unique_entityRelations(json_dict: dict):
    result = {}

    for file_name in json_dict:
        json_data = json_dict[file_name]
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                if 'entityRelations' in schema.keys():
                    entityRelation_list = schema['entityRelations']
                    if entityRelation_list:
                        for i in range(0, len(entityRelation_list)):
                            entityRelation = entityRelation_list[i]
                            if not dup.is_hard_duplicated(file_name, entityRelation, result):
                                entityRelation_id = file_name + '_entityRelation_' + str(i)
                                temp_dict = {entityRelation_id: entityRelation}
                                result.update(temp_dict)

    return result


# get all unique entityRelations with confidence, confidence 0 is excluded
def get_unique_entityRelations_with_confidence(json_dict: dict):
    result = {}

    for file_name in json_dict:
        json_data = json_dict[file_name]
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                if 'entityRelations' in schema.keys():
                    entityRelation_list = schema['entityRelations']
                    if entityRelation_list:
                        for i in range(0, len(entityRelation_list)):
                            entityRelation = entityRelation_list[i]
                            if is_entityRelations_with_confidence(entityRelation):
                                if not dup.is_hard_duplicated(file_name, entityRelation, result):
                                    entityRelation_id = file_name + '_entityRelation_' + str(i)
                                    temp_dict = {entityRelation_id: entityRelation}
                                    result.update(temp_dict)

    return result


def get_ent_rel_prov(json_dict: dict):
    result_list = []

    for file_name in json_dict:
        json_data = json_dict[file_name]
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                if 'entityRelations' in schema.keys():
                    ent_rel_list = schema['entityRelations']
                    if ent_rel_list:
                        for ent_rel in ent_rel_list:
                            rel_list = ent_rel['relations']
                            if rel_list:
                                for rel in rel_list:
                                    if 'provenance' in rel.keys():
                                        rel_prov = rel['provenance']
                                        if len(rel_prov) > 0:
                                            if rel_prov not in result_list:
                                                result_list.append(rel_prov)

    return result_list


if __name__ == "__main__":
    ta2_team_name = ['CMU']
    ta2_output_directory = '../../../../../Quizlet_4/TA2_outputs/'
    ta1_team_name = ['ALL']
    ta1_output_directory = '../../../../../Quizlet_4/TA1_outputs/'
    json_dict = load.load_json_directory(ta1_team_name, ta1_output_directory)
    print('keywords of entityRelations are: ' + str(get_entityRelation_keywords(json_dict)))
    print('keywords of relations are: ' + str(get_entityRelations_relations_keywords(json_dict)))
    print('keywords of entityRelations_relations_relationPredicate are: ' +
          str(get_entityRelations_relations_relationPredicate_keywords(json_dict)))
    print('size of all entityRelations: ' + str(len(get_all_entityRelations(json_dict))))
    print('size of unique orders: ' + str(len(get_unique_entityRelations(json_dict))))
    print('size of unique orders with confidence: ' + str(len(get_unique_entityRelations_with_confidence(json_dict))))
    # ent_rel_prov_list = get_ent_rel_prov(json_dict)
    # print('provenance of entity relations: ' + str(ent_rel_prov_list))