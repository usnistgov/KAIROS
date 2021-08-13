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
# analyze the order related information, e.g., size and keywords
######################################################################################
from scripts import load_data as load, duplication as dup


# get order keywords
def get_order_keywords(json_dict: dict):
    keyword_list = []

    for file_name in json_dict:
        json_data = json_dict[file_name]
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                if 'order' in schema.keys():
                    order_list = schema['order']
                    if order_list:
                        for order in order_list:
                            for order_key in order:
                                # if order_key == 'flag':
                                #     print('wow')
                                if order_key not in keyword_list:
                                    keyword_list.append(order_key)

    return keyword_list


# get order_flags(flag) keywords
def get_order_flags_keywords(json_dict: dict):
    keyword_list = []

    for file_name in json_dict:
        json_data = json_dict[file_name]
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                if 'order' in schema.keys():
                    order_list = schema['order']
                    if order_list:
                        for order in order_list:
                            if 'flag' in order.keys():
                                flag = order['flag']
                                if flag not in keyword_list:
                                    keyword_list.append(flag)
                            elif 'flags' in order.keys():
                                flags = order['flags']
                                if flags not in keyword_list:
                                    keyword_list.append(flags)

    return keyword_list


# check whether an order has confidence
def is_order_with_confidence(order: dict):
    if 'confidence' in order.keys():
        confidence = float(order['confidence'])
        if confidence > 0:
            return True

    return False


# get all orders without checking duplication and confidence
def get_all_orders(json_dict: dict):
    result = {}

    for file_name in json_dict:
        json_data = json_dict[file_name]
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                if 'order' in schema.keys():
                    order_list = schema['order']
                    if order_list:
                        for i in range(0, len(order_list)):
                            order = order_list[i]
                            order_id = file_name + '_order_' + str(i)
                            temp_dict = {order_id: order}
                            result.update(temp_dict)

    return result


# get all orders with hard-duplication check
def get_unique_orders(json_dict: dict):
    result = {}

    for file_name in json_dict:
        json_data = json_dict[file_name]
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                if 'order' in schema.keys():
                    order_list = schema['order']
                    if order_list:
                        for i in range(0, len(order_list)):
                            order = order_list[i]
                            if not dup.is_hard_duplicated(file_name, order, result):
                                order_id = file_name + '_order_' + str(i)
                                temp_dict = {order_id: order}
                                result.update(temp_dict)

    return result


# get all unique orders with confidence, confidence 0 is excluded
def get_unique_orders_with_confidence(json_dict: dict):
    result = {}

    for file_name in json_dict:
        json_data = json_dict[file_name]
        schema_list = json_data['schemas']
        if schema_list:
            for schema in schema_list:
                if 'order' in schema.keys():
                    order_list = schema['order']
                    if order_list:
                        for i in range(0, len(order_list)):
                            order = order_list[i]
                            if is_order_with_confidence(order):
                                if not dup.is_hard_duplicated(file_name, order, result):
                                    order_id = file_name + '_order_' + str(i)
                                    temp_dict = {order_id: order}
                                    result.update(temp_dict)

    return result


if __name__ == "__main__":
    ta2_team_name = ['RESIN']
    ta2_output_directory = '/Users/xnj1/OneDrive - National Institute of Standards and Technology (NIST)/KAIROS/Quizlet_4/TA2_outputs/'
    ta1_team_name = ['ALL']
    ta1_output_directory = '/Users/xnj1/OneDrive - National Institute of Standards and Technology (NIST)/KAIROS/Quizlet_4/TA1_outputs/'
    json_dict = load.load_json_directory(ta1_team_name, ta1_output_directory)
    print('keywords of order are: ' + str(get_order_keywords(json_dict)))
    print('keywords of order_flags are: ' + str(get_order_flags_keywords(json_dict)))
    print('size of all orders: ' + str(len(get_all_orders(json_dict))))
    print('size of unique orders: ' + str(len(get_unique_orders(json_dict))))
    print('size of unique orders with confidence: ' + str(len(get_unique_orders_with_confidence(json_dict))))
