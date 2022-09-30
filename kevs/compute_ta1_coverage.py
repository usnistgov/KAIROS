import os
import pandas as pd
import ast
from collections import deque

from kevs.TA2Instantiation import TA2Collection
from kevs.produce_event_trees import get_ta1_full_name


def search_schema_for_ev(ins_ev_list: list, ta1_team: str, ta1_collection: str) -> int:
    ta1_library = ta1_collection.ta1dict[ta1_team]

    team_ev_df = ta1_library.ev_df.copy(deep=True)
    team_ev_child_df = ta1_library.ev_df.copy(deep=True)
    team_ev_child_df['ev_child_list'] = team_ev_child_df['ev_child_list'].apply(ast.literal_eval)
    team_ev_child_df = team_ev_child_df.explode("ev_child_list")[["ev_id", "ev_child_list"]]
    team_ev_child_df = team_ev_child_df.loc[pd.notnull(team_ev_child_df["ev_child_list"]), :]
    child_list = pd.unique(team_ev_child_df['ev_child_list']).tolist()
    top_nodes = pd.unique(team_ev_df.loc[~team_ev_df['ev_id'].isin(child_list), 'ev_id']).tolist()

    num_ev_in_schema = 0
    ev_in_schema = False
    for root in top_nodes:
        for ev in ins_ev_list:
            if ev == root:
                ev_in_schema = True
                break
            children_to_search = deque([root])
            root_name = get_ta1_full_name(root, ta1_library)
            node2children = {root_name: []}
            searched_list = deque([])
            while len(children_to_search) > 0:
                parent = children_to_search.pop()
                if ev == parent:
                    ev_in_schema = True
                    break
                parent_name = get_ta1_full_name(parent, ta1_library)
                searched_list.append(parent)
                child_list = team_ev_child_df.loc[team_ev_child_df["ev_id"] == parent,
                                                  'ev_child_list'].tolist()
                if len(child_list) > 0 and (child_list != ['']):
                    for child in child_list:
                        child_name = get_ta1_full_name(child, ta1_library)
                        if not (child in searched_list):
                            children_to_search.append(child)
                            childnode = {child_name: []}
                            # If child is a leaf node, omit the '[]' for a leaf node
                            if all(team_ev_df.loc[team_ev_df['ev_id'] == child,
                                                  'ev_child_list'] == "[]"):
                                childnode = child_name
                            children = node2children[parent_name]
                            children.append(childnode)
                            if all(team_ev_df.loc[team_ev_df['ev_id'] == child,
                                                  'ev_child_list'] == "[]"):
                                node2children[child_name] = childnode
                            else:
                                node2children[child_name] = childnode[child_name]
                        else:
                            childnode = {"(Additional link to) {}".format(child_name): []}
                            children = node2children[parent_name]
                            children.append(childnode)
                            node2children["(Additional link to) {}".format(child_name)] = \
                                childnode["(Additional link to) {}".format(child_name)]
            if ev_in_schema is True:
                break
        if ev_in_schema is True:
            num_ev_in_schema += 1
    return num_ev_in_schema


def compute_task1_ins_schema(ta2_task1_score_dir: str, task1_annotation_subdir: str,
                             ta1_collection: str, ta1_analysis_dir: str) -> None:
    print("Importing TA2 Collection")
    ta2_task1_collection = TA2Collection(is_task2=False)
    ta2_task1_collection.import_extractions_from_file_collection(ta2_task1_score_dir)

    ta1_team_list = ['CMU', 'IBM', 'ISI', 'RESIN', 'SBU']
    complex_event_list = os.listdir(task1_annotation_subdir)
    complex_event_list.remove('.DS_Store')

    ta2_task1_ins_schema_df = pd.DataFrame(columns=['ta1_team', 'ta2_team', 'ce', 'num_ins_schema'])

    # Notification: This loop consumes few hours
    for ce in complex_event_list:
        ta2_team_list = ta2_task1_collection.ta2dict.keys()
        for ta2_team in ta2_team_list:
            ta2_instance = ta2_task1_collection.ta2dict[ta2_team]
            for ta1_team in ta1_team_list:
                ta2_ce_instance_list = [(key, value) for
                                        key, value in ta2_instance.ta2dict.items() if
                                        ((ta1_team in key.split('|')[0]) and
                                        (ta2_team in key.split('|')[1]) and
                                        (ce == key.split('.')[0].split('|')[2]))]

                for (key, value) in ta2_ce_instance_list:
                    ta2_ceinstance = value
                    ta2_file_name = ta2_ceinstance.ce_instance_file_name_base
                    ta2_task1_ev_df = ta2_ceinstance.ev_df
                    ins_ta2_task1_ev_df = ta2_task1_ev_df.loc[
                        (ta2_task1_ev_df['ev_ta1ref'] != "kairos:NULL")]
                    ins_ev_list = pd.unique(ins_ta2_task1_ev_df['ev_ta1ref']).tolist()

                    num_ins_schema = search_schema_for_ev(ins_ev_list, ta1_team, ta1_collection)
                    print("Computing # instantiated schema for {}: {}".format(
                        ta2_file_name, num_ins_schema))

                    ta2_task1_ins_schema_df.loc[len(ta2_task1_ins_schema_df.index),
                                                ['ta2_file_name', 'ta1_team',
                                                'ta2_team', 'ce', 'num_ins_schema']] = \
                        [ta2_file_name, ta1_team, ta2_team, ce, num_ins_schema]

    ta2_task1_ins_schema_df.to_csv(os.path.join(ta1_analysis_dir,
                                                "TA2_task1_instantiated_schema.csv"), index=False)

    ta2_task1_ins_schema_temp_df = pd.read_csv(os.path.join(ta1_analysis_dir,
                                                            "TA2_task1_instantiated_schema.csv"))

    ta2_task1_ins_schema_result_df = \
        ta2_task1_ins_schema_temp_df.groupby(['ta1_team', 'ta2_team']).agg(
            {'num_ins_schema': ['mean', 'min', 'max']})
    ta2_task1_ins_schema_result_df.columns = ['average', 'min', 'max']
    ta2_task1_ins_schema_result_df = ta2_task1_ins_schema_result_df.reset_index()
    ta2_task1_ins_schema_result_df.to_csv(os.path.join(ta1_analysis_dir,
                                                       "TA2_task1_instantiated_schema_scores.csv"),
                                          index=False)


def compute_ta1_coverage(ta1_score_directory: str, ta1_collection: str,
                         ta1_analysis_dir: str) -> None:

    ta1_coverage_df = pd.DataFrame(columns=['ta1_team', 'num_schemas',
                                            'num_ep_avg', 'num_ep_min', 'num_ep_max'])

    ta1_team_list = os.listdir(ta1_score_directory)
    ta1_team_list.remove('.DS_Store')

    for ta1_team_name in ta1_team_list:
        ta1_library = ta1_collection.ta1dict[ta1_team_name]

        # compute stats for event primitives
        team_ev_df = ta1_library.ev_df.copy(deep=True)
        team_ev_child_df = ta1_library.ev_df.copy(deep=True)
        team_ev_child_df['ev_child_list'] = team_ev_child_df['ev_child_list'].apply(
            ast.literal_eval)
        team_ev_child_df = team_ev_child_df.explode("ev_child_list")[["ev_id", "ev_child_list"]]
        team_ev_child_df = team_ev_child_df.loc[pd.notnull(team_ev_child_df["ev_child_list"]), :]
        child_list = pd.unique(team_ev_child_df['ev_child_list']).tolist()
        top_nodes = pd.unique(
            team_ev_df.loc[~team_ev_df['ev_id'].isin(child_list), 'ev_id']).tolist()

        num_schemas = len(top_nodes)
        num_ep_min = 200
        num_ep_max = 0
        num_ep_all = 0
        for root in top_nodes:
            num_ep_count = 0
            children_to_search = deque([root])
            root_name = get_ta1_full_name(root, ta1_library)
            node2children = {root_name: []}
            searched_list = deque([])
            while len(children_to_search) > 0:
                num_ep_count += 1
                parent = children_to_search.pop()
                parent_name = get_ta1_full_name(parent, ta1_library)
                searched_list.append(parent)
                child_list = team_ev_child_df.loc[team_ev_child_df["ev_id"]
                                                  == parent, 'ev_child_list'].tolist()
                if len(child_list) > 0 and (child_list != ['']):
                    for child in child_list:
                        child_name = get_ta1_full_name(child, ta1_library)
                        if not (child in searched_list):
                            children_to_search.append(child)
                            childnode = {child_name: []}
                            # If child is a leaf node, omit the '[]' for a leaf node
                            if all(team_ev_df.loc[team_ev_df['ev_id'] == child,
                                                  'ev_child_list'] == "[]"):
                                childnode = child_name
                            children = node2children[parent_name]
                            children.append(childnode)
                            if all(team_ev_df.loc[team_ev_df['ev_id'] == child,
                                                  'ev_child_list'] == "[]"):
                                node2children[child_name] = childnode
                            else:
                                node2children[child_name] = childnode[child_name]
                        else:
                            childnode = {"(Additional link to) {}".format(child_name): []}
                            children = node2children[parent_name]
                            children.append(childnode)
                            node2children["(Additional link to) {}".format(child_name)] = \
                                childnode["(Additional link to) {}".format(child_name)]
            num_ep_all += num_ep_count
            if num_ep_count >= num_ep_max:
                num_ep_max = num_ep_count
            if num_ep_count <= num_ep_min:
                num_ep_min = num_ep_count
        num_ep_avg = round(num_ep_all / num_schemas, 1)
        ta1_coverage_df.loc[len(ta1_coverage_df.index),
                            ['ta1_team', 'num_schemas', 'num_ep_avg',
                                'num_ep_min', 'num_ep_max']] = \
            [ta1_team_name, num_schemas, num_ep_avg, num_ep_min, num_ep_max]

    ta1_coverage_df.to_csv(os.path.join(ta1_analysis_dir, "TA1_coverage_stats.csv"), index=False)

# # compute stats for relations
# team_rel_df = ta1_library.rel_df.copy(deep=True)
# num_rel_all = len(team_rel_df)
# num_rel_avg = round(num_rel_all / num_schemas, 1)
# print("# relations: {}, avg: {}".format(num_rel_all, num_rel_avg))

# # compute stats for num_orders
# team_temporalrel_df = ta1_library.temporalrel_df.copy(deep=True)
# num_temporalrel_all = len(team_temporalrel_df)
# num_temporalrel_avg = round(num_temporalrel_all / num_schemas, 1)
# print("# temprel: {}, avg: {}".format(num_temporalrel_all, num_temporalrel_avg))
