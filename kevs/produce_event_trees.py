import os
import pandas as pd
import yaml
import ast

from collections import deque


def is_ta2_event_instantiated(ev_id: str, ta2_ce_instance) -> bool:
    """
    As the specifications change, use a method that tells us if a ta2 event is instantiated
    Args:
        ev_id:
        ta2_ce_instance:

    Returns:

    """
    is_ins = False
    ev_df = ta2_ce_instance.ev_df
    if pd.notnull(ev_df.loc[ev_df['ev_id'] == ev_id, 'ev_provenance'].iloc[0]):
        is_ins = True
    return is_ins


def is_ta2_event_predicted(ev_id: str, ta2_ce_instance) -> bool:
    """
    As the specifications change, use a method that tells us if a ta2 event is instantiated
    Args:
        ev_id:
        ta2_ce_instance:

    Returns:

    """
    is_pred = False
    ev_df = ta2_ce_instance.ev_df
    if pd.notnull(ev_df.loc[ev_df['ev_id'] == ev_id, 'ev_prediction_provenance'].iloc[0]):
        is_pred = True
    return is_pred


def is_event_ins_or_pred(ev_id: str, ta2_ce_instance) -> (bool, bool):
    """

    Args:
        ev_id:
        ta2_ce_instance:

    Returns: is_ins, is_pred

    """
    is_ins = is_ta2_event_instantiated(ev_id, ta2_ce_instance)
    is_pred = is_ta2_event_predicted(ev_id, ta2_ce_instance)
    return is_ins, is_pred


def get_ta2_instantiated_events(ev_df: pd.DataFrame) -> pd.DataFrame:
    """
    As the specifications change, use a method to give us the instantiated events
    Args:
        ev_df:

    Returns:

    """
    return ev_df.loc[(ev_df['ev_provenance'] != "") & pd.notna(ev_df['ev_provenance'])
                     & pd.notnull(ev_df['ev_provenance']), ]


def get_ta2_predicted_events(ev_df: pd.DataFrame) -> pd.DataFrame:
    """
    As the specifications change, use a method to give us the predicted events
    Args:
        ev_df:

    Returns:

    """
    return ev_df.loc[(ev_df['ev_prediction_provenance'] != "")
                     & pd.notna(ev_df['ev_prediction_provenance'])
                     & pd.notnull(ev_df['ev_prediction_provenance']), ]


def list_split_function(x):
    xlist = str(x).split(", ")
    xlist = [x.replace("[", "").replace("]", "").replace(" ", "").replace("\n", "").replace("'", "")
             for x in xlist]
    return xlist


def get_ta1_full_name(ev_id: str, ta1_library) -> str:
    """
    Produce the full information we wish to display for each event given its id in the event tree

    Args:
        ev_id:
        ta1_library:

    Returns:

    """
    dforig = ta1_library.ev_df
    if dforig.loc[dforig['ev_id'] == ev_id, 'ev_name'].tolist():
        ev_name_root = dforig.loc[dforig['ev_id'] == ev_id, 'ev_name'].iloc[0]
        ev_id_root = dforig.loc[dforig['ev_id'] == ev_id, 'ev_id'].iloc[0].split('/')[1]
        ev_tree_name_root = '{}({})'.format(ev_name_root, ev_id_root)
        # Add entity names with id
        arg_df = ta1_library.arg_df
        ent_df = ta1_library.ent_df
        arg_sub_df = arg_df.loc[arg_df['ev_id'] == ev_id, :]
        ent_sub_df = ent_df.loc[ent_df['ent_id'].isin(arg_sub_df['arg_entity'].tolist()), :].copy()
        ent_list = []
        if len(ent_sub_df) > 0:
            ent_sub_df['hr_id_part'] = ent_sub_df.loc[:, 'ent_id'].str.split('/', expand=True)[1]
            ent_sub_df['hr_name_str'] = \
                ent_sub_df['ent_name'] + "(" + ent_sub_df['hr_id_part'] + ")"
            ent_list = ent_sub_df['hr_name_str'].tolist()
        ev_tree_name = "{} {}".format(ev_tree_name_root, ent_list)
    else:
        ev_tree_name = "**Event Name missing for event id {}**".format(ev_id)
    return ev_tree_name


def create_ta1_event_tree(ta1_library, output_dir):
    output_file = os.path.join(output_dir, "{}_event_tree.yml".format(ta1_library.ta1_team_name))
    dforig = ta1_library.ev_df.copy(deep=True)
    dfin = ta1_library.ev_df.copy(deep=True)
    dfin['ev_child_list'] = dfin['ev_child_list'].apply(ast.literal_eval)
    dfin = dfin.explode("ev_child_list")[["ev_id", "ev_child_list"]]
    dfin = dfin.loc[pd.notnull(dfin["ev_child_list"]), :]
    # Use all nodes with no parents as roots
    child_list = pd.unique(dfin['ev_child_list']).tolist()
    top_nodes = pd.unique(dforig.loc[~dforig['ev_id'].isin(child_list), 'ev_id']).tolist()
    yaml_tree_list = []
    for root in top_nodes:
        children_to_search = deque([root])
        root_name = get_ta1_full_name(root, ta1_library)
        node2children = {root_name: []}
        searched_list = deque([])
        while len(children_to_search) > 0:
            parent = children_to_search.pop()
            parent_name = get_ta1_full_name(parent, ta1_library)
            searched_list.append(parent)
            child_list = dfin.loc[dfin["ev_id"] == parent, 'ev_child_list'].tolist()
            if len(child_list) > 0 and (child_list != ['']):
                for child in child_list:
                    child_name = get_ta1_full_name(child, ta1_library)
                    if not (child in searched_list):
                        children_to_search.append(child)
                        childnode = {child_name: []}
                        # If child is a leaf node, omit the '[]' for a leaf node
                        if all(dforig.loc[dforig['ev_id'] == child, 'ev_child_list'] == "[]"):
                            childnode = child_name
                        children = node2children[parent_name]
                        children.append(childnode)
                        if all(dforig.loc[dforig['ev_id'] == child, 'ev_child_list'] == "[]"):
                            node2children[child_name] = childnode
                        else:
                            node2children[child_name] = childnode[child_name]
                    else:
                        childnode = {"(Additional link to) {}".format(child_name): []}
                        children = node2children[parent_name]
                        children.append(childnode)
                        node2children["(Additional link to) {}".format(child_name)] = \
                            childnode["(Additional link to) {}".format(child_name)]
        yaml_tree_list.append({root_name: node2children[root_name]})

    with open(output_file, 'w') as outfile:
        for yaml_tree in yaml_tree_list:
            yaml.dump(yaml_tree, outfile, default_flow_style=False, width=80, default_style='"')
    # Read the yaml_file and return it as the tree
    with open(output_file, 'r') as infile:
        yaml_df = yaml.load(infile, Loader=yaml.FullLoader)
    return yaml_df


def get_ta2_full_name(ev_id: str, ta2_ce_instance) -> str:
    """
    Produce the full information we wish to display for each event given its id in the event tree

    Args:
        ev_id:
        ta2_ce_instance:

    Returns:

    """
    dforig = ta2_ce_instance.ev_df
    if dforig.loc[dforig['ev_id'] == ev_id, 'ev_name'].tolist():
        ev_name_root = dforig.loc[dforig['ev_id'] == ev_id, 'ev_name'].iloc[0]
        # if ta2_ce_instance.ta2_team_name == "IBM":
        #    a=2
        ev_id_root = dforig.loc[dforig['ev_id'] == ev_id, 'ev_id'].iloc[0].split("/")[1]
        ev_ins_str = ""
        ev_pred_str = ""
        is_ins, is_pred = is_event_ins_or_pred(ev_id, ta2_ce_instance)
        if is_pred:
            ev_pred_str = "*predicted* "
        if (not is_pred) and (not is_ins):
            ev_ins_str = "|uninstantiated| "
        ev_tree_name_root = "{}{}{}({})".format(ev_pred_str, ev_ins_str, ev_name_root, ev_id_root)
        # Add entity names with id
        arg_df = ta2_ce_instance.arg_df
        ent_df = ta2_ce_instance.ent_df
        arg_sub_df = arg_df.loc[arg_df['ev_id'] == ev_id, :]
        # We only care about instantiated arguments
        ent_ins_sub_df = \
            ent_df.loc[ent_df['ent_id'].isin(arg_sub_df['arg_ta2entity'].tolist()), :].copy()
        ent_list = []
        if (is_ins or is_pred) and len(ent_ins_sub_df) > 0:
            ent_ins_sub_df['hr_id_part'] = \
                ent_ins_sub_df.loc[:, 'ent_id'].str.split("/", expand=True)[1]
            ent_ins_sub_df['hr_name_str'] = \
                ent_ins_sub_df['ent_name'] + "(" + ent_ins_sub_df['hr_id_part'] + ")"
            ent_list = ent_ins_sub_df['hr_name_str'].tolist()
        ev_tree_name = "{} {}".format(ev_tree_name_root, ent_list)
    else:
        ev_tree_name = "**Event Name missing for event id {}**".format(ev_id)
    return ev_tree_name


def get_ta2_nearest_parent(child_id, parent_id, root_id, searched_list, ta2_ce_instance,
                           include_all_events):
    """
    Returns the nearest instantiated event in the ancestor tree, or the
    uninstantiated root_id if no ancestor is instantiated or predicted
    Args:
        child_id:
        parent_id:
        root_id:
        searched_list:
        ta2_ce_instance:
        include_all_events:

    Returns: Event id of nearest instantiated or predicted ancestor, or the root node if nothing


    """
    if include_all_events:
        return parent_id
    dfin = ta2_ce_instance.ev_df.copy(deep=True)
    dfin['ev_child_list'] = dfin['ev_child_list'].apply(ast.literal_eval)
    dfin = dfin.explode("ev_child_list")[["ev_id", "ev_child_list"]]
    if child_id == root_id:
        return root_id
    is_parent_ins, is_parent_pred = is_event_ins_or_pred(parent_id, ta2_ce_instance)
    if is_parent_ins or is_parent_pred:
        return parent_id
    ancestor_id = parent_id
    ancestor_searched_list = []
    ancestors_to_search = dfin.loc[dfin["ev_child_list"] == ancestor_id, "ev_id"].tolist()
    # We only check ancestors that have appeared in this tree already. The tree is depth-first
    # so we know that the root or a valid ancestor has been searched
    ancestors_to_examine = deque([x for x in ancestors_to_search if x in searched_list])
    while len(ancestors_to_examine) > 0:
        ancestor_id = ancestors_to_examine.pop()
        ancestor_searched_list.append(ancestor_id)
        is_ins, is_pred = is_event_ins_or_pred(ancestor_id, ta2_ce_instance)
        if is_ins or is_pred:
            return ancestor_id
        else:
            ancestors_to_add = dfin.loc[dfin["ev_child_list"] == ancestor_id, "ev_id"].tolist()
            for new_ancestor in ancestors_to_add:
                if (new_ancestor in searched_list) and \
                   (not (new_ancestor in ancestor_searched_list)):
                    ancestors_to_examine.append(new_ancestor)
    return root_id


def create_ta2_event_tree(ta2_ce_instance, output_dir, include_all_events):
    if ta2_ce_instance.is_task2:
        output_file = os.path.join(output_dir,
                                   "{}-{}-task2-{}_event_tree.yml".format(
                                       ta2_ce_instance.ta1_team_name,
                                       ta2_ce_instance.ta2_team_name,
                                       ta2_ce_instance.ce_name))
    else:
        output_file = os.path.join(output_dir,
                                   "{}-{}-task1-{}_event_tree.yml".format(
                                       ta2_ce_instance.ta1_team_name,
                                       ta2_ce_instance.ta2_team_name,
                                       ta2_ce_instance.ce_name))

    dforig = ta2_ce_instance.ev_df.copy(deep=True)
    dfin = ta2_ce_instance.ev_df.copy(deep=True)
    dfin['ev_child_list'] = dfin['ev_child_list'].apply(ast.literal_eval)
    dfin = dfin.explode("ev_child_list")[["ev_id", "ev_child_list"]]
    dfin = dfin.loc[pd.notnull(dfin["ev_child_list"]), :]
    # Use all nodes with no parents as roots
    child_list = pd.unique(dfin['ev_child_list']).tolist()
    # Include all root nodes regardless of whether they are instantiated or not
    top_nodes = pd.unique(dforig.loc[~dforig['ev_id'].isin(child_list), 'ev_id']).tolist()
    yaml_tree_list = []
    for root in top_nodes:
        children_to_search = deque([root])
        root_name = get_ta2_full_name(root, ta2_ce_instance)
        node2children = {root_name: []}
        searched_list = deque([])
        while len(children_to_search) > 0:
            parent = children_to_search.pop()
            searched_list.append(parent)
            child_list = dfin.loc[dfin["ev_id"] == parent, 'ev_child_list'].tolist()
            if len(child_list) > 0 and (child_list != ['']):
                for child in child_list:
                    child_name = get_ta2_full_name(child, ta2_ce_instance)
                    is_child_ins, is_child_pred = is_event_ins_or_pred(child, ta2_ce_instance)
                    nearest_parent = None
                    nearest_parent_name = ""
                    if is_child_ins or is_child_pred or include_all_events:
                        nearest_parent = get_ta2_nearest_parent(child, parent, root, searched_list,
                                                                ta2_ce_instance, include_all_events)
                        nearest_parent_name = get_ta2_full_name(nearest_parent, ta2_ce_instance)
                    if not (child in searched_list):
                        children_to_search.append(child)
                        childnode = {child_name: []}
                        # If child is a leaf node, omit the '[]' for a leaf node
                        if all(dforig.loc[dforig['ev_id'] == child, 'ev_child_list'] == "[]"):
                            childnode = child_name
                        if is_child_ins or is_child_pred or include_all_events:
                            children = node2children[nearest_parent_name]
                            children.append(childnode)
                            if all(dforig.loc[dforig['ev_id'] == child, 'ev_child_list'] == "[]"):
                                node2children[child_name] = childnode
                            else:
                                node2children[child_name] = childnode[child_name]
                    else:
                        childnode = {"(Additional link to) {}".format(child_name): []}
                        # Only add if child is instantiated or predicted
                        if is_child_ins or is_child_pred or include_all_events:
                            children = node2children[nearest_parent_name]
                            children.append(childnode)
                            node2children["(Additional link to) {}".format(child_name)] = \
                                childnode["(Additional link to) {}".format(child_name)]
        # If it is just an uninstantiated unpredicted root that has no instantiated
        # or predicted descendants, do not add
        is_root_ins, is_root_pred = is_event_ins_or_pred(root, ta2_ce_instance)
        if is_root_ins or is_root_pred or include_all_events or \
                (node2children[root_name] != []):
            yaml_tree_list.append({root_name: node2children[root_name]})

    with open(output_file, 'w') as outfile:
        for yaml_tree in yaml_tree_list:
            yaml.dump(yaml_tree, outfile, default_flow_style=False, width=80, default_style='"')
    # Read the yaml_file and return it as the tree
    with open(output_file, 'r') as infile:
        yaml_df = yaml.load(infile, Loader=yaml.FullLoader)
    return yaml_df
