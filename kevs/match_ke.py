import pandas as pd
import os
import re
import numpy as np
import networkx as nx

from kevs.qnode_libs import produce_qnode_pair_df, get_bulk_qnode_sim_scores, \
    process_ta2_qnode_fields, process_annotation_qnode_fields


def get_graph_arg_metric_match(edge_df, sim_metric="topsim"):
    sim_metric_trans = "{}_trans".format(sim_metric)
    arg_graph = nx.Graph()
    ta2_args = edge_df['ta2_arg_id'].unique().tolist()
    ann_args = edge_df['ann_arg_id'].unique().tolist()
    arg_graph.add_nodes_from(ta2_args, bipartite=0)
    arg_graph.add_nodes_from(ann_args, bipartite=1)
    num_ta2_args = len(ta2_args)
    num_ann_args = len(ann_args)
    # For this copy, all missing weights have value 0
    # edge_df.fillna(0, inplace=True)
    # There is only a minimum weight, so use 1-metric to get inverse
    arg_graph.add_weighted_edges_from(
        [(row['ta2_arg_id'], row['ann_arg_id'], row[sim_metric_trans])
         for idx, row in edge_df.iterrows()],
        weight=sim_metric_trans)
    # nx.to_pandas_edgelist(ev_graph)
    arg_matching = nx.bipartite.minimum_weight_full_matching(arg_graph, weight=sim_metric_trans)
    # We have the mapping of both directions. Convert to one
    arg_mappings_df = pd.DataFrame(arg_matching.items())
    # Name 1
    # To merge the data frames, we first use the mapping of
    # [0: "ta2_ev_id", 1: "ann_ev_id"] to get the ta2 matches,
    # and then use the second mapping
    # [0: "ann_ev_id", 1: "ta2_ev_id"] to get the annotation matches
    arg_mappings_df.rename(columns={0: "ann_arg_id", 1: "ta2_arg_id"}, inplace=True)
    if num_ta2_args <= num_ann_args:
        arg_match_df = pd.merge(arg_mappings_df.iloc[num_ta2_args:len(arg_mappings_df), :],
                                edge_df[["ta2_arg_id", "ann_arg_id", sim_metric]],
                                on=["ta2_arg_id", "ann_arg_id"],
                                how="inner")
    else:
        arg_mappings_df.rename(columns={"ann_arg_id": "ta2_arg_id", "ta2_arg_id": "ann_arg_id"},
                               inplace=True)
        arg_match_df = pd.merge(arg_mappings_df.iloc[0:num_ta2_args, :],
                                edge_df[["ta2_arg_id", "ann_arg_id", sim_metric]],
                                on=["ta2_arg_id", "ann_arg_id"],
                                how="inner")
    return arg_match_df


def get_graph_metric_match(ev_edge_df, sim_metric="topsim"):
    edge_df = ev_edge_df.copy()
    ev_graph = nx.Graph()
    ta2_ev = edge_df['ta2_ev_id'].unique().tolist()
    ann_ev = edge_df['ann_ev_id'].unique().tolist()
    ev_graph.add_nodes_from(ta2_ev, bipartite=0)
    ev_graph.add_nodes_from(ann_ev, bipartite=1)
    num_ta2_ev = len(ta2_ev)
    num_ann_ev = len(ann_ev)

    # For this copy, all missing weights have value 0
    edge_df.fillna(0, inplace=True)
    # There is only a minimum weight, so use 1-metric to get inverse
    edge_df["sim_metric_trans"] = 1 - edge_df[sim_metric]
    ev_graph.add_weighted_edges_from(
        [(row['ta2_ev_id'], row['ann_ev_id'], row["sim_metric_trans"])
         for idx, row in edge_df.iterrows()],
        weight="sim_metric_trans")

    # nx.to_pandas_edgelist(ev_graph)
    ev_matching = nx.bipartite.minimum_weight_full_matching(ev_graph, weight="sim_metric_trans")
    # We have the mapping of both directions. Convert to one
    ev_mappings_df = pd.DataFrame(ev_matching.items())
    # Name 1
    # To merge the data frames, we first use the mapping of
    # [0: "ta2_ev_id", 1: "ann_ev_id"] to get the ta2 matches,
    # and then use the second mapping
    # [0: "ann_ev_id", 1: "ta2_ev_id"] to get the annotation matches
    # We use the
    ev_mappings_df.rename(columns={0: "ann_ev_id", 1: "ta2_ev_id"}, inplace=True)
    if num_ta2_ev <= num_ann_ev:
        ev_match_df = pd.merge(ev_mappings_df.iloc[num_ta2_ev:len(ev_mappings_df), :],
                               edge_df[["ta2_ev_id", "ann_ev_id", sim_metric]],
                               on=["ta2_ev_id", "ann_ev_id"],
                               how="inner")
    else:
        ev_mappings_df.rename(columns={"ann_ev_id": "ta2_ev_id", "ta2_ev_id": "ann_ev_id"},
                              inplace=True)
        ev_match_df = pd.merge(ev_mappings_df.iloc[0:num_ta2_ev, :],
                               edge_df[["ta2_ev_id", "ann_ev_id", sim_metric]],
                               on=["ta2_ev_id", "ann_ev_id"],
                               how="inner")
    return ev_match_df


def match_graph_ke_elements(ev_edge_df):
    ev_class_df = get_graph_metric_match(ev_edge_df, sim_metric="class")
    ev_complex_df = get_graph_metric_match(ev_edge_df, sim_metric="complex")
    ev_jc_df = get_graph_metric_match(ev_edge_df, sim_metric="jc")
    ev_text_df = get_graph_metric_match(ev_edge_df, sim_metric="text")
    ev_topsim_df = get_graph_metric_match(ev_edge_df, sim_metric="topsim")
    ev_transe_df = get_graph_metric_match(ev_edge_df, sim_metric="transe")
    ev_match_df = pd.merge(ev_class_df, ev_complex_df, on=["ta2_ev_id", "ann_ev_id"],
                           how="outer")
    ev_match_df = pd.merge(ev_match_df, ev_jc_df, on=["ta2_ev_id", "ann_ev_id"],
                           how="outer")
    ev_match_df = pd.merge(ev_match_df, ev_text_df, on=["ta2_ev_id", "ann_ev_id"],
                           how="outer")
    ev_match_df = pd.merge(ev_match_df, ev_topsim_df, on=["ta2_ev_id", "ann_ev_id"],
                           how="outer")
    ev_match_df = pd.merge(ev_match_df, ev_transe_df, on=["ta2_ev_id", "ann_ev_id"],
                           how="outer")
    ev_match_df = pd.merge(ev_match_df,
                           ev_edge_df[["ta2_ev_id", "ann_ev_id", "schema_instance_id",
                                       "ta2_team_name", "ta1_team_name",
                                       "task", "ce_id", "ev_confidence"]],
                           on=["ta2_ev_id", "ann_ev_id"], how="right")
    return ev_match_df


def score_ke_elements(ta2_ceinstance, task_str, ev_match_df):
    # We have an empty match and need to give it a recall, precision of 0

    ke_precision_df = pd.DataFrame([{'schema_instance_id': ta2_ceinstance.schema_instance_id,
                                     'ta1_team_name': ta2_ceinstance.ta1_team_name,
                                     'ta2_team_name': ta2_ceinstance.ta2_team_name,
                                     'task': task_str, 'ce_id': ta2_ceinstance.ce_name,
                                     'precision_class': 0,
                                     'precision_complex': 0, 'precision_jc': 0, 'precision_text': 0,
                                     'precision_topsim': 0, 'precision_transe': 0}])
    ke_recall_df = pd.DataFrame([{'schema_instance_id': ta2_ceinstance.schema_instance_id,
                                  'ta1_team_name': ta2_ceinstance.ta1_team_name,
                                  'ta2_team_name': ta2_ceinstance.ta2_team_name,
                                  'task': task_str, 'ce_id': ta2_ceinstance.ce_name,
                                  'recall_class': 0,
                                  'recall_complex': 0, 'recall_jc': 0, 'recall_text': 0,
                                  'recall_topsim': 0, 'recall_transe': 0}])
    if ev_match_df.shape[0] != 0:
        ke_precision_pivot = pd.pivot_table(ev_match_df,
                                            values=['class', 'complex', 'jc', 'text',
                                                    'topsim', 'transe'],
                                            index=['schema_instance_id', 'ta1_team_name',
                                                   'ta2_team_name', "task", "ce_id", 'ta2_ev_id'],
                                            aggfunc=np.max)

        ke_recall_pivot = pd.pivot_table(ev_match_df,
                                         values=['class', 'complex', 'jc',
                                                 'text', 'topsim', 'transe'],
                                         index=['schema_instance_id', 'ta1_team_name',
                                                'ta2_team_name', "task", "ce_id", 'ann_ev_id'],
                                         aggfunc=np.max)

        ke_precision_df = pd.pivot_table(ke_precision_pivot,
                                         values=['class', 'complex', 'jc',
                                                 'text', 'topsim', 'transe'],
                                         index=['schema_instance_id', 'ta1_team_name',
                                                'ta2_team_name', "task", "ce_id", ],
                                         aggfunc=np.sum).reset_index()
        # Divide by number of TA2 events
        ke_precision_df.loc[:, ['class', 'complex', 'jc', 'text', 'topsim', 'transe']] = \
            ke_precision_df.loc[:, ['class', 'complex', 'jc', 'text',
                                    'topsim', 'transe']] / ev_match_df['ta2_ev_id'].nunique()

        ke_recall_df = pd.pivot_table(ke_recall_pivot,
                                      values=['class', 'complex', 'jc', 'text', 'topsim', 'transe'],
                                      index=['schema_instance_id', 'ta1_team_name',
                                             'ta2_team_name', "task", "ce_id", ],
                                      aggfunc=np.sum).reset_index()

        ke_recall_df.loc[:, ['class', 'complex', 'jc', 'text', 'topsim', 'transe']] = \
            ke_recall_df.loc[:, ['class', 'complex', 'jc',
                                 'text', 'topsim', 'transe']] / ev_match_df['ann_ev_id'].nunique()

        ke_precision_df.rename(columns={'class': 'precision_class', 'complex': 'precision_complex',
                                        'jc': 'precision_jc',
                                        'text': 'precision_text', 'topsim': 'precision_topsim',
                                        'transe': 'precision_transe'},
                               inplace=True)
        ke_recall_df.rename(columns={'class': 'recall_class', 'complex': 'recall_complex',
                                     'jc': 'recall_jc',
                                     'text': 'recall_text', 'topsim': 'recall_topsim',
                                     'transe': 'recall_transe'},
                            inplace=True)
    else:
        print("Warning: Submission Instance " + ta2_ceinstance.schema_instance_id +
              " does not have any instantiated or predicted ta2 events that " +
              "pass automated matching criteria")
    ke_score_df = pd.merge(ke_precision_df, ke_recall_df, how="outer")
    ke_score_df["f1_class"] = 2 * (ke_score_df["precision_class"] *
                                   ke_score_df["recall_class"]) / (
        ke_score_df["precision_class"] +
        ke_score_df["recall_class"])
    ke_score_df["f1_complex"] = 2 * (
        ke_score_df["precision_complex"] * ke_score_df["recall_complex"]) / (
        ke_score_df["precision_complex"] +
        ke_score_df["recall_complex"])
    ke_score_df["f1_jc"] = 2 * (ke_score_df["precision_jc"] * ke_score_df["recall_jc"]) / (
        ke_score_df["precision_jc"] + ke_score_df["recall_jc"])
    ke_score_df["f1_text"] = 2 * (ke_score_df["precision_text"] * ke_score_df["recall_text"]) / (
        ke_score_df["precision_text"] + ke_score_df["recall_text"])
    ke_score_df["f1_topsim"] = 2 * (
        ke_score_df["precision_topsim"] * ke_score_df["recall_topsim"]) / (
        ke_score_df["precision_topsim"] +
        ke_score_df["recall_topsim"])
    ke_score_df["f1_transe"] = 2 * (
        ke_score_df["precision_transe"] * ke_score_df["recall_transe"]) / (
        ke_score_df["precision_transe"] +
        ke_score_df["recall_transe"])
    return ke_score_df


# @profile
def get_ev_arg_sim(qnode_arg_match_df):
    """

    Args:
        qnode_arg_match_df:

    Returns:

    """
    # Make data frame with ta2_ev_id, ann_ev_id, each of the six sim scores

    # now loop over each event
    # If need to speed up, try something else besides a double foor loop
    rows = []
    # arg_match_df = pd.DataFrame(columns = df_columns)
    # qnode_arg_match_df.drop(columns=["q1","q2",'q1_label','q2_label'], inplace=True)
    for ta2_ev in qnode_arg_match_df['ta2_ev_id'].unique().tolist():
        # We do not want to deal with the missing events
        # Which are provided to give the extra arguments that are not linked
        if ta2_ev == 0:
            continue
        for ann_ev in qnode_arg_match_df['ann_ev_id'].unique().tolist():
            # We do not want to deal with the missing events
            # Which are provided to give the extra arguments that are not linked
            if ann_ev == 0:
                continue
            filt_df = qnode_arg_match_df.loc[(qnode_arg_match_df['ta2_ev_id'] == ta2_ev) &
                                             (qnode_arg_match_df['ann_ev_id'] == ann_ev), :]
            filt_df = filt_df.loc[(filt_df['ta2_arg_id'] != 0) & (filt_df['ann_arg_id'] != 0), :]
            ta2_args = \
                qnode_arg_match_df.loc[(qnode_arg_match_df['ta2_ev_id'] == ta2_ev), :][
                    'ta2_arg_id'].unique().tolist()
            num_ta2_args = len(ta2_args)
            ann_arg_df = qnode_arg_match_df.loc[(qnode_arg_match_df['ann_ev_id'] == ann_ev), :]
            ann_args = ann_arg_df['ann_arg_id'].unique().tolist()
            num_ann_args = len(ann_args)
            num_ann_arg_nums = len(ann_arg_df['arg_num'].unique().tolist())
            # If no annotation arguments, give a score of 1
            if num_ann_args == 0:
                row_dict = {'ta2_ev_id': ta2_ev, 'ann_ev_id': ann_ev,
                            'num_ta2_args': num_ta2_args,
                            'num_ann_args': num_ann_args,
                            'num_ann_arg_slots': num_ann_arg_nums,
                            'class_arg': 1.0,
                            'complex_arg': 1.0,
                            'jc_arg': 1.0,
                            'text_arg': 1.0,
                            'topsim_arg': 1.0,
                            'transe_arg': 1.0}
                rows.append(row_dict)
                # arg_match_df = pd.concat([arg_match_df, row_df], ignore_index=True)
                continue
            # Add all 0's if no ta2_arguments instantiated
            elif num_ta2_args == 0:
                row_dict = {'ta2_ev_id': ta2_ev, 'ann_ev_id': ann_ev,
                            'num_ta2_args': num_ta2_args,
                            'num_ann_args': num_ann_args,
                            'num_ann_arg_slots': num_ann_arg_nums,
                            'class_arg': 0.0,
                            'complex_arg': 0.0,
                            'jc_arg': 0.0,
                            'text_arg': 0.0,
                            'topsim_arg': 0.0,
                            'transe_arg': 0.0}
                rows.append(row_dict)
                continue
            # Else, we are here
            filt_df["class_trans"] = 1 - filt_df["class"]
            filt_df["complex_trans"] = 1 - filt_df["complex"]
            filt_df["jc_trans"] = 1 - filt_df["jc"]
            filt_df["text_trans"] = 1 - filt_df["text"]
            filt_df["transe_trans"] = 1 - filt_df["transe"]
            filt_df["topsim_trans"] = 1 - filt_df["topsim"]
            class_df = get_graph_arg_metric_match(filt_df, sim_metric="class")
            complex_df = get_graph_arg_metric_match(filt_df, sim_metric="complex")
            jc_df = get_graph_arg_metric_match(filt_df, sim_metric="jc")
            text_df = get_graph_arg_metric_match(filt_df, sim_metric="text")
            topsim_df = get_graph_arg_metric_match(filt_df, sim_metric="topsim")
            transe_df = get_graph_arg_metric_match(filt_df, sim_metric="transe")
            match_df = pd.merge(class_df, complex_df, on=["ta2_arg_id", "ann_arg_id"],
                                how="outer")
            match_df = pd.merge(match_df, jc_df, on=["ta2_arg_id", "ann_arg_id"],
                                how="outer")
            match_df = pd.merge(match_df, text_df, on=["ta2_arg_id", "ann_arg_id"],
                                how="outer")
            match_df = pd.merge(match_df, topsim_df, on=["ta2_arg_id", "ann_arg_id"],
                                how="outer")
            match_df = pd.merge(match_df, transe_df, on=["ta2_arg_id", "ann_arg_id"],
                                how="outer")
            recall_df = match_df.loc[:, ['class', 'complex', 'jc', 'text', 'topsim',
                                         'transe']].sum()
            # If we have fewer ta2_args than the unique arg, we punish them
            if num_ta2_args <= num_ann_arg_nums:
                recall_df = recall_df / num_ann_arg_nums
            elif num_ann_arg_nums < num_ta2_args <= num_ann_args:
                recall_df = recall_df / num_ta2_args
            # Here the ta2 arguments have extra arguments
            else:
                recall_df = recall_df / num_ann_args

            if num_ta2_args > num_ann_args:
                recall_df = recall_df * (num_ann_args / num_ta2_args)

            row_dict = {'ta2_ev_id': ta2_ev, 'ann_ev_id': ann_ev,
                        'num_ta2_args': num_ta2_args,
                        'num_ann_args': num_ann_args,
                        'num_ann_arg_slots': num_ann_arg_nums,
                        'class_arg': recall_df['class'],
                        'complex_arg': recall_df['complex'],
                        'jc_arg': recall_df['jc'],
                        'text_arg': recall_df['text'],
                        'topsim_arg': recall_df['topsim'],
                        'transe_arg': recall_df['transe']}
            rows.append(row_dict)
            del filt_df, match_df, class_df, complex_df, jc_df, text_df, topsim_df, transe_df
    arg_match_df = pd.DataFrame(rows)
    return arg_match_df


def get_ev_sim_score():
    pass


def match_ke_ce_elements(task1_ceannotation, ta2_ceinstance, graphg_ceinstance,
                         qnode_directory, qnode_sim_fp,
                         is_task2=False,
                         use_graphg=False,
                         min_confidence_threshold=0):
    task_str = "task_1"
    if is_task2:
        task_str = "task_2"

    # Incorporate Graph G First
    if is_task2 and use_graphg:
        pass

    # Start with event matching
    ta2_ev_df = ta2_ceinstance.ev_df
    # Save graph g variable for later graphg_ev_df = graphg_ceinstance.ev_df
    annotation_ev_df = task1_ceannotation.ep_df

    ta2_earg_df = \
        ta2_ceinstance.arg_df.merge(ta2_ceinstance.ent_df.loc[:,
                                    ['ent_id', 'ent_name', 'ent_qnode', 'ent_qlabel',
                                     'ent_TA2qnode', 'ent_TA2qlabel']], left_on='arg_ta2entity',
                                    right_on='ent_id', how="inner")

    ann_argfilt_df = \
        task1_ceannotation.arg_df.loc[pd.notna(task1_ceannotation.arg_df['eventprimitive_id']) &
                                      (task1_ceannotation.arg_df[
                                          'eventprimitive_id'] != "EMPTY_NA") &
                                      (task1_ceannotation.arg_df['entity_id'] != "EMPTY_NA"), :]
    ann_earg_df = ann_argfilt_df.merge(task1_ceannotation.entity_qnode_df,
                                       on="entity_id", how="left")
    ann_earg_df['str'] = "|"
    ann_earg_df['argu_id'] = \
        ann_earg_df['arg_id'] + ann_earg_df['str'] + \
        ann_earg_df['eventprimitive_id'] + + ann_earg_df['str'] + ann_earg_df['arg_num']
    ann_earg_df.drop(columns="str", inplace=True)

    ta2_proc_qnode_df = process_ta2_qnode_fields(ta2_ev_df, id_field_name="ev_id",
                                                 qnode_field_name="ev_ta2qnode",
                                                 qnode_backup_field_name="ev_qnode")
    ta2_earg_proc_qnode_df = process_ta2_qnode_fields(ta2_earg_df, id_field_name="arg_id",
                                                      qnode_field_name="ent_TA2qnode",
                                                      qnode_backup_field_name="ent_qnode")

    ann_proc_qnode_df = process_annotation_qnode_fields(annotation_ev_df,
                                                        id_field_name="eventprimitive_id",
                                                        qnode_field_name="qnode_type_id")

    if ann_earg_df.loc[pd.isna(ann_earg_df["qnode_kb_id_identity"]) |
                       (ann_earg_df["qnode_kb_id_identity"] == "NIL") |
                       (ann_earg_df["qnode_kb_id_identity"] == "EMPTY_NA") |
                       (ann_earg_df["qnode_kb_id_identity"] == "EMPTY_TBD"),
                       "qnode_kb_id_identity"].shape[0] > 0:
        ann_earg_df.loc[pd.isna(ann_earg_df["qnode_kb_id_identity"]) |
                        (ann_earg_df["qnode_kb_id_identity"] == "NIL") |
                        (ann_earg_df["qnode_kb_id_identity"] == "EMPTY_NA") |
                        (ann_earg_df[
                            "qnode_kb_id_identity"] == "EMPTY_TBD"), "qnode_kb_id_identity"] = \
            ann_earg_df.loc[pd.isna(ann_earg_df["qnode_kb_id_identity"]) |
                            (ann_earg_df["qnode_kb_id_identity"] == "NIL") |
                            (ann_earg_df["qnode_kb_id_identity"] == "EMPTY_NA") |
                            (ann_earg_df[
                                "qnode_kb_id_identity"] == "EMPTY_TBD"), "qnode_kb_id_type"]
    # if ann_earg_df.loc[pd.isna(ann_earg_df["qnode_kb_id_identity"]) |
    #                    (ann_earg_df["qnode_kb_id_identity"] == "NIL") |
    #                    (ann_earg_df["qnode_kb_id_identity"] == "EMPTY_NA")|
    #                    (ann_earg_df["qnode_kb_id_identity"] == "EMPTY_TBD"),
    #                    "qnode_kb_id_identity"].shape[0] > 0:
    #     print("Missing Annotation Qnodes")
    #     print(ann_earg_df.loc[pd.isna(ann_earg_df["qnode_kb_id_identity"]) |
    #                           (ann_earg_df["qnode_kb_id_identity"] == "NIL") |
    #                           (ann_earg_df["qnode_kb_id_identity"] == "EMPTY_NA")|
    #                           (ann_earg_df["qnode_kb_id_identity"] == "EMPTY_TBD"),
    #                           ["qnode_kb_id_identity"]])
    ann_earg_proc_qnode_df = \
        process_annotation_qnode_fields(ann_earg_df, id_field_name="argu_id",
                                        qnode_field_name="qnode_kb_id_identity")
    qnode_prod_df = produce_qnode_pair_df(ta2_proc_qnode_df, ann_proc_qnode_df)
    earg_qnode_prod_df = produce_qnode_pair_df(ta2_earg_proc_qnode_df, ann_earg_proc_qnode_df)
    # Query event and argument qnode similarities in the same file
    qnode_prod_df = pd.concat([qnode_prod_df, earg_qnode_prod_df], ignore_index=True)
    qnode_prod_df.drop_duplicates(inplace=True)

    qnode_sim_df = get_bulk_qnode_sim_scores(qnode_prod_df, qnode_directory, qnode_sim_fp)

    ta2_proc_qnode_df.rename(columns={"id": "ta2_ev_id", "qnode_proc": "q1"},
                             inplace=True)
    ann_proc_qnode_df.rename(columns={"id": "ann_ev_id", "qnode_proc": "q2"},
                             inplace=True)

    ta2_earg_proc_qnode_df.rename(columns={"id": "ta2_arg_id", "qnode_proc": "q1"},
                                  inplace=True)
    ann_earg_proc_qnode_df.rename(columns={"id": "ann_arg_id", "qnode_proc": "q2"},
                                  inplace=True)

    # Now, we join with events, and average if there are multiple qnodes
    # Recall that ta2_events are q1, ann_events are q2
    qnode_match_df = pd.merge(ann_proc_qnode_df,
                              qnode_sim_df, on="q2", how="left")
    qnode_match_df = pd.merge(ta2_proc_qnode_df,
                              qnode_match_df, on="q1", how="left")
    qnode_match_df.drop(columns=["q1", "q2", "q1_label", "q2_label"], inplace=True)
    # Average values with multiple qnodes
    qnode_match_df['class'] = pd.to_numeric(qnode_match_df['class'], errors="coerce")
    qnode_match_df['complex'] = pd.to_numeric(qnode_match_df['complex'], errors="coerce")
    qnode_match_df['jc'] = pd.to_numeric(qnode_match_df['jc'], errors="coerce")
    qnode_match_df['text'] = pd.to_numeric(qnode_match_df['text'], errors="coerce")
    qnode_match_df['transe'] = pd.to_numeric(qnode_match_df['transe'], errors="coerce")
    qnode_match_df['topsim'] = pd.to_numeric(qnode_match_df['topsim'], errors="coerce")
    qnode_match_df = qnode_match_df.groupby(['ta2_ev_id', "ann_ev_id"]).mean().reset_index()

    qnode_match_df.fillna(0, inplace=True)
    # qnode_match_df.drop_duplicates(inplace=True)
    qnode_arg_match_df = pd.merge(ann_earg_proc_qnode_df,
                                  qnode_sim_df, on="q2", how="right")
    qnode_arg_match_df = pd.merge(ta2_earg_proc_qnode_df,
                                  qnode_arg_match_df, on="q1", how="right")
    # Need to investigate why we have duplicates
    qnode_arg_match_df.drop_duplicates(inplace=True)
    qnode_arg_match_df = pd.merge(qnode_arg_match_df, ta2_earg_df.loc[:, ["arg_id", "ev_id"]],
                                  how="left", left_on="ta2_arg_id", right_on="arg_id")
    qnode_arg_match_df.drop(columns="arg_id", inplace=True)
    qnode_arg_match_df.rename(columns={"ev_id": "ta2_ev_id"}, inplace=True)
    qnode_arg_match_df = pd.merge(qnode_arg_match_df, ann_earg_df.loc[:, ["argu_id",
                                                                          "eventprimitive_id",
                                                                          "arg_num"]],
                                  how="inner", left_on="ann_arg_id", right_on="argu_id")
    qnode_arg_match_df.drop(columns="argu_id", inplace=True)
    qnode_arg_match_df.rename(columns={"eventprimitive_id": "ann_ev_id"}, inplace=True)
    # Need to investigate why we have duplicates
    qnode_arg_match_df.drop_duplicates(inplace=True)
    qnode_arg_match_df.drop(columns=["q1", "q2", "q1_label", "q2_label"], inplace=True)
    qnode_arg_match_df['class'] = pd.to_numeric(qnode_arg_match_df['class'], errors="coerce")
    qnode_arg_match_df['complex'] = pd.to_numeric(qnode_arg_match_df['complex'], errors="coerce")
    qnode_arg_match_df['jc'] = pd.to_numeric(qnode_arg_match_df['jc'], errors="coerce")
    qnode_arg_match_df['text'] = pd.to_numeric(qnode_arg_match_df['text'], errors="coerce")
    qnode_arg_match_df['transe'] = pd.to_numeric(qnode_arg_match_df['transe'], errors="coerce")
    qnode_arg_match_df['topsim'] = pd.to_numeric(qnode_arg_match_df['topsim'], errors="coerce")
    # We have multiple qnodes in an argument, so we average with them
    qnode_arg_match_df = qnode_arg_match_df.groupby(['ta2_ev_id', "ann_ev_id", "ta2_arg_id",
                                                     "ann_arg_id", "arg_num"]).mean().reset_index()

    # Now that we have this, we can join on ta2 event information and filter by primitive events
    # Or thresholds
    qnode_arg_match_df.fillna(0, inplace=True)

    # We need to filter out to get only the largest set of events we need to save on time
    if not use_graphg:
        qnode_arg_match_df = \
            qnode_arg_match_df.merge(ta2_ev_df.loc[:, ["ev_id", "ev_ta1ref",
                                                       "ev_provenance",
                                                       "ev_prediction_provenance"]],
                                     left_on="ta2_ev_id", right_on="ev_id")
        qnode_arg_match_df = qnode_arg_match_df.loc[
            ((pd.notna(qnode_arg_match_df['ev_provenance'])) |
             (pd.notna(qnode_arg_match_df['ev_prediction_provenance']))) &
            (qnode_arg_match_df['ev_ta1ref'] != "kairos:NULL") &
            (pd.notna(qnode_arg_match_df['ev_ta1ref'])), :]
        qnode_arg_match_df.drop(columns=["ev_id", "ev_ta1ref",
                                         "ev_provenance", "ev_prediction_provenance"], inplace=True)
    ev_arg_sim_df = get_ev_arg_sim(qnode_arg_match_df)

    # Pivot to averge qnodes
    qnode_match_df['class'] = pd.to_numeric(qnode_match_df['class'])
    qnode_match_df['complex'] = pd.to_numeric(qnode_match_df['complex'])
    qnode_match_df['jc'] = pd.to_numeric(qnode_match_df['jc'])
    qnode_match_df['transe'] = pd.to_numeric(qnode_match_df['transe'])
    qnode_match_df['text'] = pd.to_numeric(qnode_match_df['text'])
    qnode_match_df['topsim'] = pd.to_numeric(qnode_match_df['topsim'])
    ev_graph_all_df = pd.pivot_table(qnode_match_df,
                                     values=['class', 'complex', 'jc', 'transe', 'text', 'topsim'],
                                     index=['ta2_ev_id', 'ann_ev_id'], aggfunc=np.mean)

    # Now get metric_ev, metric_arg, and metric (the average of the two, which is what will be
    # used to match)

    # Later, events without a score get a sim score of 0
    ev_graph_all_df.fillna(0, inplace=True)
    ev_graph_all_df = ev_graph_all_df.reset_index()

    # Now rename to ev_metrics, and then join the arg metrics, and then combine to get the regular
    # Metrics
    ev_graph_all_df.rename(columns={'class': 'class_ev', 'complex': 'complex_ev',
                                    'jc': 'jc_ev',
                                    'text': 'text_ev', 'topsim': 'topsim_ev',
                                    'transe': 'transe_ev'}, inplace=True)
    if len(ev_arg_sim_df) > 0:
        ev_graph_all_df = ev_graph_all_df.merge(ev_arg_sim_df,
                                                how="left", on=["ta2_ev_id", "ann_ev_id"])
        ev_graph_all_df.fillna(0, inplace=True)
    else:
        # If there are no instantiated arguments, give an argument score of 0 for all events
        ev_graph_all_df.fillna(0, inplace=True)
        ev_graph_all_df['class_arg'] = 0.0
        ev_graph_all_df['complex_arg'] = 0.0
        ev_graph_all_df['jc_arg'] = 0.0
        ev_graph_all_df['transe_arg'] = 0.0
        ev_graph_all_df['topsim_arg'] = 0.0
        ev_graph_all_df['text_arg'] = 0.0

    weight_ev = 0.5
    weight_arg = 0.5
    ev_graph_all_df['class'] = \
        weight_ev * ev_graph_all_df['class_ev'] + weight_arg * ev_graph_all_df['class_arg']
    ev_graph_all_df['complex'] = \
        weight_ev * ev_graph_all_df['complex_ev'] + weight_arg * ev_graph_all_df['complex_arg']
    ev_graph_all_df['jc'] = \
        weight_ev * ev_graph_all_df['jc_ev'] + weight_arg * ev_graph_all_df['jc_arg']
    ev_graph_all_df['transe'] = \
        weight_ev * ev_graph_all_df['transe_ev'] + weight_arg * ev_graph_all_df['transe_arg']
    ev_graph_all_df['topsim'] = \
        weight_ev * ev_graph_all_df['topsim_ev'] + weight_arg * ev_graph_all_df['topsim_arg']
    ev_graph_all_df['text'] = \
        weight_ev * ev_graph_all_df['topsim_ev'] + weight_arg * ev_graph_all_df['topsim_arg']

    ev_graph_all_df = pd.merge(ev_graph_all_df, ta2_ev_df, how="inner",
                               left_on="ta2_ev_id", right_on="ev_id")
    ev_graph_all_df.drop(['ev_id'], axis=1, inplace=True)
    # For now, ignore confidence values that are lists since we should have a number
    ev_graph_all_df['ev_confidence'] = pd.to_numeric(ev_graph_all_df['ev_confidence'],
                                                     errors='coerce')
    ev_graph_large_df = ev_graph_all_df
    if not use_graphg:
        ev_graph_large_df = \
            ev_graph_all_df.loc[((pd.notna(ev_graph_all_df['ev_provenance'])) |
                                 (pd.notna(ev_graph_all_df['ev_prediction_provenance']))) &
                                (ev_graph_all_df['ev_ta1ref'] != "kairos:NULL") &
                                (pd.notna(ev_graph_all_df['ev_ta1ref'])), :]

    # Now we can filter events how we like
    ev_graph_df = \
        ev_graph_all_df.loc[(ev_graph_all_df['ev_child_list'] == "[]") &
                            (ev_graph_all_df['ev_confidence'] >= min_confidence_threshold) &
                            (ev_graph_all_df['ev_ta1ref'] != "kairos:NULL") &
                            (pd.notna(ev_graph_all_df['ev_ta1ref'])) &
                            ((pd.notna(ev_graph_all_df['ev_provenance'])) |
                             (pd.notna(ev_graph_all_df['ev_prediction_provenance']))), :]

    # We want to filter all of the obligated events out
    if ta2_ceinstance.ta2_team_name == "RESIN":
        ev_graph_df = ev_graph_df.loc[(ev_graph_df['ev_confidence'] > 0.01), :]
    if ta2_ceinstance.ta2_team_name == "CMU":
        ev_graph_df = ev_graph_df.loc[(ev_graph_df['ev_confidence'] > 0.0), :]

    ev_match_df = ev_graph_df
    ev_match_large_df = ev_graph_large_df
    if ev_graph_df.shape[0] > 0:
        ev_match_df = match_graph_ke_elements(ev_graph_df)
    if ev_graph_large_df.shape[0] > 0:
        ev_match_large_df = match_graph_ke_elements(ev_graph_large_df)

    # Now just return the subset of filtered events
    ev_graph_return_df = ev_graph_large_df.loc[:, ["schema_instance_id", "ta2_team_name",
                                                   "ta1_team_name",
                                                   "task", "ce_id", "ta2_ev_id", "ann_ev_id",
                                                   "class", "complex", "jc", "text",
                                                   "topsim", "transe",
                                                   "class_ev", "complex_ev", "jc_ev",
                                                   "text_ev", "topsim_ev", "transe_ev",
                                                   "class_arg", "complex_arg", "jc_arg",
                                                   "text_arg", "topsim_arg", "transe_arg"]]
    ev_match_return_df = ev_match_large_df.loc[:, ["schema_instance_id", "ta2_team_name",
                                                   "ta1_team_name",
                                                   "task", "ce_id", "ta2_ev_id", "ann_ev_id",
                                                   "class", "complex", "jc", "text",
                                                   "topsim", "transe"]]
    ev_match_df.fillna(0, inplace=True)
    ev_match_large_df.fillna(0, inplace=True)

    ke_score_df = score_ke_elements(ta2_ceinstance, task_str, ev_graph_df)
    ke_score_large_df = score_ke_elements(ta2_ceinstance, task_str, ev_graph_large_df)
    ke_score_match_df = score_ke_elements(ta2_ceinstance, task_str, ev_match_df)
    ke_score_match_large_df = score_ke_elements(ta2_ceinstance, task_str, ev_match_large_df)

    return ev_graph_return_df, ev_match_return_df, ke_score_df, \
        ke_score_large_df, ke_score_match_df, ke_score_match_large_df


# @profile


def match_ke_elements(output_dir, annotation_collection, ta2_collection, graph_g_collection,
                      qnode_directory, qnode_sim_fp,
                      is_task2=False, use_graph_g=False,
                      min_confidence_threshold=0):
    """

    Args:
        output_dir:
        annotation_collection:
        ta2_collection:
        graph_g_collection:
        qnode_sim_fp:
        is_task2:
        use_graph_g:
        sim_metric_list: List of similarity metrics. Options for isi metrics are
            ["class", "jc", "complex", "transe", "text", "topsim"]

    Returns:

    """
    task_str = "task_1"
    if is_task2:
        task_str = "task_2"
    ta2_score_df = pd.DataFrame(columns=["schema_instance_id", "ta1_team_name", "ta2_team_name",
                                         "task", "ce_id",
                                         "precision_class", "precision_complex", "precision_jc",
                                         "precision_text", "precision_topsim", "precision_transe",
                                         "recall_class", "recall_complex", "recall_jc",
                                         "recall_text", "recall_topsim", "recall_transe",
                                         "f1_class", "f1_complex", "f1_jc",
                                         "f1_text", "f1_topsim", "f1_transe"
                                         ])
    ta2_score_large_df = pd.DataFrame(columns=["schema_instance_id", "ta1_team_name",
                                               "ta2_team_name",
                                               "task", "ce_id",
                                               "precision_class", "precision_complex",
                                               "precision_jc",
                                               "precision_text", "precision_topsim",
                                               "precision_transe",
                                               "recall_class", "recall_complex", "recall_jc",
                                               "recall_text", "recall_topsim", "recall_transe",
                                               "f1_class", "f1_complex", "f1_jc",
                                               "f1_text", "f1_topsim", "f1_transe"
                                               ])
    ta2_score_match_df = pd.DataFrame(columns=["schema_instance_id", "ta1_team_name",
                                               "ta2_team_name",
                                               "task", "ce_id",
                                               "precision_class", "precision_complex",
                                               "precision_jc",
                                               "precision_text", "precision_topsim",
                                               "precision_transe",
                                               "recall_class", "recall_complex", "recall_jc",
                                               "recall_text", "recall_topsim", "recall_transe",
                                               "f1_class", "f1_complex", "f1_jc",
                                               "f1_text", "f1_topsim", "f1_transe"
                                               ])
    ta2_score_match_large_df = pd.DataFrame(columns=["schema_instance_id", "ta1_team_name",
                                                     "ta2_team_name",
                                                     "task", "ce_id",
                                                     "precision_class", "precision_complex",
                                                     "precision_jc",
                                                     "precision_text", "precision_topsim",
                                                     "precision_transe",
                                                     "recall_class", "recall_complex", "recall_jc",
                                                     "recall_text", "recall_topsim",
                                                     "recall_transe",
                                                     "f1_class", "f1_complex", "f1_jc",
                                                     "f1_text", "f1_topsim", "f1_transe"
                                                     ])

    ta2_team_list = ta2_collection.ta2dict.keys()

    for ta2_team in ta2_team_list:
        ta2_instance = ta2_collection.ta2dict[ta2_team]
        # it is possible by being
        ce_list = pd.Series([key.split('.')[0].split('|')[2].lower()
                             for key in ta2_instance.ta2dict.keys()]).unique().tolist()
        # ce_list = ["ce2104"]
        for ce in ce_list:
            ta2_ce_items = [(key, value) for key, value
                            in ta2_instance.ta2dict.items()
                            if (ce == key.split('.')[0].split('|')[2].lower())]
            # Now get all of the items in a ce
            # As there are multiple instances per (ta1, ta2, ce), we want to cover them all
            for key, value in ta2_ce_items:
                ta2_ceinstance = value
                instance_split = key.split('.')[0].split('|')
                ta2_team_name = ta2_ceinstance.ta2_team_name
                ce_item = instance_split[2].lower()
                if ce != ce_item:
                    print("Inconsistent: Finding ce item {} with loop of ce {}".format(ce_item, ce))
                task1_ce = ce
                # Get the relevant task1 annotaiton from the graphG task 2 event
                if is_task2:
                    task1_ce = ce[0:re.search("[0-9]+", ce).end()]
                task1_ceannotation = annotation_collection.annotation_dict['task1|{}'.format(
                    task1_ce)]
                graph_g_ce_list = [value for key, value
                                   in graph_g_collection.ta2dict['GRAPHG'].ta2dict.items()
                                   if (ce == key.split("|")[2])]
                # If empty, use the exposed version
                if not graph_g_ce_list:
                    graph_g_ce_list = [value for key, value
                                       in graph_g_collection.ta2dict['GRAPHG'].ta2dict.items()
                                       if (ce + "exposed" == key.split("|")[2])]
                # There is only one Graph G instance per event
                if len(graph_g_ce_list) > 1:
                    print("Warning: Multiple Graph G instances of the same complex event")
                    print("Using First of these")
                graph_g_ce = graph_g_ce_list[0]
                base_filename = ta2_ceinstance.ce_instance_file_name_base
                if not os.path.isdir(output_dir):
                    os.makedirs(output_dir)
                if not os.path.isdir(os.path.join(output_dir, ta2_team_name)):
                    os.makedirs(os.path.join(output_dir, ta2_team_name))

                # Check for the files:
                ev_graph_fpath = os.path.join(output_dir, ta2_team_name,
                                              "{}_event_similarities.csv".format(base_filename))
                ev_match_fpath = os.path.join(output_dir, ta2_team_name,
                                              "{}_matched_events.csv".format(base_filename))
                ke_score_fpath = os.path.join(output_dir, ta2_team_name,
                                              "{}_ke_score_df.csv".format(base_filename))
                ke_score_large_fpath = os.path.join(output_dir, ta2_team_name,
                                                    "{}_ke_score_large_df.csv".format(
                                                        base_filename))
                ke_score_match_fpath = os.path.join(output_dir, ta2_team_name,
                                                    "{}_ke_score_match_df.csv".format(
                                                        base_filename))
                ke_score_match_large_fpath = os.path.join(output_dir, ta2_team_name,
                                                          "{}_ke_score_match_large_df.csv".format(
                                                              base_filename))
                if os.path.exists(ev_graph_fpath) and os.path.exists(
                        ev_match_fpath) and os.path.exists(
                    ke_score_fpath) and os.path.exists(ke_score_large_fpath) and \
                        os.path.exists(ke_score_match_fpath) and \
                        os.path.exists(ke_score_match_large_fpath):
                    print("Importing Precomputed Data from {}".format(base_filename))
                    ev_graph_df = pd.read_csv(ev_graph_fpath)
                    ev_match_df = pd.read_csv(ev_match_fpath)
                    ke_score_df = pd.read_csv(ke_score_fpath)
                    ke_score_large_df = pd.read_csv(ke_score_large_fpath)
                    ke_score_match_df = pd.read_csv(ke_score_match_fpath)
                    ke_score_match_large_df = pd.read_csv(ke_score_match_large_fpath)
                else:
                    print("-- Computing Matches for {}".format(base_filename))
                    ev_graph_df, ev_match_df, ke_score_df, ke_score_large_df, \
                        ke_score_match_df, ke_score_match_large_df = \
                        match_ke_ce_elements(
                            task1_ceannotation, ta2_ceinstance,
                            graph_g_ce, is_task2=is_task2,
                            qnode_directory=qnode_directory,
                            qnode_sim_fp=qnode_sim_fp,
                            use_graphg=use_graph_g,
                            min_confidence_threshold=min_confidence_threshold)
                    ev_graph_df.to_csv(ev_graph_fpath, index=False)
                    ev_match_df.to_csv(ev_match_fpath, index=False)
                    ke_score_df.to_csv(ke_score_fpath, index=False)
                    ke_score_large_df.to_csv(ke_score_large_fpath, index=False)
                    ke_score_match_df.to_csv(ke_score_match_fpath, index=False)
                    ke_score_match_large_df.to_csv(ke_score_match_large_fpath, index=False)
                # Add the current score
                ta2_score_df = pd.concat([ta2_score_df, ke_score_df], ignore_index=True)
                ta2_score_large_df = pd.concat(
                    [ta2_score_large_df, ke_score_large_df], ignore_index=True)
                ta2_score_match_df = pd.concat([ta2_score_match_df, ke_score_match_df],
                                               ignore_index=True)
                ta2_score_match_large_df = pd.concat([ta2_score_match_large_df,
                                                      ke_score_match_large_df],
                                                     ignore_index=True)
                del ev_graph_df, ev_match_df, ke_score_df, ke_score_large_df, \
                    ke_score_match_df, ke_score_match_large_df
    # Write scores to file
    ta2_score_df.to_csv(
        os.path.join(output_dir, "TA2_{}_automated_instance_ke_scores.csv".format(task_str)),
        index=False)
    ta2_score_large_df.to_csv(
        os.path.join(output_dir,
                     "TA2_{}_automated_instance_ke_scores_large.csv".format(
                         task_str)),
        index=False)
    ta2_score_match_df.to_csv(
        os.path.join(output_dir,
                     "TA2_{}_automated_instance_matched_ke_scores.csv".format(
                         task_str)),
        index=False)
    ta2_score_match_large_df.to_csv(
        os.path.join(output_dir,
                     "TA2_{}_automated_instance_matched_ke_scores_large.csv".format(
                         task_str)),
        index=False)


# @profile
def get_qnode_sim(output_dir, annotation_collection, ta2_collection, graph_g_collection,
                  qnode_directory, qnode_sim_fp,
                  is_task2=False, use_graph_g=False,
                  min_confidence_threshold=0):
    """

    Args:
        output_dir:
        annotation_collection:
        ta2_collection:
        graph_g_collection:
        qnode_sim_fp:
        is_task2:
        use_graph_g:
        sim_metric_list: List of similarity metrics. Options for isi metrics are
            ["class", "jc", "complex", "transe", "text", "topsim"]

    Returns:

    """
    ta2_team_list = ta2_collection.ta2dict.keys()
    for ta2_team in ta2_team_list:
        ta2_instance = ta2_collection.ta2dict[ta2_team]
        # We go through all of the complex events in the TA2 output if we just loop the elements
        # In our imported collection.
        # As there are multiple instances per (ta1, ta2, ce), we want to cover them all
        for key, value in ta2_instance.ta2dict.items():
            ta2_ceinstance = value
            instance_split = key.split('.')[0].split('|')
            ta2_team_name = ta2_ceinstance.ta2_team_name
            ce = instance_split[2].lower()
            task1_ce = ce
            # Get the relevant task1 annotaiton from the graphG task 2 event
            if is_task2:
                task1_ce = ce[0:re.search("[0-9]+", ce).end()]
            task1_ceannotation = annotation_collection.annotation_dict['task1|{}'.format(task1_ce)]
            graph_g_ce_list = [value for key, value
                               in graph_g_collection.ta2dict['GRAPHG'].ta2dict.items()
                               if (ce == key.split("|")[2])]
            # If empty, use the exposed version
            if not graph_g_ce_list:
                graph_g_ce_list = [value for key, value
                                   in graph_g_collection.ta2dict['GRAPHG'].ta2dict.items()
                                   if (ce + "exposed" == key.split("|")[2])]
            # There is only one Graph G instance per event
            if len(graph_g_ce_list) > 1:
                print("Warning: Multiple Graph G instances of the same complex event")
                print("Using First of these")
            graph_g_ce = graph_g_ce_list[0]
            if not os.path.isdir(output_dir):
                os.makedirs(output_dir)
            if not os.path.isdir(os.path.join(output_dir, ta2_team_name)):
                os.makedirs(os.path.join(output_dir, ta2_team_name))
            ev_graph_df, ev_match_df, ke_score_df, ke_score_large_df, \
                ke_score_match_df, ke_score_match_large_df = \
                match_ke_ce_elements(task1_ceannotation, ta2_ceinstance,
                                     graph_g_ce, is_task2=is_task2,
                                     qnode_directory=qnode_directory,
                                     qnode_sim_fp=qnode_sim_fp,
                                     use_graphg=use_graph_g,
                                     min_confidence_threshold=min_confidence_threshold)
    return
