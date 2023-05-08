import os
import numpy as np
import pandas as pd
import typing
from kevs.TA1Library import TA1Collection
from kevs.TA2Instantiation import TA2Collection
from kevs.produce_event_trees import get_ta2_instantiated_events, get_ta2_predicted_events


def initiate_ta1_stats_dataframe() -> pd.DataFrame:
    ta1_stats_df = pd.DataFrame(columns=['file_name', 'ta1_team_name', 'schema_id',
                                         'ev_count', 'ev_primitive_count', 'ev_wd_node_count',
                                         'avg_wd_node_ev', 'ev_distinct_wd_node_count',
                                         'ev_wd_label_count',
                                         'ev_ta1explanation_count',
                                         'arg_count', 'avg_arg_per_ev', 'child_count',
                                         'avg_child_per_ev', 'child_outlinks_count',
                                         'ent_count', 'avg_arg_per_ent',
                                         'rel_count', 'rel_distinct_subject_count',
                                         'temporalrel_count',
                                         'ent_coref_count', 'frac_ent_coref'])

    return ta1_stats_df


def initiate_ta2_stats_dataframe() -> pd.DataFrame:
    ta2_stats_df = pd.DataFrame(columns=['file_name', 'ta2_team_name', 'ta1_team_name',
                                         'schema_instance_id', 'schema_id',
                                         'ev_count', 'ev_primitive_count', 'ev_wd_node_count',
                                         'avg_wd_node_ev', 'ev_ta1ref_count',
                                         'ev_instantiated_count',
                                         'ev_distinct_wd_node_count', 'ev_wd_label_count',
                                         'ev_predicted_count',
                                         'ev_instantiated_ta1ref_count',
                                         'ev_predicted_ta1ref_count',
                                         'arg_count', 'avg_arg_per_ev', 'pred_arg_count',
                                         'ins_arg_count', 'ins_pred_arg_count',
                                         'ins_arg_in_ins_ev_count',
                                         'frac_arg_ins',
                                         'ev_top_level_count',
                                         'ev_with_child_count',
                                         'ev_leaf_count',
                                         'child_count', 'avg_child_per_ev',
                                         'avg_hier_depth', 'max_hier_depth',
                                         'ent_count', 'avg_arg_per_ent', 'pred_ent_count',
                                         'ins_ent_count', 'frac_ent_ins',
                                         'ins_pred_ent_count',
                                         'rel_count', 'rel_distinct_subject_count',
                                         'temporalrel_count',
                                         'ent_coref_count', 'ins_ent_coref_count',
                                         'ent_predicted_ref_count',
                                         'ent_predicted_and_non_coref_count',
                                         'ins_ent_predicted_ref_count',
                                         'ins_ent_predicted_and_non_coref_count',
                                         'frac_ent_coref', 'frac_ins_ent_coref'])

    return ta2_stats_df


def initiate_ta1_ent_ev_dataframe() -> pd.DataFrame:
    ta1_ent_ev_df = pd.DataFrame(columns=['file_name', 'ta1_team_name',
                                          'ent_id', 'ent_name', 'ent_wd_node',
                                          'ent_wd_label', 'num_events', 'num_non_predicted_events',
                                          'num_predicted_events'])

    return ta1_ent_ev_df


def initiate_ta2_ent_ev_dataframe() -> pd.DataFrame:
    ta2_ent_ev_df = pd.DataFrame(columns=['file_name', 'ta2_team_name', 'ta1_team_name',
                                          'schema_instance_id',
                                          'ent_id', 'ent_name', 'ent_wd_node',
                                          'ent_wd_label', 'ent_ta2wd_node', 'ent_ta2wd_label',
                                          'num_events', 'num_non_predicted_events',
                                          'num_predicted_events'])

    return ta2_ent_ev_df


def initiate_wd_node_dataframe() -> pd.DataFrame:
    wd_node_df = pd.DataFrame(columns=['file_name', 'ta2_team_name', 'ta1_team_name',
                                       'schema_id', 'wd_node', 'wd_label', 'count'])

    return wd_node_df


def get_schema_ev_wd_node_dict(schema_ev_wd_node_df: pd.DataFrame) -> dict:
    schema_ev_wd_node_dict = {}
    if len(schema_ev_wd_node_df) == 0:
        return schema_ev_wd_node_dict

    for schema_ev_row in schema_ev_wd_node_df.itertuples():
        ev_wd_node = schema_ev_row.ev_wd_node
        ev_wd_label = schema_ev_row.ev_wd_label
        if ev_wd_node not in schema_ev_wd_node_dict.keys():
            schema_ev_wd_node_dict[ev_wd_node] = [ev_wd_label, 1]
        else:
            count = schema_ev_wd_node_dict[ev_wd_node][1]
            schema_ev_wd_node_dict[ev_wd_node] = [ev_wd_label, count+1]

    return schema_ev_wd_node_dict


def get_wd_node_dataframe(file_name: str, ta2_team_name: str, ta1_team_name: str, schema_id: str,
                          schema_ev_wd_node_dict: dict) -> pd.DataFrame:
    wd_node_df = initiate_wd_node_dataframe()

    for key, value in schema_ev_wd_node_dict.items():
        wd_label = value[0]
        count = value[1]
        wd_node_row = {'file_name': file_name, 'ta2_team_name': ta2_team_name,
                       'ta1_team_name': ta1_team_name,
                       'schema_id': schema_id, 'wd_node': key,
                       'wd_label': wd_label, 'count': count}
        wd_node_df = pd.concat([wd_node_df, pd.DataFrame([wd_node_row])],
                               ignore_index=True)

    return wd_node_df


# TO DO: FIx this method
def get_ta2_predicted_arg_count(ev_instantiated_df: pd.DataFrame) -> int:
    predicted_arg_count = 0

    for ev_row in ev_instantiated_df.itertuples():
        predicted_arg = ev_row.ev_predicted_participant_list
        if len(predicted_arg) > 0:
            predicted_arg_count += len(predicted_arg)

    return predicted_arg_count


def update_ta1_team_stats(team_stats_df: pd.DataFrame, team_wd_node_df: pd.DataFrame,
                          file_name: str, schema_id: str, ta1_team_name: str,
                          team_ev_df: pd.DataFrame,
                          team_arg_df: pd.DataFrame, team_child_df: pd.DataFrame,
                          team_ent_df: pd.DataFrame, team_rel_df: pd.DataFrame,
                          team_temporalrel_df: pd.DataFrame,
                          team_ent_ev_df: pd.DataFrame):
    # compute event stats
    ev_count = ev_primitive_count = ev_wd_node_count = ev_distinct_wd_node_count = 0
    ev_wd_label_count = 0
    ev_ta1explanation_count = 0
    avg_wd_node_ev = avg_arg_per_ev = avg_child_per_ev = frac_ent_coref = avg_arg_per_ent = np.nan
    # calculate
    if len(team_ev_df) > 0:
        ev_df = team_ev_df.loc[team_ev_df['schema_id'] == schema_id]
        ev_count = len(ev_df)
        ev_primitive_count = len(ev_df.loc[ev_df['ev_child_list'] == "[]", :])
        ev_wd_node_df = ev_df.loc[ev_df['ev_wd_node'].notnull()]
        ev_wd_node_count = len(ev_wd_node_df)
        ev_wd_node_dict = get_schema_ev_wd_node_dict(ev_wd_node_df)
        ev_distinct_wd_node_count = len(ev_wd_node_dict)
        wd_node_df = get_wd_node_dataframe(file_name, '', ta1_team_name, schema_id, ev_wd_node_dict)
        ev_wd_label_df = ev_df.loc[ev_df['ev_wd_label'].notnull()]
        ev_wd_label_count = len(ev_wd_label_df)
        ev_ta1explanation_df = ev_df.loc[ev_df['ev_ta1explanation'].notnull()]
        ev_ta1explanation_count = len(ev_ta1explanation_df)
        avg_wd_node_ev = ev_wd_node_count / ev_count
    # compute argument stats
    arg_count = 0
    # calculate
    if len(team_arg_df) > 0:
        arg_df = team_arg_df.loc[team_arg_df['schema_id'] == schema_id]
        arg_count = len(arg_df)
        if ev_count > 0:
            avg_arg_per_ev = arg_count/ev_count
    # compute child stats
    child_count = child_outlinks_count = 0
    # calculate
    if len(team_child_df) > 0:
        child_df = team_child_df.loc[team_child_df['schema_id'] == schema_id]
        child_count = len(child_df)
        child_outlinks_df = child_df.loc[child_df['child_outlinks'].notnull()]
        child_outlinks_count = len(child_outlinks_df)
        if ev_count > 0:
            avg_child_per_ev = child_count/ev_count
    # compute entity stats
    ent_count = 0
    if len(team_ent_df) > 0:
        ent_df = team_ent_df.loc[team_ent_df['schema_id'] == schema_id]
        ent_count = len(ent_df)
        avg_arg_per_ent = arg_count/ent_count
    # compute relation stats
    rel_count = rel_distinct_subject_count = 0
    if len(team_rel_df) > 0:
        rel_df = team_rel_df.loc[team_rel_df['schema_id'] == schema_id]
        rel_count = len(rel_df)
        rel_subject_df = rel_df.loc[rel_df['rel_subject'].notnull()]
        if len(rel_subject_df) == 0:
            rel_subject_df = rel_df.loc[rel_df['rel_relationSubject'].notnull()]
            rel_distinct_subject_count = len(rel_subject_df.rel_relationSubject.unique())
        else:
            rel_distinct_subject_count = len(rel_subject_df.rel_subject.unique())
    temporalrel_count = 0
    if len(team_temporalrel_df) > 0:
        temporalrel_df = team_temporalrel_df.loc[team_temporalrel_df['schema_id'] == schema_id]
        temporalrel_count = len(temporalrel_df)
    # combine as a row (dict)
    ent_coref_count = len(team_ent_ev_df.loc[team_ent_ev_df['num_events'] >= 2, :])
    if ent_count != 0:
        frac_ent_coref = ent_coref_count/ent_count
    team_stats_row = {
        'file_name': file_name, 'ta1_team_name': ta1_team_name, 'schema_id': schema_id,
        'ev_count': ev_count, 'ev_primitive_count': ev_primitive_count,
        'ev_wd_node_count': ev_wd_node_count, 'avg_wd_node_ev': avg_wd_node_ev,
        'ev_distinct_wd_node_count': ev_distinct_wd_node_count,
        'ev_wd_label_count': ev_wd_label_count,
        'ev_ta1explanation_count': ev_ta1explanation_count,
        'arg_count': arg_count, 'avg_arg_per_ev': avg_arg_per_ev,
        'child_count': child_count, 'avg_child_per_ev': avg_child_per_ev,
        'child_outlinks_count': child_outlinks_count,
        'ent_count': ent_count, 'avg_arg_per_ent': avg_arg_per_ent,
        'rel_count': rel_count, 'rel_distinct_subject_count': rel_distinct_subject_count,
        'temporalrel_count': temporalrel_count,
        'ent_coref_count': ent_coref_count, 'frac_ent_coref': frac_ent_coref
    }
    # add to team stats
    team_stats_df = pd.concat([team_stats_df, pd.DataFrame([team_stats_row])], ignore_index=True)
    team_wd_node_df = pd.concat([team_wd_node_df, wd_node_df], ignore_index=True)

    return team_stats_df, team_wd_node_df


def link_ta1_entities_with_events(team_ent_ev_df: pd.DataFrame, team_wd_node_df: pd.DataFrame,
                                  file_name: str, schema_id: str, team_ev_df: pd.DataFrame,
                                  team_arg_df: pd.DataFrame, team_child_df: pd.DataFrame,
                                  team_ent_df: pd.DataFrame, team_rel_df: pd.DataFrame,
                                  team_temporalrel_df: pd.DataFrame,
                                  ta1_team_name: str):
    ev_df = team_ev_df.loc[team_ev_df['schema_id'] == schema_id, :]
    arg_df = team_arg_df.loc[team_arg_df['schema_id'] == schema_id]
    ent_df = team_ent_df.loc[team_ent_df['schema_id'] == schema_id]
    # All arguments with all entities
    join_df = pd.merge(ev_df.loc[:, ['ev_id', 'ev_name', 'ev_wd_node', 'ev_wd_label']],
                       arg_df, on='ev_id', how="inner")
    join_df = pd.merge(join_df.loc[:, ['ev_id', 'ev_name', 'schema_id',
                                       'arg_id', 'arg_entity']],
                       ent_df.loc[:, ['ent_id', 'ent_name', 'ent_wd_node', 'ent_wd_label']],
                       left_on='arg_entity', right_on='ent_id', how="inner")
    join_df[['ent_wd_node', 'ent_wd_label']] = join_df[['ent_wd_node', 'ent_wd_label']].fillna('')
    ent_ev_df = join_df.groupby(['schema_id', 'ent_id', 'ent_name',
                                 'ent_wd_node',
                                 'ent_wd_label']).size().reset_index(name='num_events')

    ent_ev_df = ent_ev_df.fillna(0)
    ent_ev_df['file_name'] = file_name
    ent_ev_df.sort_values(by=['schema_id', 'num_events'], ascending=False, inplace=True)
    ent_ev_df.insert(ent_ev_df.shape[1], 'ta1_team_name', ta1_team_name)
    cols_order = ['file_name', 'ta1_team_name', 'schema_id', 'ent_id', 'ent_name', 'ent_wd_node',
                  'ent_wd_label', 'num_events', ]
    ent_ev_df = ent_ev_df[cols_order]
    team_ent_ev_df = pd.concat([team_ent_ev_df, ent_ev_df], ignore_index=True)

    return team_ent_ev_df


def link_ta2_entities_with_events(team_ent_ev_df: pd.DataFrame, team_ins_ent_ev_df: pd.DataFrame,
                                  team_wd_node_df: pd.DataFrame,
                                  file_name: str, schema_id: str, ta2_team_name: str,
                                  team_ev_df: pd.DataFrame,
                                  team_arg_df: pd.DataFrame, team_child_df: pd.DataFrame,
                                  team_ent_df: pd.DataFrame, team_rel_df: pd.DataFrame,
                                  team_temporalrel_df: pd.DataFrame,
                                  schema_instance_id: str, ta1_team_name: str):
    ev_df = team_ev_df.loc[team_ev_df['schema_instance_id'] == schema_instance_id, :]
    ev_predicted_df = get_ta2_predicted_events(ev_df)
    ev_df.insert(ev_df.shape[1], 'predicted_bool',
                 ev_df.loc[:, 'ev_id'].isin(ev_predicted_df['ev_id']))
    arg_df = team_arg_df.loc[team_arg_df['schema_instance_id'] == schema_instance_id]
    ent_df = team_ent_df.loc[team_ent_df['schema_instance_id'] == schema_instance_id]
    # All arguments with all entities
    join_df = pd.merge(ev_df.loc[:, ['ev_id', 'ev_name', 'ev_wd_node', 'ev_wd_label',
                                     'ev_ta1ref', 'predicted_bool']], arg_df, on='ev_id',
                       how="inner")
    join_df = pd.merge(join_df.loc[:, ['ev_id', 'ev_name', 'schema_instance_id', 'arg_id',
                                       'arg_entity', 'predicted_bool']],
                       ent_df.loc[:, ['ent_id', 'ent_name', 'ent_wd_node', 'ent_wd_label',
                                      'ent_ta2wd_node', 'ent_ta2wd_label']], left_on='arg_entity',
                       right_on='ent_id', how="inner")
    join_df.loc[:, ['ent_ta2wd_node', 'ent_ta2wd_label']] = \
        join_df.loc[:, ['ent_ta2wd_node', 'ent_ta2wd_label']].fillna("")
    join_df[['ent_wd_node', 'ent_wd_label']] = join_df[['ent_wd_node', 'ent_wd_label']].fillna('')
    ent_ev_predicted_df = join_df.groupby(['schema_instance_id', 'ent_id', 'predicted_bool',
                                           'ent_name', 'ent_wd_node',
                                           'ent_wd_label', 'ent_ta2wd_node',
                                           'ent_ta2wd_label']).size().reset_index(name='num_events')
    ent_ev_df = ent_ev_predicted_df.pivot_table(index=['schema_instance_id', 'ent_id',
                                                       'ent_name', 'ent_wd_node', 'ent_wd_label',
                                                       'ent_ta2wd_node', 'ent_ta2wd_label'],
                                                columns='predicted_bool', values='num_events').\
        reset_index().rename(columns={False: 'num_non_predicted_events',
                                      True: 'num_predicted_events'})
    ent_ev_df = ent_ev_df.fillna(0)
    ent_ev_df['file_name'] = file_name
    ent_ev_df['ta2_team_name'] = ta2_team_name
    if 'num_predicted_events' not in ent_ev_df.columns:
        ent_ev_df['num_predicted_events'] = 0
    if 'num_non_predicted_events' not in ent_ev_df.columns:
        ent_ev_df['num_non_predicted_events'] = 0
    ent_ev_df['num_events'] = \
        ent_ev_df['num_non_predicted_events'] + ent_ev_df['num_predicted_events']
    ent_ev_df.sort_values(by=['schema_instance_id', 'num_events'], ascending=False, inplace=True)
    ent_ev_df.insert(ent_ev_df.shape[1], 'ta1_team_name', ta1_team_name)
    cols_order = ['file_name', 'ta2_team_name', 'ta1_team_name', 'schema_instance_id', 'ent_id',
                  'ent_name', 'ent_wd_node', 'ent_wd_label', 'ent_ta2wd_node', 'ent_ta2wd_label',
                  'num_events', 'num_non_predicted_events', 'num_predicted_events', ]
    ent_ev_df = ent_ev_df[cols_order]
    team_ent_ev_df = pd.concat([team_ent_ev_df, ent_ev_df], ignore_index=True)

    # instantiated entities and events only
    ins_join_df = pd.merge(ev_df.loc[:, ['ev_id', 'ev_name', 'ev_wd_node', 'ev_wd_label',
                                         'ev_ta1ref', 'predicted_bool']], arg_df, on='ev_id',
                           how="inner")
    if pd.notnull(arg_df['arg_ta2entity']).any():
        ins_join_df = pd.merge(ins_join_df.loc[:, ['ev_id', 'ev_name', 'schema_instance_id',
                                                   'arg_id', 'arg_ta2entity', 'predicted_bool']],
                               ent_df.loc[:, ['ent_id', 'ent_name', 'ent_wd_node', 'ent_wd_label',
                                              'ent_ta2wd_node', 'ent_ta2wd_label']],
                               left_on='arg_ta2entity', right_on='ent_id', how="inner")
        ins_join_df[['ent_wd_node', 'ent_wd_label']] = ins_join_df[['ent_wd_node',
                                                                    'ent_wd_label']].fillna('')
        ins_ent_ev_predicted_df = ins_join_df.groupby(['schema_instance_id', 'ent_id',
                                                       'predicted_bool', 'ent_name', 'ent_wd_node',
                                                       'ent_wd_label', 'ent_ta2wd_node',
                                                       'ent_ta2wd_label']).size().\
            reset_index(name='num_events')
        ins_ent_ev_df = ins_ent_ev_predicted_df.pivot_table(index=['schema_instance_id', 'ent_id',
                                                                   'ent_name', 'ent_wd_node',
                                                                   'ent_wd_label', 'ent_ta2wd_node',
                                                                   'ent_ta2wd_label'],
                                                            columns='predicted_bool',
                                                            values='num_events').\
            reset_index().rename(columns={False: 'num_non_predicted_events',
                                          True: 'num_predicted_events'})
        ins_ent_ev_df = ins_ent_ev_df.fillna(0)
        ins_ent_ev_df['file_name'] = file_name
        ins_ent_ev_df['ta2_team_name'] = ta2_team_name
        if 'num_predicted_events' not in ins_ent_ev_df.columns:
            ins_ent_ev_df['num_predicted_events'] = 0
        if 'num_non_predicted_events' not in ins_ent_ev_df.columns:
            ins_ent_ev_df['num_non_predicted_events'] = 0
        ins_ent_ev_df['num_events'] = \
            ins_ent_ev_df['num_non_predicted_events'] + ins_ent_ev_df['num_predicted_events']
        ins_ent_ev_df.sort_values(by=['schema_instance_id', 'num_events'], ascending=False,
                                  inplace=True)
        ins_ent_ev_df.insert(ins_ent_ev_df.shape[1], 'ta1_team_name', ta1_team_name)
        cols_order = ['file_name', 'ta2_team_name', 'ta1_team_name', 'schema_instance_id',
                      'ent_id', 'ent_name', 'ent_wd_node', 'ent_wd_label',
                      'ent_ta2wd_node', 'ent_ta2wd_label', 'num_events', 'num_non_predicted_events',
                      'num_predicted_events', ]
        ins_ent_ev_df = ins_ent_ev_df[cols_order]
        team_ins_ent_ev_df = pd.concat([team_ins_ent_ev_df, ins_ent_ev_df], ignore_index=True)

    return team_ent_ev_df, team_ins_ent_ev_df


"""
To do:
child
"""


def update_ta2_team_stats(team_stats_df: pd.DataFrame, team_wd_node_df: pd.DataFrame,
                          file_name: str, schema_id: str, ta2_team_name: str,
                          team_ev_df: pd.DataFrame,
                          team_arg_df: pd.DataFrame, team_child_df: pd.DataFrame,
                          team_ent_df: pd.DataFrame, team_rel_df: pd.DataFrame,
                          team_temporalrel_df: pd.DataFrame,
                          schema_instance_id: str, ta1_team_name: str,
                          team_ent_ev_df: pd.DataFrame, team_ins_ent_ev_df: pd.DataFrame):
    # # compute event stats
    # initiate
    ev_count = ev_primitive_count = ev_instantiated_count = ev_wd_node_count = 0
    ev_distinct_wd_node_count = ev_wd_label_count = 0
    ev_predicted_count = 0
    avg_wd_node_ev = avg_arg_per_ev = frac_arg_ins = avg_child_per_ev = frac_ent_ins = \
        frac_ins_ent_coref = avg_arg_per_ent = np.nan
    # calculate
    if len(team_ev_df) > 0:
        # ev_df = team_ev_df.loc[team_ev_df['schema_id'] == schema_id]
        ev_df = team_ev_df.loc[team_ev_df['schema_instance_id'] == schema_instance_id]
        ev_count = len(ev_df)
        ev_primitive_count = len(ev_df.loc[ev_df['ev_child_list'] == "[]", :])
        ev_instantiated_df = get_ta2_instantiated_events(ev_df)
        ev_instantiated_count = len(ev_instantiated_df)

        ev_wd_node_df = ev_df.loc[(ev_df['ev_wd_node'].notnull()) |
                                  (ev_df['ev_ta2wd_node'].notnull())]
        ev_wd_node_count = len(ev_wd_node_df)
        ev_wd_node_dict = get_schema_ev_wd_node_dict(ev_wd_node_df)
        ev_distinct_wd_node_count = len(ev_wd_node_dict)
        wd_node_df = get_wd_node_dataframe(file_name, ta2_team_name, ta1_team_name, schema_id,
                                           ev_wd_node_dict)
        ev_wd_label_df = ev_df.loc[(ev_df['ev_wd_label'].notnull()) |
                                   (ev_df['ev_ta2wd_label'].notnull())]
        ev_wd_label_count = len(ev_wd_label_df)

        ev_predicted_df = get_ta2_predicted_events(ev_df)
        ev_predicted_count = len(ev_predicted_df)
        avg_wd_node_ev = ev_wd_node_count / ev_count

        ev_ta1ref_count = len(ev_df.loc[(ev_df['ev_ta1ref'] != 'none'), :])
        ev_instantiated_ta1ref_count = len(ev_instantiated_df.loc[(
            ev_instantiated_df['ev_ta1ref'] != 'none'), :])
        ev_predicted_ta1ref_count = len(ev_predicted_df.loc[(
                                        ev_predicted_df['ev_ta1ref'] != 'none'), :])

    # compute argument stats
    arg_count = 0
    ins_arg_count = pred_arg_count = ins_pred_arg_count = 0
    pred_ent_count = ins_pred_ent_count = 0
    # calculate
    if len(team_arg_df) > 0:
        arg_df = team_arg_df.loc[team_arg_df['schema_instance_id'] == schema_instance_id]
        arg_count = len(arg_df)
        ins_arg_count = len(arg_df.loc[(pd.notnull(arg_df['arg_ta2entity'])) &
                                       (arg_df['arg_ta2entity'] != "kairos:NULL"), :])
        frac_arg_ins = ins_arg_count/arg_count
        if ev_count > 0:
            avg_arg_per_ev = arg_count/ev_count
        # Get predicted_arg_count
        ev_df = team_ev_df.loc[team_ev_df['schema_instance_id'] == schema_instance_id]
        ev_predicted_df = get_ta2_predicted_events(ev_df)
        arg_ins_df = arg_df.loc[(pd.notnull(arg_df['arg_ta2entity'])) &
                                (arg_df['arg_ta2entity'] != "kairos:NULL"), :]
        arg_pred_df = pd.merge(arg_df.loc[:, ['schema_instance_id', 'ta2_team_name',
                                              'ta1_team_name', 'arg_id', 'ev_id',
                                              'arg_entity', 'arg_ta2entity']],
                               ev_predicted_df.loc[:, ['ev_id', 'ev_prediction_provenance',
                                                       'ev_confidence']],
                               on='ev_id', how='inner')
        arg_ins_pred_df = pd.merge(arg_ins_df.loc[:, ['schema_instance_id', 'ta2_team_name',
                                                      'ta1_team_name', 'arg_id', 'ev_id',
                                                      'arg_entity', 'arg_ta2entity']],
                                   ev_predicted_df.loc[:, ['ev_id', 'ev_prediction_provenance',
                                                           'ev_confidence']],
                                   on='ev_id', how='inner')
        if arg_pred_df.shape[0] > 0:
            pred_arg_count = arg_pred_df.shape[0]
            pred_ent_count = len(pd.unique(
                arg_pred_df.loc[(pd.notnull(arg_pred_df['arg_ta2entity'])) &
                                (arg_pred_df['arg_ta2entity'] != "kairos:NULL"),
                                'arg_ta2entity']).tolist())
        if arg_ins_pred_df.shape[0] > 0:
            ins_pred_arg_count = arg_ins_pred_df.shape[0]
            ins_pred_ent_count = len(pd.unique(
                arg_ins_pred_df.loc[(pd.notnull(arg_ins_pred_df['arg_ta2entity'])) &
                                    (arg_ins_pred_df['arg_ta2entity'] != "kairos:NULL"),
                                    'arg_ta2entity']).tolist())

    # compute child stats
    child_count = 0
    # calculate
    if len(team_child_df) > 0:
        child_df = team_child_df.loc[team_child_df['schema_instance_id'] == schema_instance_id]
        child_count = len(child_df)
        unique_child_list = child_df['child_ev_id'].unique().tolist()
        ev_id_list = ev_df['ev_id'].unique().tolist()
        ev_root_df = ev_df.loc[ev_df['ev_parent'] == 'kairos:NULL', :]
        ev_root_id = ev_root_df['ev_id'].to_string(index=False)
        ev_top_level_count = len(child_df.loc[child_df['ev_id'] == ev_root_id, :])

        ev_with_child_count = child_df['ev_id'].nunique()
        ev_leaf_count = ev_count - ev_with_child_count  # same as ev_primitive count above

        if ev_count > 0:
            avg_child_per_ev = child_count/ev_count

        def dfs(child: str, cnt: int = 1) -> int:
            if child in ev_id_list:
                child_temp_df = child_df.loc[child_df['ev_id'] == child, :]
                grand_child_list = child_temp_df['child_ev_id'].unique().tolist()
                if len(grand_child_list) > 0:
                    for grand_child in grand_child_list:
                        return dfs(grand_child, cnt + 1)

            return cnt

        depth_list = []
        for child_id in unique_child_list:
            depth_list.append(dfs(child_id, 1))
        avg_hier_depth = sum(depth_list) / len(depth_list)
        max_hier_depth = max(depth_list)

    # compute entity stats
    ent_count = 0
    ins_ent_count = 0
    if len(team_ent_df) > 0:
        ent_df = team_ent_df.loc[team_ent_df['schema_instance_id'] == schema_instance_id]
        ent_count = len(ent_df)
        avg_arg_per_ent = arg_count/ent_count
        if len(team_arg_df) > 0:
            temp_arg_df = team_arg_df.loc[team_arg_df['schema_instance_id'] == schema_instance_id]
            arg_df = temp_arg_df.loc[temp_arg_df['arg_ta2entity'] != "kairos:NULL", :]
            if pd.notnull(arg_df['arg_ta2entity']).any():
                join_df = pd.merge(arg_df, ent_df.loc[:, ['ent_id', 'ent_name', 'ent_wd_node',
                                                          'ent_wd_label', 'ent_ta2wd_node',
                                                          'ent_ta2wd_label']],
                                   left_on=['arg_ta2entity'], right_on='ent_id', how="inner")
                join_df = join_df[['ent_id']].drop_duplicates()
                ins_ent_count = len(join_df)
                frac_ent_ins = ins_ent_count/ent_count
    # compute relation stats
    rel_count = rel_distinct_subject_count = 0
    if len(team_rel_df) > 0:
        rel_df = team_rel_df.loc[team_rel_df['schema_instance_id'] == schema_instance_id]
        rel_count = len(rel_df)
        rel_subject_df = rel_df.loc[rel_df['rel_subject'].notnull()]
        if len(rel_subject_df) == 0:
            rel_subject_df = rel_df.loc[rel_df['rel_relationSubject'].notnull()]
            rel_distinct_subject_count = len(rel_subject_df.rel_relationSubject.unique())
        else:
            rel_distinct_subject_count = len(rel_subject_df.rel_subject.unique())
    temporalrel_count = 0
    if len(team_temporalrel_df) > 0:
        temporalrel_df = team_temporalrel_df.loc[team_temporalrel_df['schema_id'] == schema_id]
        temporalrel_count = len(temporalrel_df)
    # combine as a row (dict)
    ent_coref_count = len(team_ent_ev_df.loc[(team_ent_ev_df['num_events'] >= 2) &
                                             (team_ent_ev_df[
                                                 'schema_instance_id'] == schema_instance_id), :])
    ins_ent_coref_count = len(
        team_ins_ent_ev_df.loc[(team_ins_ent_ev_df['num_events'] >= 2) &
                               (team_ins_ent_ev_df['schema_instance_id'] == schema_instance_id), :])
    frac_ent_coref = ent_coref_count/ent_count
    if ins_ent_count > 0:
        frac_ins_ent_coref = ins_ent_coref_count/ins_ent_count
    # All entities that appear
    ent_predicted_ref_count = len(
        team_ent_ev_df.loc[(team_ent_ev_df['num_predicted_events'] >= 1) &
                           (team_ent_ev_df['schema_instance_id'] == schema_instance_id), :])
    ins_ent_predicted_ref_count = len(
        team_ins_ent_ev_df.loc[(team_ins_ent_ev_df['num_predicted_events'] >= 1) &
                               (team_ins_ent_ev_df['schema_instance_id'] == schema_instance_id), :])
    ent_predicted_and_non_coref_count = \
        len(team_ent_ev_df.loc[(team_ent_ev_df['num_predicted_events'] >= 1) &
                               ((team_ent_ev_df['num_non_predicted_events'] >= 1)) &
                               (team_ent_ev_df['schema_instance_id'] == schema_instance_id), :])
    ins_ent_predicted_and_non_coref_count = \
        len(team_ins_ent_ev_df.loc[(team_ins_ent_ev_df['num_predicted_events'] >= 1) &
                                   (team_ins_ent_ev_df['num_non_predicted_events'] >= 1) &
                                   (team_ins_ent_ev_df['schema_instance_id'] ==
                                    schema_instance_id), :])

    ins_arg_in_ins_ev_count = ins_arg_count - ins_pred_arg_count
    team_stats_row = {
        'file_name': file_name, 'ta2_team_name': ta2_team_name, 'ta1_team_name': ta1_team_name,
        'schema_instance_id': schema_instance_id,
        'schema_id': schema_id, 'ev_count': ev_count, 'ev_primitive_count': ev_primitive_count,
        'ev_instantiated_count': ev_instantiated_count, 'ev_wd_node_count': ev_wd_node_count,
        'avg_wd_node_ev': avg_wd_node_ev,
        'ev_ta1ref_count': ev_ta1ref_count,
        'ev_distinct_wd_node_count': ev_distinct_wd_node_count,
        'ev_wd_label_count': ev_wd_label_count,
        'ev_predicted_count': ev_predicted_count,
        'ev_instantiated_ta1ref_count': ev_instantiated_ta1ref_count,
        'ev_predicted_ta1ref_count': ev_predicted_ta1ref_count,
        'arg_count': arg_count, 'avg_arg_per_ev': avg_arg_per_ev, 'ins_arg_count': ins_arg_count,
        'pred_arg_count': pred_arg_count, 'ins_pred_arg_count': ins_pred_arg_count,
        'ins_arg_in_ins_ev_count': ins_arg_in_ins_ev_count,
        'frac_arg_ins': frac_arg_ins,
        'ev_top_level_count': ev_top_level_count,
        'ev_with_child_count': ev_with_child_count,
        'ev_leaf_count': ev_leaf_count,
        'child_count': child_count, 'avg_child_per_ev': avg_child_per_ev,
        'avg_hier_depth': avg_hier_depth, 'max_hier_depth': max_hier_depth,
        'ent_count': ent_count, 'avg_arg_per_ent': avg_arg_per_ent, 'ins_ent_count': ins_ent_count,
        'frac_ent_ins': frac_ent_ins,
        'pred_ent_count': pred_ent_count, 'ins_pred_ent_count': ins_pred_ent_count,
        'rel_count': rel_count, 'rel_distinct_subject_count': rel_distinct_subject_count,
        'temporalrel_count': temporalrel_count,
        'ent_coref_count': ent_coref_count, 'ins_ent_coref_count': ins_ent_coref_count,
        'ent_predicted_ref_count': ent_predicted_ref_count,
        'ent_predicted_and_non_coref_count': ent_predicted_and_non_coref_count,
        'ins_ent_predicted_ref_count': ins_ent_predicted_ref_count,
        'ins_ent_predicted_and_non_coref_count': ins_ent_predicted_and_non_coref_count,
        'frac_ent_coref': frac_ent_coref, 'frac_ins_ent_coref': frac_ins_ent_coref
    }
    # add to team stats
    team_stats_df = pd.concat([team_stats_df, pd.DataFrame([team_stats_row])], ignore_index=True)
    team_wd_node_df = pd.concat([team_wd_node_df, wd_node_df], ignore_index=True)

    return team_stats_df, team_wd_node_df


def compute_ta1_submission_stats(ta1_score_directory: str,
                                 ta1_analysis_directory: str) -> \
        typing.Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ta1_stats_df = initiate_ta1_stats_dataframe()
    ta1_wd_node_df = initiate_wd_node_dataframe()
    ta1_ent_ev_df = initiate_ta1_ent_ev_dataframe()

    ta1_collection = TA1Collection()
    ta1_collection.import_extractions_from_file_collection(ta1_score_directory)

    for file_dir in os.listdir(ta1_score_directory):
        if os.path.isdir(os.path.join(ta1_score_directory, file_dir)):
            ta1_team_name = file_dir
            # Check for an empty directory and skip if it is empty
            try:
                ta1_library = ta1_collection.ta1dict[ta1_team_name]
            except KeyError as e:
                print("Required TA1 Library " + ta1_team_name + " Not found for file " + file_dir)
                print("Skipping file directory. Exception message:")
                print(e)
                continue
            team_stats_df = initiate_ta1_stats_dataframe()
            team_wd_node_df = initiate_wd_node_dataframe()
            # get ta1 team aggregated element dataframe
            team_schema_df = ta1_library.schema_df
            team_ev_df = ta1_library.ev_df
            team_arg_df = ta1_library.arg_df
            team_child_df = ta1_library.children_df
            team_ent_df = ta1_library.ent_df
            team_rel_df = ta1_library.rel_df
            team_temporalrel_df = ta1_library.temporalrel_df

            for schema_row in team_schema_df.itertuples():
                file_name = schema_row.file_name
                schema_id = schema_row.schema_id
                team_ent_ev_df = initiate_ta1_ent_ev_dataframe()

                team_ent_ev_df = link_ta1_entities_with_events(team_ent_ev_df, team_wd_node_df,
                                                               file_name,
                                                               schema_id, team_ev_df,
                                                               team_arg_df, team_child_df,
                                                               team_ent_df, team_rel_df,
                                                               team_temporalrel_df,
                                                               ta1_team_name)

                team_stats_df, team_wd_node_df = update_ta1_team_stats(team_stats_df,
                                                                       team_wd_node_df,
                                                                       file_name,
                                                                       schema_id, ta1_team_name,
                                                                       team_ev_df,
                                                                       team_arg_df, team_child_df,
                                                                       team_ent_df, team_rel_df,
                                                                       team_temporalrel_df,
                                                                       team_ent_ev_df)

            # write team stats to excel files
            ta1_team_directory = os.path.join(ta1_analysis_directory, ta1_team_name)
            if not os.path.isdir(ta1_team_directory):
                os.makedirs(ta1_team_directory)
            team_stats_fn = os.path.join(ta1_team_directory, 'TA1_' + ta1_team_name + '_stats.xlsx')
            team_stats_df.to_excel(team_stats_fn, index=False)
            team_wd_node_fn = os.path.join(
                ta1_team_directory, 'TA1_' + ta1_team_name + '_wd_node.xlsx')
            team_wd_node_df.to_excel(team_wd_node_fn, index=False)
            team_ent_ev_fn = os.path.join(ta1_team_directory, 'TA1_' + ta1_team_name +
                                          '_ent_ev.xlsx')
            team_ent_ev_df.to_excel(team_ent_ev_fn, index=False)
            # merge to ta1 stats/wd_node
            ta1_stats_df = pd.concat([ta1_stats_df, team_stats_df], ignore_index=True)
            ta1_wd_node_df = pd.concat([ta1_wd_node_df, team_wd_node_df], ignore_index=True)
            ta1_ent_ev_df = pd.concat([ta1_ent_ev_df, team_ent_ev_df], ignore_index=True)

    # save the merged ta1 stats
    ta1_stats_fn = os.path.join(ta1_analysis_directory, 'TA1_stats.xlsx')
    ta1_stats_df.to_excel(ta1_stats_fn, index=False)
    ta1_wd_node_fn = os.path.join(ta1_analysis_directory, 'TA1_wd_node.xlsx')
    ta1_wd_node_df.to_excel(ta1_wd_node_fn, index=False)
    ta1_ent_ev_fn = os.path.join(ta1_analysis_directory, 'TA1_entities.xlsx')
    ta1_ent_ev_df.to_excel(ta1_ent_ev_fn, index=False)
    return ta1_stats_df, ta1_wd_node_df, ta1_ent_ev_df


def compute_ta2_submission_stats(ta2_score_directory: str, ta2_analysis_directory: str,
                                 extract_for_graph_g=False) -> \
        typing.Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ta2_stats_df = initiate_ta2_stats_dataframe()
    ta2_ent_ev_df = initiate_ta2_ent_ev_dataframe()
    ta2_ins_ent_ev_df = initiate_ta2_ent_ev_dataframe()
    ta2_wd_node_df = initiate_wd_node_dataframe()

    # First, Import the collection
    ta2_collection = TA2Collection()
    ta2_collection.import_extractions_from_file_collection(ta2_score_directory,
                                                           extract_for_graph_g=extract_for_graph_g)

    # Return if empty without crashing
    if not ta2_collection.ta2dict:
        print("Empty TA2 collection.")
        return ta2_stats_df, pd.DataFrame(), ta2_wd_node_df, ta2_ent_ev_df, ta2_ins_ent_ev_df

    for file_dir in os.listdir(ta2_score_directory):
        if os.path.isdir(os.path.join(ta2_score_directory, file_dir)):
            ta2_team_name = file_dir
            # Get the library for the TA2 team
            try:
                ta2_team_library = ta2_collection.ta2dict[ta2_team_name]
            except KeyError as e:
                print("Required TA2 Team " + ta2_team_name + " Not found for file " + file_dir)
                print("Skipping file directory. Exception message:")
                print(e)
                continue
            # Now we have the data imported and ready to work with
            team_stats_df = initiate_ta2_stats_dataframe()
            team_ent_ev_df = initiate_ta2_ent_ev_dataframe()
            team_ins_ent_ev_df = initiate_ta2_ent_ev_dataframe()
            team_wd_node_df = initiate_wd_node_dataframe()
            # get ta2 team aggregated element dataframe
            team_schema_df = ta2_team_library.schema_df
            team_ev_df = ta2_team_library.ev_df
            team_arg_df = ta2_team_library.arg_df
            team_child_df = ta2_team_library.children_df
            team_ent_df = ta2_team_library.ent_df
            team_rel_df = ta2_team_library.rel_df
            team_temporalrel_df = ta2_team_library.temporalrel_df

            for schema_row in team_schema_df.itertuples():
                file_name = schema_row.file_name
                schema_id = schema_row.schema_id
                schema_instance_id = schema_row.schema_instance_id
                ta1_team_name = schema_row.ta1_team_name

                team_ent_ev_df, team_ins_ent_ev_df = \
                    link_ta2_entities_with_events(team_ent_ev_df, team_ins_ent_ev_df,
                                                  team_wd_node_df,
                                                  file_name,
                                                  schema_id, ta2_team_name, team_ev_df,
                                                  team_arg_df, team_child_df,
                                                  team_ent_df, team_rel_df,
                                                  team_temporalrel_df,
                                                  schema_instance_id, ta1_team_name)

                team_stats_df, team_wd_node_df = \
                    update_ta2_team_stats(team_stats_df, team_wd_node_df, file_name,
                                          schema_id, ta2_team_name, team_ev_df,
                                          team_arg_df, team_child_df,
                                          team_ent_df, team_rel_df,
                                          team_temporalrel_df,
                                          schema_instance_id, ta1_team_name,
                                          team_ent_ev_df, team_ins_ent_ev_df)

            # write team stats to excel files
            team_stats_df.sort_values(by=['ta2_team_name', 'ta1_team_name',
                                          'schema_instance_id'], inplace=True)
            team_wd_node_df.sort_values(by=['ta2_team_name', 'ta1_team_name', 'schema_id', 'count'],
                                        ascending=[True, True, True, False], inplace=True)
            team_ent_ev_df.sort_values(by=['ta2_team_name', 'ta1_team_name',
                                           'schema_instance_id', 'num_events'],
                                       ascending=[True, True, True, False], inplace=True)
            ta2_team_directory = os.path.join(ta2_analysis_directory, ta2_team_name)
            if not os.path.isdir(ta2_team_directory):
                os.makedirs(ta2_team_directory)
            else:
                pass

            team_stats_fn = os.path.join(ta2_team_directory, 'TA2_' + ta2_team_name + '_stats.xlsx')
            team_stats_df.to_excel(team_stats_fn, index=False)
            team_wd_node_fn = os.path.join(
                ta2_team_directory, 'TA2_' + ta2_team_name + '_wd_node.xlsx')
            team_wd_node_df.to_excel(team_wd_node_fn, index=False)
            team_ent_ev_fn = os.path.join(ta2_team_directory, 'TA2_' + ta2_team_name +
                                          '_ent_ev.xlsx')
            team_ent_ev_df.to_excel(team_ent_ev_fn, index=False)
            team_ins_ent_ev_fn = os.path.join(ta2_team_directory, 'TA2_' + ta2_team_name +
                                              '_ins_ent_ev.xlsx')
            team_ins_ent_ev_df.to_excel(team_ins_ent_ev_fn, index=False)

            # Extra ta2 processing
            team_ent_freq_df = \
                team_ent_ev_df.groupby(['ta2_team_name', 'ta1_team_name', 'ent_name',
                                        'ent_wd_node', 'ent_wd_label', 'ent_ta2wd_node',
                                        'ent_ta2wd_label']
                                       )['num_events'].sum().reset_index(name='num_events')
            if len(team_ent_freq_df) > 0:
                team_ent_freq_df.sort_values(by=['ta2_team_name', 'ta1_team_name', 'num_events'],
                                             ascending=[True, True, False], inplace=True)
            team_ent_freq_fn = os.path.join(ta2_team_directory, 'TA2_' + ta2_team_name +
                                            '_entity_name_frequency.xlsx')
            team_ent_freq_df.to_excel(team_ent_freq_fn, index=False)

            team_ins_ent_freq_df = \
                team_ins_ent_ev_df.groupby(['ta2_team_name', 'ta1_team_name', 'ent_name',
                                            'ent_wd_node', 'ent_wd_label', 'ent_ta2wd_node',
                                            'ent_ta2wd_label']
                                           )['num_events'].sum().reset_index(name='num_events')
            team_ins_ent_freq_fn = os.path.join(ta2_team_directory, 'TA2_' + ta2_team_name +
                                                '_instantiated_entity_name_frequency.xlsx')
            team_ins_ent_freq_df.to_excel(team_ins_ent_freq_fn, index=False)
            # merge to ta1 stats/wd_node
            ta2_stats_df = pd.concat([ta2_stats_df, team_stats_df], ignore_index=True)
            ta2_wd_node_df = pd.concat([ta2_wd_node_df, team_wd_node_df], ignore_index=True)
            ta2_ent_ev_df = pd.concat([ta2_ent_ev_df, team_ent_ev_df], ignore_index=True)
            ta2_ins_ent_ev_df = pd.concat([ta2_ins_ent_ev_df, team_ins_ent_ev_df],
                                          ignore_index=True)

    # Get TA2 stats by library
    ta2_stats_df_copy = ta2_stats_df.fillna(0).copy(deep=True)
    ta2_group_stats_df = ta2_stats_df_copy.groupby(['ta2_team_name', 'ta1_team_name']).\
        agg(np.sum).reset_index()
    ta2_group_stats_df = ta2_group_stats_df.drop(['file_name', 'schema_id', 'schema_instance_id'],
                                                 axis=1, errors='ignore')
    # Now we do the averages for ta2_group stats
    ta2_group_stats_df['avg_wd_node_ev'] = \
        ta2_group_stats_df['ev_wd_node_count']/ta2_group_stats_df['ev_count']
    ta2_group_stats_df['avg_wd_node_ins_ev'] = \
        ta2_group_stats_df['ev_wd_node_count']/ta2_group_stats_df['ev_instantiated_count']
    ta2_group_stats_df['avg_arg_per_ev'] = \
        ta2_group_stats_df['arg_count']/ta2_group_stats_df['ev_count']
    ta2_group_stats_df['frac_arg_ins'] = \
        ta2_group_stats_df['ins_arg_count']/ta2_group_stats_df['arg_count']
    ta2_group_stats_df['avg_child_per_ev'] = \
        ta2_group_stats_df['child_count']/ta2_group_stats_df['ev_count']
    ta2_group_stats_df['avg_arg_per_ent'] = \
        ta2_group_stats_df['arg_count']/ta2_group_stats_df['ent_count']
    ta2_group_stats_df['frac_ent_ins'] = \
        ta2_group_stats_df['ins_ent_count']/ta2_group_stats_df['ent_count']
    ta2_group_stats_df['frac_ent_coref'] = \
        ta2_group_stats_df['ent_coref_count']/ta2_group_stats_df['ent_count']
    ta2_group_stats_df['frac_ins_ent_coref'] = \
        ta2_group_stats_df['ins_ent_coref_count']/ta2_group_stats_df['ins_ent_count']

    # save the merged ta2 stats
    ta2_stats_df.sort_values(by=['ta2_team_name', 'ta1_team_name', 'schema_instance_id'],
                             inplace=True)
    ta2_wd_node_df.sort_values(by=['ta2_team_name', 'ta1_team_name', 'schema_id', 'count'],
                               ascending=[True, True, True, False], inplace=True)
    ta2_ent_ev_df.sort_values(by=['ta2_team_name', 'ta1_team_name', 'schema_instance_id',
                                  'num_events'],
                              ascending=[True, True, True, False], inplace=True)
    ta2_ins_ent_ev_df.sort_values(by=['ta2_team_name', 'ta1_team_name', 'schema_instance_id',
                                      'num_events'],
                                  ascending=[True, True, True, False], inplace=True)
    ta2_group_stats_df.sort_values(by=['ta2_team_name', 'ta1_team_name'], inplace=True)
    ta2_stats_fn = os.path.join(ta2_analysis_directory, 'TA2_stats.xlsx')
    ta2_stats_df.to_excel(ta2_stats_fn, index=False)
    ta2_wd_node_fn = os.path.join(ta2_analysis_directory, 'TA2_wd_node.xlsx')
    ta2_wd_node_df.to_excel(ta2_wd_node_fn, index=False)
    ta2_ent_ev_fn = os.path.join(ta2_analysis_directory, 'TA2_entities.xlsx')
    ta2_ent_ev_df.to_excel(ta2_ent_ev_fn, index=False)
    ta2_ins_ent_ev_fn = os.path.join(ta2_analysis_directory, 'TA2_instantiated_entities.xlsx')
    ta2_ins_ent_ev_df.to_excel(ta2_ins_ent_ev_fn, index=False)
    ta2_group_stats_fn = os.path.join(ta2_analysis_directory, 'TA2_group_stats.xlsx')
    ta2_group_stats_df.to_excel(ta2_group_stats_fn, index=False)
    return ta2_stats_df, ta2_group_stats_df, ta2_wd_node_df, ta2_ent_ev_df, ta2_ins_ent_ev_df
