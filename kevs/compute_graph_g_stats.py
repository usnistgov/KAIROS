import os
import pandas as pd
import numpy as np
from kevs.compare_graph_g_versions import get_graph_g_entity


def score_graph_g_stats(graph_g_stats_dir, output_dir) -> None:
    """
    Args:
        graph_g_stats_dir: "Graph_G_Stats"
        output_dir: "Graph_G_Scores"
    """
    graphg_scores_df = pd.DataFrame(columns=['ce', 'schema_instance_id', 'ta1_team',
                                             'ta2_team', 'recall_graphg_ev'])
    abridged_graphg_scores_df = pd.DataFrame(columns=['ce', 'schema_instance_id', 'ta1_team',
                                             'ta2_team', 'recall_graphg_ev'])
    for ce in os.listdir(graph_g_stats_dir):
        if ce != ".DS_Store":
            filename = os.path.join(graph_g_stats_dir, ce)
        graphg_stats_df = pd.read_csv(filename)
        graphg_scores_temp_df = graphg_stats_df[['ce', 'schema_instance_id', 'ta1_team',
                                                 'ta2_team', 'recall_graphg_ev']].copy()

        graphg_scores_temp_df = \
            graphg_scores_temp_df.loc[graphg_scores_temp_df.groupby(['ta1_team', 'ta2_team'])[
                'recall_graphg_ev'].idxmax()][['ce', 'schema_instance_id', 'ta1_team',
                                               'ta2_team', 'recall_graphg_ev']]
        graphg_scores_df = pd.concat([graphg_scores_df, graphg_scores_temp_df])
        if ("abridged" in ce) and (("g2" or "g3") not in ce):
            abridged_graphg_scores_df = pd.concat([abridged_graphg_scores_df,
                                                   graphg_scores_temp_df])

    graphg_scores_result_df = graphg_scores_df.groupby(['ta1_team', 'ta2_team']).agg(
        {'recall_graphg_ev': ['mean', 'min', 'max', 'std']})
    graphg_scores_result_df.columns = ['recall_average', 'recall_min',
                                       'recall_max', 'recall_std']
    graphg_scores_result_df = graphg_scores_result_df.reset_index()
    graphg_scores_result_df.to_csv(os.path.join(output_dir,
                                                "graphg_scores_all.csv"), index=False)

    abridged_graphg_scores_result_df = \
        abridged_graphg_scores_df.groupby(['ta1_team', 'ta2_team']).agg(
            {'recall_graphg_ev': ['mean', 'min', 'max', 'std']})
    abridged_graphg_scores_result_df.columns = ['recall_average', 'recall_min',
                                                'recall_max', 'recall_std']
    abridged_graphg_scores_result_df = abridged_graphg_scores_result_df.reset_index()
    abridged_graphg_scores_result_df.to_csv(os.path.join(output_dir,
                                                         "graphg_scores_abridged.csv"), index=False)


def compute_graph_g_stats(output_dir, ta2_collection,
                          graph_g_collection,
                          task2_annotation_dir) -> None:
    """
    Args:
        output_dir:
        ta2_collection:
        graph_g_collection:
        task2_annotation_dir:

    Returns:
    """
    ta2_team_list = ta2_collection.ta2dict.keys()
    complex_event_list = os.listdir(task2_annotation_dir)
    complex_event_list.remove('.DS_Store')
    ta1_team_list = ['CMU', 'IBM', 'ISI', 'RESIN', 'SBU']

    for ce in complex_event_list:
        # Get Graph G instances and annotation
        graphg_key = "GRAPHG|GRAPHG|{}|nist:Instances/00001/nistQuizlet9GraphG". \
            format(ce)
        graphg_ceinstance = graph_g_collection.ta2dict['GRAPHG'].ta2dict[
            graphg_key]
        graph_g_ev_df = graphg_ceinstance.ev_df
        graph_g_ev_df['graphg_ev_id'] = graph_g_ev_df['ev_id'].str.split('/', expand=True)[2]

        # get the Annotation ID for Event, Argument, Entity, and Relation
        graphg_ceinstance.ev_df['graphg_ev_id'] = \
            graphg_ceinstance.ev_df['ev_id'].str.split('/', expand=True)[2]
        graphg_ceinstance.arg_df['graphg_arg_id'] = \
            graphg_ceinstance.arg_df['arg_id'].str.split('/', expand=True)[2]
        graphg_ceinstance.arg_df['graphg_ent_id'] = \
            graphg_ceinstance.arg_df['arg_ta2entity'].str.split('/', expand=True)[2]
        graphg_ceinstance.ent_df['graphg_ent_id'] = \
            graphg_ceinstance.ent_df['ent_id'].str.split('/', expand=True)[2]

        output_df = pd.DataFrame(columns=['ce', 'schema_instance_id', 'ta1_team',
                                          'ta2_team', 'num_graphg_ev', 'ratio_graphg_ev_linked',
                                          'num_graphg_ev_ref_ta2',
                                          'num_ins_graphg_ev_ref_ta2', 'per_of_ins_graphg_ev',
                                          'recall_graphg_ev',
                                          'num_ta2_ev_linking_graphg',
                                          'num_ins_ta2_ev_linking_graphg',
                                          'per_of_ins_ta2_ev_linking_g',
                                          'num_graphg_arg',
                                          'num_graphg_arg_ref_ta2',
                                          'num_ins_graphg_arg_ref_ta2',
                                          'per_of_ins_graphg_arg',
                                          'num_graphg_ent',
                                          'num_graphg_ent_ref_ta2',
                                          'num_ins_graphg_ent_ref_ta2',
                                          'per_of_ins_graphg_ent',
                                          'num_args_with_ref_graphg_entities',
                                          'num_ins_args_with_ref_graphg_entities', ])

        for ta2_team in ta2_team_list:
            ta2_instance = ta2_collection.ta2dict[ta2_team]
            for ta1_team in ta1_team_list:
                ta2_ce_instance_list = [(key, value) for
                                        key, value in ta2_instance.ta2dict.items() if
                                        ((ta1_team in key.split('|')[0]) and
                                        (ta2_team in key.split('|')[1]) and
                                        (ce == key.split('.')[0].split('|')[2]))]

                for (key, value) in ta2_ce_instance_list:
                    ta2_ceinstance = value
                    ta2_graphg_ev_df = ta2_ceinstance.ev_df
                    if pd.notna(ta2_graphg_ev_df['ev_provenance']).any():
                        ta2_graphg_ev_df = ta2_ceinstance.ev_df. \
                            merge(graph_g_ev_df.loc[:, ['graphg_ev_id']], how="left",
                                  left_on="ev_provenance", right_on="graphg_ev_id")
                    else:
                        ta2_graphg_ev_df['graphg_ev_id'] = np.nan

                    ta2_graphg_arg_df = ta2_ceinstance.arg_df
                    if ta2_team == 'RESIN':
                        ta2_graphg_arg_df['ta2_ent_id'] = ta2_graphg_arg_df['arg_ta2provenance']
                    else:
                        ta2_graphg_arg_df['ta2_ent_id'] = ta2_graphg_arg_df['arg_ta2entity']\
                            .apply(get_graph_g_entity)

                    ta2_graphg_arg_df.loc[ta2_graphg_arg_df['ta2_ent_id']
                                          == 'kairos:NULL', 'ta2_ent_id'] = np.nan

                    if ta2_graphg_arg_df['arg_id'].str.contains('AR').any():
                        if ta2_team == 'CMU':
                            ta2_graphg_arg_df['ta2_arg_id'] = \
                                ta2_graphg_arg_df['arg_id'].str.split('/', expand=True)[2]
                        elif ta2_team == 'IBM':
                            ta2_graphg_arg_df['ta2_arg_id'] = \
                                ta2_graphg_arg_df['arg_id'].str.split('/', expand=True)[3]
                    else:
                        ta2_graphg_arg_df['ta2_arg_id'] = np.nan

                    if pd.notna(ta2_graphg_arg_df['ta2_arg_id']).any():
                        ta2_graphg_arg_df = ta2_graphg_arg_df.merge(
                            graphg_ceinstance.arg_df.loc[:, ['graphg_arg_id']],
                            how="left", left_on="ta2_arg_id", right_on="graphg_arg_id")
                    else:
                        ta2_graphg_arg_df['graphg_arg_id'] = np.nan

                    ins_ta2_graphg_ev_df = ta2_graphg_ev_df.loc[
                        (ta2_graphg_ev_df['ev_ta1ref'] != "kairos:NULL") &
                        pd.notna(ta2_graphg_ev_df['ev_provenance']), :]
                    ins_ta2_graphg_arg_df = ta2_graphg_arg_df.loc[ta2_graphg_arg_df['ev_id'].isin(
                        ins_ta2_graphg_ev_df['ev_id'])]

                    ta2_graphg_ent_df = ta2_ceinstance.ent_df
                    if ta2_team == 'CMU':
                        ta2_graphg_ent_df['ta2_ent_id'] = \
                            ta2_graphg_ent_df['ent_id'].str.split('/', expand=True)[2]
                    elif ta2_team == 'IBM':
                        ta2_graphg_ent_df['ta2_ent_id'] = \
                            ta2_graphg_ent_df['ent_id'].str.split('/', expand=True)[3]
                    else:  # RESIN does not have any Graph G oriented entitiy ids in dataframe
                        ta2_graphg_ent_df['ta2_ent_id'] = np.nan

                    ta2_graphg_ent_df.loc[ta2_graphg_ent_df['ta2_ent_id'] == 'kairos:NULL',
                                          'ta2_ent_id'] = np.nan

                    if pd.notna(ta2_graphg_ent_df['ta2_ent_id']).any():
                        ta2_graphg_ent_df = ta2_graphg_ent_df.merge(
                            graphg_ceinstance.ent_df.loc[:, ['graphg_ent_id']],
                            how="left", left_on="ta2_ent_id", right_on="graphg_ent_id")
                    else:
                        ta2_graphg_ent_df['graphg_ent_id'] = np.nan

                    """
                    Statistics for Events
                    - num_graphg_ev_ref_ta2 : unique # of G events referred to by TA2
                    - num_ins_graphg_ev_ref_ta2: # of instantiated G events
                    - per_of_ins_graphg_ev
                    - recall_graphg_ev
                    - ratio_graphg_ev_linked
                    - num_ta2_ev_linking_graphg: # of ta2 events link to G
                    - num_ins_ta2_ev_linking_graphg: # of instantiated ta2 events
                    - per_of_ins_ta2_ev_linking_g
                    """
                    num_graphg_ev_ref_ta2 = ta2_graphg_ev_df['graphg_ev_id'][
                        pd.notna(ta2_graphg_ev_df['ev_provenance'])].nunique()
                    num_ins_graphg_ev_ref_ta2 = ta2_graphg_ev_df['graphg_ev_id'][
                        (ta2_graphg_ev_df['ev_ta1ref'] != "kairos:NULL")
                        & (pd.notna(ta2_graphg_ev_df['ev_provenance']))].nunique()
                    try:
                        per_of_ins_graphg_ev = num_ins_graphg_ev_ref_ta2 \
                            / num_graphg_ev_ref_ta2 * 100
                    except ZeroDivisionError:
                        per_of_ins_graphg_ev = 0
                    recall_graphg_ev = num_ins_graphg_ev_ref_ta2 \
                        / (len(graph_g_ev_df) - 1)
                    ratio_graphg_ev_linked = num_graphg_ev_ref_ta2 / (len(graph_g_ev_df) - 1)

                    num_ta2_ev_linking_graphg = ta2_graphg_ev_df['ev_id'][
                        pd.notna(ta2_graphg_ev_df['ev_provenance'])].nunique()
                    num_ins_ta2_ev_linking_graphg = ta2_graphg_ev_df['ev_id'][
                        (ta2_graphg_ev_df['ev_ta1ref'] != "kairos:NULL")
                        & (pd.notna(ta2_graphg_ev_df['ev_provenance']))].nunique()
                    try:
                        per_of_ins_ta2_ev_linking_g = num_ins_ta2_ev_linking_graphg \
                            / num_ta2_ev_linking_graphg * 100
                    except ZeroDivisionError:
                        per_of_ins_ta2_ev_linking_g = 0

                    """
                    Statistics for Arugments
                    - num_graphg_arg: number of unique arguments in Graph G
                    - num_graphg_arg_ref_ta2: # of Graph G arguments referred to by TA2
                    - num_ins_graphg_arg_ref_ta2: # of instantiated Graph G arguments
                    - per_of_ins_graphg_arg: % of instantiated Graph G arguments
                    """
                    num_graphg_arg = graphg_ceinstance.arg_df['graphg_arg_id'].nunique()
                    num_graphg_arg_ref_ta2 = ta2_graphg_arg_df['graphg_arg_id'].nunique()
                    num_ins_graphg_arg_ref_ta2 = ins_ta2_graphg_arg_df['graphg_arg_id'].nunique()
                    try:
                        per_of_ins_graphg_arg = num_ins_graphg_arg_ref_ta2 \
                            / num_graphg_arg_ref_ta2 * 100
                    except ZeroDivisionError:
                        per_of_ins_graphg_arg = 0

                    """
                    Statistics for Entities
                    - num_graphg_ent: number of unique entities in Graph G
                    - num_graphg_ent_ref_ta2: # of Graph G entities referred to by TA2
                    - num_ins_graphg_ent_ref_ta2: # of instantiated Graph G entities
                    - per_of_ins_graphg_arg: % of instantiated Graph G arguments
                    - num_args_with_ref_graphg_entities:
                    - num_ins_args_with_ref_graphg_entities:
                    """
                    num_graphg_ent = graphg_ceinstance.ent_df['graphg_ent_id'].nunique()
                    if ta2_team == 'RESIN':
                        num_graphg_ent_ref_ta2 = ta2_graphg_arg_df['ta2_ent_id'].nunique()
                    else:
                        num_graphg_ent_ref_ta2 = ta2_graphg_ent_df['graphg_ent_id'].nunique()
                    num_ins_graphg_ent_ref_ta2 = ins_ta2_graphg_arg_df['ta2_ent_id'].nunique()
                    try:
                        per_of_ins_graphg_ent = num_ins_graphg_ent_ref_ta2 \
                            / num_graphg_ent_ref_ta2 * 100
                    except ZeroDivisionError:
                        per_of_ins_graphg_ent = 0

                    # if (ta2_team == 'RESIN') and (ta1_team == 'IBM') and (ce == 'ce2013abridged'):
                    #     print("Check ta2_graphg_ent_df, ins_ta2_graphg_arg_df  ")

                    num_args_with_ref_graphg_entities = ins_ta2_graphg_arg_df['arg_id'].\
                        loc[pd.notna(ins_ta2_graphg_arg_df['ta2_ent_id'])].nunique()
                    num_ins_args_with_ref_graphg_entities = \
                        ins_ta2_graphg_arg_df['graphg_arg_id'].loc[
                            pd.notna(ins_ta2_graphg_arg_df['ta2_ent_id'])].nunique()

                    output_df.loc[len(output_df.index), [
                        'ce', 'ta1_team',
                        'ta2_team', 'schema_instance_id',
                        'num_graphg_ev',
                        'ratio_graphg_ev_linked',
                        'num_graphg_ev_ref_ta2',
                        'num_ins_graphg_ev_ref_ta2',
                        'per_of_ins_graphg_ev',
                        'recall_graphg_ev',
                        'num_ta2_ev_linking_graphg',
                        'num_ins_ta2_ev_linking_graphg',
                        'per_of_ins_ta2_ev_linking_g',
                        'num_graphg_arg',
                        'num_graphg_arg_ref_ta2',
                        'num_ins_graphg_arg_ref_ta2',
                        'per_of_ins_graphg_arg',
                        'num_graphg_ent',
                        'num_graphg_ent_ref_ta2',
                        'num_ins_graphg_ent_ref_ta2',
                        'per_of_ins_graphg_ent',
                        'num_args_with_ref_graphg_entities',
                        'num_ins_args_with_ref_graphg_entities']]\
                        = [ce, ta1_team, ta2_team,
                           ta2_graphg_ev_df.loc[0, 'schema_instance_id'],
                           len(graph_g_ev_df) - 1, ratio_graphg_ev_linked,
                           num_graphg_ev_ref_ta2,
                           num_ins_graphg_ev_ref_ta2, per_of_ins_graphg_ev,
                           recall_graphg_ev,
                           num_ta2_ev_linking_graphg, num_ins_ta2_ev_linking_graphg,
                           per_of_ins_ta2_ev_linking_g, num_graphg_arg,
                           num_graphg_arg_ref_ta2, num_ins_graphg_arg_ref_ta2,
                           per_of_ins_graphg_arg,
                           num_graphg_ent, num_graphg_ent_ref_ta2,
                           num_ins_graphg_ent_ref_ta2, per_of_ins_graphg_ent,
                           num_args_with_ref_graphg_entities,
                           num_ins_args_with_ref_graphg_entities]

        output_df.to_csv(os.path.join(output_dir, "graphg_stats_{}.csv".format(ce)), index=False)
