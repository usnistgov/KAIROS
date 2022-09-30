import re
import numpy as np
import pandas as pd
import os

from kevs.produce_event_trees import get_ta2_instantiated_events, get_ta2_predicted_events


def produce_summary_matrix(output_dir):
    ta2_ce_df = pd.read_csv(os.path.join(output_dir, "TA2_Assessed_CE_stats.csv"))
    ta2_ev_df = pd.read_csv(os.path.join(output_dir, "TA2_Assessed_Event_stats.csv"))
    ta2_arg_df = pd.read_csv(os.path.join(output_dir, "TA2_Assessed_Arguments_stats.csv"))
    ta2_pred_df = pd.read_csv(os.path.join(output_dir, "TA2_Assessed_Prediction_stats.csv"))

    ce_ev_df = ta2_ce_df.merge(ta2_ev_df, how="outer", on=[
                               "schema_instance_id", 'ta1_team', 'ta2_team', 'ce'])
    ce_ev_df['TA1-TA2'] = ce_ev_df['ta1_team'] + "-" + ce_ev_df['ta2_team']

    ce_arg_df = ta2_ce_df.merge(ta2_arg_df, how="outer", on=[
                                'schema_instance_id', 'ta1_team', 'ta2_team', 'ce'])
    ce_arg_df['TA1-TA2'] = ce_arg_df['ta1_team'] + "-" + ce_arg_df['ta2_team']

    ce_pred_df = ta2_ce_df.merge(ta2_pred_df, how="outer", on=[
                                 'schema_instance_id', 'ta1_team', 'ta2_team', 'ce'])
    ce_pred_df['TA1-TA2'] = ce_pred_df['ta1_team'] + "-" + ce_pred_df['ta2_team']

    ce_list = ['ce2013', 'ce2020', 'ce2024', 'ce2075', 'ce2079', 'ce2094',
               'ce2101', 'ce2102', 'ce2103', 'ce2104']
    team_list_unsorted = ce_ev_df['TA1-TA2'].unique()
    team_list = sorted(team_list_unsorted, key=lambda x: (x.split("-")[1], x.split("-")[0]))

    matrix_event_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_event_percentage_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_event_relevance_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_event_schema_order_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_event_precision_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_event_recall_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_event_recall_extrarel_approx_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_event_f1_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_pred_event_has_ref_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_per_pred_event_has_ref_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_num_pred_as_event_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_num_pred_as_event_plau_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_num_pred_as_event_implau_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_all_pred_event_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_per_plau_ev_vs_all_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_per_ev_plau_implau_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_num_hidden_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_num_pred_match_hidden_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_per_pred_match_hidden_df = pd.DataFrame(index=team_list, columns=ce_list)

    matrix_argument_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_pred_argument_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_per_assessed_arg_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_argument_precision_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_argument_recall_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_argument_f1_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_arg_precision_20_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_arg_recall_20_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_num_pred_as_arg_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_per_pred_as_arg_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_num_pred_as_arg_plau_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_num_pred_as_arg_implau_df = pd.DataFrame(index=team_list, columns=ce_list)
    matrix_per_arg_plau_implau_df = pd.DataFrame(index=team_list, columns=ce_list)

    for team in team_list:
        for ce in ce_list:
            instance_order_series = ce_ev_df.loc[(ce_ev_df['TA1-TA2'] == team) &
                                                 (ce_ev_df['ce'] == ce), 'schema_instance_id']
            instance_order_list = instance_order_series.tolist()
            instance_order_list.sort()

            if ce_ev_df.loc[(ce_ev_df['TA1-TA2'] == team) & (ce_ev_df['ce'] == ce) &
                            (ce_ev_df['instance_status'] == 'yes'), :].shape[0] > 0:
                num_events_assessed = ce_ev_df.loc[(ce_ev_df['TA1-TA2'] == team) &
                                                   (ce_ev_df['ce'] == ce) &
                                                   (ce_ev_df['instance_status'] == 'yes'),
                                                   'num_events_assessed'].item()
                num_events = ce_ev_df.loc[(ce_ev_df['TA1-TA2'] == team) &
                                          (ce_ev_df['ce'] == ce) &
                                          (ce_ev_df['instance_status'] == 'yes'),
                                          'num_events'].item()
                matrix_event_df.at[team, ce] = num_events_assessed
                matrix_event_percentage_df.at[team, ce] = num_events_assessed / num_events * 100

                event_precision = ce_ev_df.loc[(ce_ev_df['TA1-TA2'] == team) &
                                               (ce_ev_df['ce'] == ce) &
                                               (ce_ev_df['instance_status'] == 'yes'),
                                               'precision_matchwextrarel'].item()
                matrix_event_precision_df.at[team, ce] = event_precision
                event_recall = ce_ev_df.loc[(ce_ev_df['TA1-TA2'] == team) &
                                            (ce_ev_df['ce'] == ce) &
                                            (ce_ev_df['instance_status'] == 'yes'),
                                            'recall_matchwextrarel'].item()
                matrix_event_recall_df.at[team, ce] = event_recall
                event_f1 = ce_ev_df.loc[(ce_ev_df['TA1-TA2'] == team) &
                                        (ce_ev_df['ce'] == ce) &
                                        (ce_ev_df['instance_status'] == 'yes'),
                                        'f1_matchwextrarel'].item()
                matrix_event_f1_df.at[team, ce] = event_f1

                num_ann_ev_matchwextrarel = ce_ev_df.loc[(ce_ev_df['TA1-TA2'] == team) &
                                                         (ce_ev_df['ce'] == ce) &
                                                         (ce_ev_df['instance_status'] == 'yes'),
                                                         'num_ann_ev_matchwextrarel'].item()
                max_num_events_extrarel = ce_ev_df.loc[(ce_ev_df['ce'] == ce) &
                                                       (ce_ev_df['instance_status'] == 'yes'),
                                                       'num_events_extrarel'].max()
                num_ann_events = ce_ev_df.loc[(ce_ev_df['TA1-TA2'] == team) &
                                              (ce_ev_df['ce'] == ce) &
                                              (ce_ev_df['instance_status'] == 'yes'),
                                              'num_ann_events'].item()
                matrix_event_recall_extrarel_approx_df.at[team, ce] = num_ann_ev_matchwextrarel /\
                    (max_num_events_extrarel + num_ann_events)

                schema_name_relevance = ce_ev_df.loc[(ce_ev_df['TA1-TA2'] == team) &
                                                     (ce_ev_df['ce'] == ce) &
                                                     (ce_ev_df['instance_status'] == 'yes'),
                                                     'schema_name_relevance'].item()
                matrix_event_relevance_df.at[team, ce] = schema_name_relevance

                schema_instance_id = ce_ev_df.loc[(ce_ev_df['TA1-TA2'] == team) &
                                                  (ce_ev_df['ce'] == ce) &
                                                  (ce_ev_df['instance_status'] == 'yes'),
                                                  'schema_instance_id']
                instance_order_num = instance_order_list.index(schema_instance_id.max())
                matrix_event_schema_order_df.at[team, ce] = instance_order_num + 1

                num_pred_events_match = ce_ev_df.loc[(ce_ev_df['TA1-TA2'] == team) &
                                                     (ce_ev_df['ce'] == ce) &
                                                     (ce_ev_df['instance_status'] == 'yes'),
                                                     'num_pred_events_match'].item()
                matrix_pred_event_has_ref_df.at[team, ce] = num_pred_events_match

                num_pred_events_extrarel = ce_ev_df.loc[(ce_ev_df['TA1-TA2'] == team) &
                                                        (ce_ev_df['ce'] == ce) &
                                                        (ce_ev_df['instance_status'] == 'yes'),
                                                        'num_pred_events_extrarel'].item()
                num_pred_events_extrairrel = ce_ev_df.loc[(ce_ev_df['TA1-TA2'] == team) &
                                                          (ce_ev_df['ce'] == ce) &
                                                          (ce_ev_df['instance_status'] == 'yes'),
                                                          'num_pred_events_extrairrel'].item()
                per_pred_events_match = ce_ev_df.loc[(ce_ev_df['TA1-TA2'] == team) &
                                                     (ce_ev_df['ce'] == ce) &
                                                     (ce_ev_df['instance_status'] == 'yes'),
                                                     'per_pred_events_match'].item()
                matrix_per_pred_event_has_ref_df.at[team, ce] = per_pred_events_match

                num_ann_hidden_events = ce_ev_df.loc[(ce_ev_df['TA1-TA2'] == team) &
                                                     (ce_ev_df['ce'] == ce) &
                                                     (ce_ev_df['instance_status'] == 'yes'),
                                                     'num_ann_hidden_events'].item()
                matrix_num_hidden_df.at[team, ce] = num_ann_hidden_events

                num_pred_matched_hidden_events = ce_ev_df.loc[
                    (ce_ev_df['TA1-TA2'] == team) &
                    (ce_ev_df['ce'] == ce) &
                    (ce_ev_df['instance_status']
                     == 'yes'),
                    'num_pred_matched_hidden_events'].item()
                matrix_num_pred_match_hidden_df.at[team, ce] = num_pred_matched_hidden_events

                per_pred_matched_hidden_events = ce_ev_df.loc[
                    (ce_ev_df['TA1-TA2'] == team) &
                    (ce_ev_df['ce'] == ce) &
                    (ce_ev_df['instance_status']
                     == 'yes'),
                    'per_pred_matched_hidden_events'].item()
                matrix_per_pred_match_hidden_df.at[team, ce] = per_pred_matched_hidden_events

                if (num_pred_events_match + num_pred_events_extrarel
                        + num_pred_events_extrairrel) == 0:
                    matrix_pred_event_has_ref_df.at[team, ce] = "not assessed"
                    matrix_per_pred_event_has_ref_df.at[team, ce] = "not assessed"
                    matrix_num_pred_match_hidden_df.at[team, ce] = "not assessed"
                    matrix_per_pred_match_hidden_df.at[team, ce] = "not assessed"

            elif ce_ev_df.loc[(ce_ev_df['TA1-TA2'] == team) & (ce_ev_df['ce'] == ce) &
                              (ce_ev_df['instance_status'] == 'no'), :].shape[0] > 0:
                matrix_event_df.at[team, ce] = 'no match'
                matrix_event_percentage_df.at[team, ce] = 'no match'
                matrix_event_relevance_df.at[team, ce] = 'no match'
                matrix_event_schema_order_df.at[team, ce] = 'no match'
                matrix_event_precision_df.at[team, ce] = 'no match'
                matrix_event_recall_df.at[team, ce] = 'no match'
                matrix_event_recall_extrarel_approx_df.at[team, ce] = 'no match'
                matrix_event_f1_df.at[team, ce] = 'no match'
                matrix_pred_event_has_ref_df.at[team, ce] = 'no match'
                matrix_per_pred_event_has_ref_df.at[team, ce] = 'no match'
                matrix_num_hidden_df.at[team, ce] = 'no match'
                matrix_num_pred_match_hidden_df.at[team, ce] = 'no match'
                matrix_per_pred_match_hidden_df.at[team, ce] = 'no match'

            if ce_arg_df.loc[(ce_arg_df['TA1-TA2'] == team) & (ce_arg_df['ce'] == ce) &
                             (ce_arg_df['instance_status'] == 'yes'), :].shape[0] > 0:
                num_arguments_assessed = ce_arg_df.loc[(ce_arg_df['TA1-TA2'] == team) &
                                                       (ce_arg_df['ce'] == ce) &
                                                       (ce_arg_df['instance_status']
                                                        == 'yes'),
                                                       'num_arguments_assessed'].item()
                num_arg_with_matched_ep = ce_arg_df.loc[(ce_arg_df['TA1-TA2'] == team) &
                                                        (ce_arg_df['ce'] == ce) &
                                                        (ce_arg_df['instance_status'] == 'yes'),
                                                        'num_arg_with_matched_ep'].item()
                matrix_argument_df.at[team, ce] = num_arguments_assessed
                try:
                    matrix_per_assessed_arg_df.at[team, ce] = num_arguments_assessed \
                        / num_arg_with_matched_ep * 100
                except ZeroDivisionError:
                    matrix_per_assessed_arg_df.at[team, ce] = 0

                argument_precision = ce_arg_df.loc[(ce_arg_df['TA1-TA2'] == team) &
                                                   (ce_arg_df['ce'] == ce) &
                                                   (ce_arg_df['instance_status'] == 'yes'),
                                                   'precision_matchwextrarel'].item()
                matrix_argument_precision_df.at[team, ce] = argument_precision
                argument_recall = ce_arg_df.loc[(ce_arg_df['TA1-TA2'] == team) &
                                                (ce_arg_df['ce'] == ce) &
                                                (ce_arg_df['instance_status'] == 'yes'),
                                                'recall_matchwextrarel'].item()
                matrix_argument_recall_df.at[team, ce] = argument_recall
                argument_f1 = ce_arg_df.loc[(ce_arg_df['TA1-TA2'] == team) &
                                            (ce_arg_df['ce'] == ce) &
                                            (ce_arg_df['instance_status'] == 'yes'),
                                            'f1_match_matchwextrarel'].item()
                matrix_argument_f1_df.at[team, ce] = argument_f1

                num_arg_assessed_at_20 = ce_arg_df.loc[(ce_arg_df['TA1-TA2'] == team) &
                                                       (ce_arg_df['ce'] == ce) &
                                                       (ce_arg_df['instance_status'] == 'yes'),
                                                       'num_arg_assessed_at_20'].item()
                precision_matchwextrarel_at_20 = ce_arg_df.loc[
                    (ce_arg_df['TA1-TA2'] == team) &
                    (ce_arg_df['ce'] == ce) &
                    (ce_arg_df['instance_status']
                     == 'yes'),
                    'precision_matchwextrarel_at_20'].item()
                matrix_arg_precision_20_df.at[team, ce] = precision_matchwextrarel_at_20
                recall_matchwextrarel_at_20 = ce_arg_df.loc[
                    (ce_arg_df['TA1-TA2'] == team) &
                    (ce_arg_df['ce'] == ce) &
                    (ce_arg_df['instance_status'] == 'yes'),
                    'recall_matchwextrarel_at_20'].item()
                matrix_arg_recall_20_df.at[team, ce] = recall_matchwextrarel_at_20

                if num_arg_assessed_at_20 < 20:
                    matrix_arg_precision_20_df.at[team, ce] = "less than 20"
                    matrix_arg_recall_20_df.at[team, ce] = "less than 20"

                if num_arguments_assessed == 0:
                    matrix_per_assessed_arg_df.at[team, ce] = 'not assessed'
                    matrix_argument_precision_df.at[team, ce] = 'not assessed'
                    matrix_argument_recall_df.at[team, ce] = 'not assessed'
                    matrix_argument_f1_df.at[team, ce] = 'not assessed'
                    matrix_pred_argument_df.at[team, ce] = 'not assessed'
                    matrix_num_pred_as_arg_df.at[team, ce] = 'not assessed'
                    matrix_per_pred_as_arg_df.at[team, ce] = 'not assessed'
                    matrix_num_pred_as_arg_plau_df.at[team, ce] = 'not assessed'
                    matrix_num_pred_as_arg_implau_df.at[team, ce] = 'not assessed'
                    matrix_per_arg_plau_implau_df.at[team, ce] = 'not assessed'
                    matrix_arg_precision_20_df.at[team, ce] = 'not assessed'
                    matrix_arg_recall_20_df.at[team, ce] = 'not assessed'

            elif ce_arg_df.loc[(ce_arg_df['TA1-TA2'] == team) & (ce_arg_df['ce'] == ce) &
                               (ce_arg_df['instance_status'] == 'no'), :].shape[0] > 0:
                matrix_argument_df.at[team, ce] = 'no match'
                matrix_per_assessed_arg_df.at[team, ce] = 'no match'
                matrix_argument_precision_df.at[team, ce] = 'no match'
                matrix_argument_recall_df.at[team, ce] = 'no match'
                matrix_argument_f1_df.at[team, ce] = 'no match'
                matrix_pred_argument_df.at[team, ce] = 'no match'
                matrix_num_pred_as_arg_df.at[team, ce] = 'no match'
                matrix_per_pred_as_arg_df.at[team, ce] = 'no match'
                matrix_num_pred_as_arg_plau_df.at[team, ce] = 'no match'
                matrix_num_pred_as_arg_implau_df.at[team, ce] = 'no match'
                matrix_per_arg_plau_implau_df.at[team, ce] = 'no match'
                matrix_arg_precision_20_df.at[team, ce] = 'no match'
                matrix_arg_recall_20_df.at[team, ce] = 'no match'

            if ce_pred_df.loc[(ce_pred_df['TA1-TA2'] == team) & (ce_pred_df['ce'] == ce) &
                              (ce_pred_df['instance_status'] == 'yes'), :].shape[0] > 0:

                num_pred_events = ce_pred_df.loc[(ce_pred_df['TA1-TA2'] == team) &
                                                 (ce_pred_df['ce'] == ce) &
                                                 (ce_pred_df['instance_status']
                                                  == 'yes'),
                                                 'num_events_predicted'].item()
                matrix_all_pred_event_df.at[team, ce] = num_pred_events
                num_pred_events_assessed = ce_pred_df.loc[(ce_pred_df['TA1-TA2'] == team) &
                                                          (ce_pred_df['ce'] == ce) &
                                                          (ce_pred_df['instance_status']
                                                           == 'yes'),
                                                          'num_pred_events_assessed'].item()
                matrix_num_pred_as_event_df.at[team, ce] = num_pred_events_assessed
                num_pred_events_assessed_plausible = ce_pred_df.loc[
                    (ce_pred_df['TA1-TA2'] == team) &
                    (ce_pred_df['ce'] == ce) &
                    (ce_pred_df['instance_status']
                     == 'yes'),
                    'num_pred_events_assessed_plausible'].\
                    item()
                matrix_num_pred_as_event_plau_df.at[team, ce] = num_pred_events_assessed_plausible
                num_pred_events_assessed_implausible = ce_pred_df.loc[
                    (ce_pred_df['TA1-TA2'] == team) &
                    (ce_pred_df['ce'] == ce) &
                    (ce_pred_df['instance_status']
                     == 'yes'),
                    'num_pred_events_assessed_implausible'].\
                    item()
                matrix_num_pred_as_event_implau_df.at[team,
                                                      ce] = num_pred_events_assessed_implausible

                percentage_pred_events_assessed = ce_pred_df.loc[
                    (ce_pred_df['TA1-TA2'] == team) &
                    (ce_pred_df['ce'] == ce) &
                    (ce_pred_df['instance_status']
                     == 'yes'),
                    'percentage_pred_events_assessed'].item()
                matrix_per_plau_ev_vs_all_df.at[team, ce] = percentage_pred_events_assessed
                precision_pred_events_assessed = ce_pred_df.loc[
                    (ce_pred_df['TA1-TA2'] == team) &
                    (ce_pred_df['ce'] == ce) &
                    (ce_pred_df['instance_status']
                     == 'yes'),
                    'precision_pred_events_assessed'].item()
                matrix_per_ev_plau_implau_df.at[team, ce] = precision_pred_events_assessed

                num_arguments_predicted = ce_pred_df.loc[
                    (ce_pred_df['TA1-TA2'] == team) &
                    (ce_pred_df['ce'] == ce) &
                    (ce_pred_df['instance_status']
                     == 'yes'),
                    'num_arguments_predicted'].item()
                matrix_pred_argument_df.at[team, ce] = num_arguments_predicted

                num_pred_arguments_assessed = ce_pred_df.loc[
                    (ce_pred_df['TA1-TA2'] == team) &
                    (ce_pred_df['ce'] == ce) &
                    (ce_pred_df['instance_status']
                     == 'yes'),
                    'num_pred_arguments_assessed'].item()
                matrix_num_pred_as_arg_df.at[team, ce] = num_pred_arguments_assessed
                num_pred_arguments_assessed_plausible = ce_pred_df.loc[
                    (ce_pred_df['TA1-TA2'] == team) &
                    (ce_pred_df['ce'] == ce) &
                    (ce_pred_df['instance_status']
                     == 'yes'),
                    'num_pred_arguments_assessed_plausible'].item()
                matrix_num_pred_as_arg_plau_df.at[team, ce] = num_pred_arguments_assessed_plausible
                num_pred_arguments_assessed_implausible = ce_pred_df.loc[
                    (ce_pred_df['TA1-TA2'] == team) &
                    (ce_pred_df['ce'] == ce) &
                    (ce_pred_df['instance_status']
                     == 'yes'),
                    'num_pred_arguments_assessed_implausible'].item()
                matrix_num_pred_as_arg_implau_df.at[team,
                                                    ce] = num_pred_arguments_assessed_implausible

                percentage_pred_arguments_assessed = ce_pred_df.loc[
                    (ce_pred_df['TA1-TA2'] == team) &
                    (ce_pred_df['ce'] == ce) &
                    (ce_pred_df['instance_status']
                     == 'yes'),
                    'percentage_pred_arguments_assessed'].item()
                matrix_per_pred_as_arg_df.at[team, ce] = percentage_pred_arguments_assessed
                precision_pred_arguments_assessed = ce_pred_df.loc[
                    (ce_pred_df['TA1-TA2'] == team) &
                    (ce_pred_df['ce'] == ce) &
                    (ce_pred_df['instance_status']
                     == 'yes'),
                    'precision_pred_arguments_assessed'].item()
                matrix_per_arg_plau_implau_df.at[team, ce] = precision_pred_arguments_assessed

                if num_pred_events_assessed == 0:
                    matrix_num_pred_as_event_plau_df.at[team, ce] = "not assessed"
                    matrix_num_pred_as_event_implau_df.at[team, ce] = "not assessed"
                    matrix_per_plau_ev_vs_all_df.at[team, ce] = 'not assessed'
                    matrix_per_ev_plau_implau_df.at[team, ce] = 'not assessed'

                if num_pred_arguments_assessed == 0:
                    matrix_num_pred_as_arg_df.at[team, ce] = 'not assessed'
                    matrix_per_pred_as_arg_df.at[team, ce] = 'not assessed'
                    matrix_num_pred_as_arg_plau_df.at[team, ce] = 'not assessed'
                    matrix_num_pred_as_arg_implau_df.at[team, ce] = 'not assessed'
                    matrix_per_arg_plau_implau_df.at[team, ce] = 'not assessed'

            elif ce_pred_df.loc[(ce_pred_df['TA1-TA2'] == team) & (ce_pred_df['ce'] == ce) &
                                (ce_pred_df['instance_status'] == 'no'), :].shape[0] > 0:
                num_pred_events = ce_pred_df.loc[(ce_pred_df['TA1-TA2'] == team) &
                                                 (ce_pred_df['ce'] == ce) &
                                                 (ce_pred_df['instance_status']
                                                  == 'no'),
                                                 'num_events_predicted'].max()
                num_arguments_predicted = ce_pred_df.loc[
                    (ce_pred_df['TA1-TA2'] == team) &
                    (ce_pred_df['ce'] == ce) &
                    (ce_pred_df['instance_status']
                     == 'no'),
                    'num_arguments_predicted'].max()
                matrix_all_pred_event_df.at[team, ce] = num_pred_events
                matrix_pred_argument_df.at[team, ce] = num_arguments_predicted
                matrix_num_pred_as_event_df.at[team, ce] = 'no match'
                matrix_num_pred_as_event_plau_df.at[team, ce] = 'no match'
                matrix_num_pred_as_event_implau_df.at[team, ce] = 'no match'
                matrix_all_pred_event_df.at[team, ce] = 'no match'
                matrix_per_plau_ev_vs_all_df.at[team, ce] = 'no match'
                matrix_per_ev_plau_implau_df.at[team, ce] = 'no match'
                matrix_num_pred_as_arg_df.at[team, ce] = 'no match'
                matrix_per_pred_as_arg_df.at[team, ce] = 'no match'
                matrix_per_arg_plau_implau_df.at[team, ce] = 'no match'
                matrix_num_pred_as_event_plau_df.at[team, ce] = 'no match'
                matrix_num_pred_as_event_implau_df.at[team, ce] = 'no match'
                matrix_num_pred_as_arg_plau_df.at[team, ce] = 'no match'
                matrix_num_pred_as_arg_implau_df.at[team, ce] = 'no match'

    writer_ev = pd.ExcelWriter(os.path.join(output_dir, "Assessment_Event_summary.xlsx"))
    matrix_event_df.to_excel(writer_ev, sheet_name="Num of Assessed Event", index=True)
    matrix_event_percentage_df.to_excel(writer_ev, sheet_name="Per of Assessed Event", index=True)
    matrix_event_relevance_df.to_excel(
        writer_ev, sheet_name="Event_Schema Relevance Score", index=True)
    matrix_event_schema_order_df.to_excel(
        writer_ev, sheet_name="Event_Assessed Schema Order", index=True)
    matrix_event_precision_df.to_excel(writer_ev, sheet_name="Event Precision", index=True)
    matrix_event_recall_df.to_excel(writer_ev, sheet_name="Event Recall", index=True)
    # matrix_event_recall_extrarel_approx_df.to_excel(
    #     writer_ev, sheet_name="Event Recall w extrarel", index=True)
    matrix_event_f1_df.to_excel(writer_ev, sheet_name="Event F1", index=True)
    matrix_all_pred_event_df.to_excel(writer_ev, sheet_name="Num of all Pred Events", index=True)
    matrix_pred_event_has_ref_df.to_excel(
        writer_ev, sheet_name="Num of Pred Events match Ann", index=True)
    matrix_per_pred_event_has_ref_df.to_excel(
        writer_ev, sheet_name="Per of Pred Events match Ann", index=True)
    matrix_num_pred_as_event_df.to_excel(
        writer_ev, sheet_name="Num of Assessed Unmatched Pred", index=True)
    matrix_num_pred_as_event_plau_df.to_excel(
        writer_ev, sheet_name="Num of Plausible Pred", index=True)
    matrix_num_pred_as_event_implau_df.to_excel(
        writer_ev, sheet_name="Num of Implausible Pred", index=True)
    matrix_per_plau_ev_vs_all_df.to_excel(
        writer_ev, sheet_name="Per of Plausible Pred Events", index=True)
    matrix_per_ev_plau_implau_df.to_excel(
        writer_ev, sheet_name="Per of Plaus vs Implaus Events", index=True)

    matrix_num_hidden_df.to_excel(writer_ev, sheet_name="Num of Hidden Ann Event", index=True)
    matrix_num_pred_match_hidden_df.to_excel(
        writer_ev, sheet_name="Num of Matched Hidden Ann", index=True)
    matrix_per_pred_match_hidden_df.to_excel(
        writer_ev, sheet_name="Per of Matched Hidden Ann", index=True)

    writer_arg = pd.ExcelWriter(os.path.join(output_dir, "Assessment_Argument_summary.xlsx"))
    matrix_argument_df.to_excel(writer_arg, sheet_name="Num of Assessed Args", index=True)
    matrix_per_assessed_arg_df.to_excel(writer_arg, sheet_name="Per of Assessed Args", index=True)
    matrix_argument_precision_df.to_excel(writer_arg, sheet_name="Argument Precision", index=True)
    matrix_argument_recall_df.to_excel(writer_arg, sheet_name="Argument Recall", index=True)
    matrix_argument_f1_df.to_excel(writer_arg, sheet_name="Argument F1", index=True)
    matrix_arg_precision_20_df.to_excel(writer_arg, sheet_name="Argument Precision@20", index=True)
    matrix_arg_recall_20_df.to_excel(writer_arg, sheet_name="Argument Recall@20", index=True)
    matrix_pred_argument_df.to_excel(writer_arg, sheet_name="Num of all Pred Args", index=True)
    matrix_num_pred_as_arg_df.to_excel(
        writer_arg, sheet_name="Num of Assessed Unmatched Pred", index=True)
    matrix_per_pred_as_arg_df.to_excel(
        writer_arg, sheet_name="Per of Assessed Unmatched Pred", index=True)
    matrix_num_pred_as_arg_plau_df.to_excel(
        writer_arg, sheet_name="Num of Plausible Pred", index=True)
    matrix_num_pred_as_arg_implau_df.to_excel(
        writer_arg, sheet_name="Num of Imlausible Pred", index=True)
    matrix_per_arg_plau_implau_df.to_excel(
        writer_arg, sheet_name="Per of Plaus vs Implaus", index=True)

    writer_ev.save()
    writer_arg.save()
    writer_ev.close()
    writer_arg.close()


def assess_ce_row(ta2_ceinstance, assessment_collection):
    as_ce_df = assessment_collection.ce_df
    as_ce_df = as_ce_df.loc[as_ce_df['schema_instance_id'] ==
                            ta2_ceinstance.schema_instance_id, :].copy()

    # We have to address EMPTY_NA, so we make it missing
    as_ce_df.loc[as_ce_df['schema_name_relevance'] == "EMPTY_NA", 'schema_name_relevance'] = np.nan

    ce_row = {'file_name': ta2_ceinstance.ce_instance_file_name_base,
              'schema_instance_id': ta2_ceinstance.schema_instance_id,
              'ta1_team': ta2_ceinstance.ta1_team_name,
              'ta2_team': ta2_ceinstance.ta2_team_name,
              'ce': ta2_ceinstance.ce_name,
              'assessment': 'yes',
              'instance_status': as_ce_df['instance_status'].to_string(index=False),
              'schema_name_relevance': pd.to_numeric(as_ce_df['schema_name_relevance'],
                                                     downcast='integer').to_string(index=False)}
    return ce_row


def assess_ep_row(task1_ceannotation, ta2_ceinstance, assessment_collection):
    as_ev_df = assessment_collection.ep_df
    as_ev_df = as_ev_df.loc[as_ev_df['schema_instance_id'] == ta2_ceinstance.schema_instance_id, :]
    ev_df = ta2_ceinstance.ev_df
    num_events = ev_df.shape[0]
    ins_ev_df = get_ta2_instantiated_events(ev_df).copy()
    num_events_instantiated = len(ins_ev_df)
    pred_ev_df = get_ta2_predicted_events(ev_df).copy()
    num_events_predicted = len(pred_ev_df)
    ev_df['system_ep_id'] = ev_df['ev_id'].str.split('/').str[0:2].str.join("/")
    ev_df = ev_df.merge(as_ev_df, how="outer", on=["schema_instance_id", "system_ep_id"])
    partition_df = task1_ceannotation.ep_partition_df

    # Do a check
    if ev_df.shape[0] > num_events:
        print("There Exists Assessed events not in Event Extraction for schema_instance_id {}".
              format(ta2_ceinstance.schema_instance_id))
    ins_ev_df['system_ep_id'] = ins_ev_df['ev_id'].str.split('/').str[0:2].str.join("/")
    ins_ev_df = ins_ev_df.merge(as_ev_df, how="left", on=["schema_instance_id", "system_ep_id"])
    pred_ev_df['system_ep_id'] = pred_ev_df['ev_id'].str.split('/').str[0:2].str.join("/")
    pred_ev_df = pred_ev_df.merge(as_ev_df, how="left", on=["schema_instance_id", "system_ep_id"])
    # Separate out the number of assessed events instantiated
    num_events_assessed = ev_df.loc[pd.notna(ev_df['ep_match_status']), :].shape[0]
    num_events_instantiated_assessed = ins_ev_df.loc[pd.notna(
        ins_ev_df['ep_match_status']), :].shape[0]
    num_events_predicted_assessed = pred_ev_df.loc[pd.notna(
        pred_ev_df['ep_match_status']), :].shape[0]
    num_events_match = ev_df.loc[ev_df['ep_match_status'] == 'match', :].shape[0]
    num_events_extrarel = ev_df.loc[ev_df['ep_match_status'] == 'extra-rel', :].shape[0]
    num_events_extrairrel = ev_df.loc[ev_df['ep_match_status'] == 'extra-irrel', :].shape[0]

    num_ann_ev_match = len(ev_df.loc[(ev_df['ep_match_status'] == 'match') &
                                     (ev_df['reference_ep_id'].str.contains('EMPTY') == False),
                                     'reference_ep_id'].unique())
    num_ann_ev_matchwextrarel = len(ev_df.loc[((ev_df['ep_match_status'] == 'match') |
                                               (ev_df['ep_match_status'] == 'match-inexact') |
                                               (ev_df['ep_match_status'] == 'extra-rel')) &
                                              (ev_df['reference_ep_id'].str.contains(
                                                  'EMPTY') == False),
                                              'reference_ep_id'].unique())
    num_ann_events = len(task1_ceannotation.ep_df)

    num_pred_events_match = len(pred_ev_df.loc[pred_ev_df['ep_match_status']
                                               == 'match', 'system_ep_id'].unique())
    num_pred_events_extrarel = len(pred_ev_df.loc[pred_ev_df['ep_match_status']
                                                  == 'extra-rel', 'system_ep_id'].unique())
    num_pred_events_extrairrel = len(pred_ev_df.loc[pred_ev_df['ep_match_status']
                                                    == 'extra-irrel', 'system_ep_id'].unique())

    num_ann_hidden_events = len(partition_df.loc[partition_df['partition'] == 'hidden', :])
    num_ann_exposed_events = len(partition_df.loc[partition_df['partition'] == 'exposed', :])

    partition_match_df = partition_df.merge(pred_ev_df, how="left", left_on="eventprimitive_id",
                                            right_on="reference_ep_id")
    num_pred_matched_hidden_events = len(partition_match_df.loc[
        (partition_match_df['ep_match_status']
         == 'match') &
        (partition_match_df['partition']
         == 'hidden'), "eventprimitive_id"].unique())
    per_pred_matched_hidden_events = num_pred_matched_hidden_events / num_ann_hidden_events * 100

    per_pred_events_match = num_pred_events_match / num_events_predicted * 100
    precision_match = num_events_match/num_events_assessed
    recall_match = num_ann_ev_match/num_ann_events
    precision_matchwextrarel = (num_events_match + num_events_extrarel)/num_events_assessed
    recall_matchwextrarel = 0  # computed later

    # Now do top k, with k = 20
    k = 20
    ev_df_k = ev_df.loc[pd.notna(ev_df['ep_match_status']), :]
    ev_df_k = ev_df_k.sort_values(by=["ev_confidence_val"], ascending=False)
    ev_df_k = ev_df_k.iloc[0:k, :]
    num_events_assessed_at_20 = ev_df_k.loc[pd.notna(ev_df_k['ep_match_status']), :].shape[0]
    num_events_match_at_20 = ev_df_k.loc[ev_df_k['ep_match_status'] == 'match', :].shape[0]
    num_events_extrarel_at_20 = ev_df_k.loc[ev_df_k['ep_match_status'] == 'extra-rel', :].shape[0]
    num_events_extrairrel_at_20 = \
        ev_df_k.loc[ev_df_k['ep_match_status'] == 'extra-irrel', :].shape[0]
    num_ann_ev_match_at_20 = len(ev_df_k.loc[(ev_df_k['ep_match_status'] == 'match') &
                                             (ev_df_k['reference_ep_id'].str.contains(
                                                 'EMPTY') == False),
                                             'reference_ep_id'].unique())
    precision_match_at_20 = num_events_match_at_20/num_events_assessed_at_20
    recall_match_at_20 = num_ann_ev_match_at_20/num_ann_events
    precision_matchwextrarel_at_20 = (num_events_match_at_20 +
                                      num_events_extrarel_at_20)/num_events_assessed_at_20
    f1_match = 2*(precision_match * recall_match)/(precision_match + recall_match)
    f1_matchwextrarel = 2*(precision_matchwextrarel * recall_matchwextrarel) \
        / (precision_matchwextrarel + recall_matchwextrarel)
    f1_match_at_20 = 2*(precision_match_at_20 * recall_match_at_20) / (precision_match_at_20 +
                                                                       recall_match_at_20)
    f1_matchwextrarel_at_20 = 2*(precision_matchwextrarel_at_20 *
                                 recall_match_at_20) / (precision_matchwextrarel_at_20 +
                                                        recall_match_at_20)

    ep_row = {'file_name': ta2_ceinstance.ce_instance_file_name_base,
              'schema_instance_id': ta2_ceinstance.schema_instance_id,
              'ta1_team': ta2_ceinstance.ta1_team_name,
              'ta2_team': ta2_ceinstance.ta2_team_name,
              'ce': ta2_ceinstance.ce_name, 'num_events': num_events,
              'num_events_instantiated': num_events_instantiated,
              'num_events_predicted': num_events_predicted,
              'num_events_assessed': num_events_assessed,
              'num_events_instantiated_assessed': num_events_instantiated_assessed,
              'num_events_predicted_assessed': num_events_predicted_assessed,
              'num_events_match': num_events_match, 'num_events_extrarel': num_events_extrarel,
              'num_events_extrairrel': num_events_extrairrel,
              'num_ann_events_matched': num_ann_ev_match,
              'num_ann_events': num_ann_events,
              'num_ann_hidden_events': num_ann_hidden_events,
              'num_ann_exposed_events': num_ann_exposed_events,
              'num_ann_ev_matchwextrarel': num_ann_ev_matchwextrarel,
              'per_pred_events_match': per_pred_events_match,
              'precision_match': precision_match, 'recall_match': recall_match,
              'precision_matchwextrarel': precision_matchwextrarel,
              'recall_matchwextrarel': recall_matchwextrarel,
              'num_events_assessed_at_20': num_events_assessed_at_20,
              'num_events_match_at_20': num_events_match_at_20,
              'num_events_extrarel_at_20': num_events_extrarel_at_20,
              'num_events_extrairrel_at_20': num_events_extrairrel_at_20,
              'num_ann_events_matched_at_20': num_ann_ev_match_at_20,
              'precision_match_at_20': precision_match_at_20,
              'recall_match_at_20': recall_match_at_20,
              'precision_matchwextrarel_at_20': precision_matchwextrarel_at_20,
              'num_pred_events_match': num_pred_events_match,
              'num_pred_events_extrarel': num_pred_events_extrarel,
              'num_pred_events_extrairrel': num_pred_events_extrairrel,
              'num_pred_matched_hidden_events': num_pred_matched_hidden_events,
              'per_pred_matched_hidden_events': per_pred_matched_hidden_events,
              'f1_match': f1_match,
              'f1_matchwextrarel': f1_matchwextrarel,
              'f1_match_at_20': f1_match_at_20,
              'f1_matchwextrarel_at_20': f1_matchwextrarel_at_20}
    return ep_row


def assess_ke_row(task1_ceannotation, ta2_ceinstance, assessment_collection):
    as_ke_df = assessment_collection.ke_df
    as_ke_df = as_ke_df.loc[as_ke_df['schema_instance_id']
                            == ta2_ceinstance.schema_instance_id, :].copy()
    as_ke_df['system_ke_id_without_ent'] = as_ke_df['system_ke_id'].str.split('_').str[0]
    as_ke_df['system_ep_ke_id'] = as_ke_df[['system_ep_id',
                                            'system_ke_id_without_ent']].agg('_'.join, axis=1)
    as_ev_df = assessment_collection.ep_df
    as_ev_df = as_ev_df.loc[as_ev_df['schema_instance_id']
                            == ta2_ceinstance.schema_instance_id, :].copy()

    arg_df = ta2_ceinstance.arg_df
    num_arguments = arg_df.shape[0]
    arg_df['system_ep_id'] = arg_df['ev_id'].str.split('/').str[0:2].str.join("/")
    arg_df['system_ke_id'] = arg_df['arg_id'].str.split('/').str[0:2].str.join("/")
    arg_df['system_ep_ke_id'] = arg_df['system_ep_id'] + '_' + arg_df['system_ke_id']
    arg_df = arg_df.merge(as_ke_df, how="outer", on=["schema_instance_id", "system_ep_ke_id",
                                                     "system_ep_id", "system_ke_id"])
    arg_df = arg_df.merge(as_ev_df, how="outer", on=["schema_instance_id", "system_ep_id"])

    num_arg_with_matched_ep = len(arg_df.loc[(arg_df['ep_match_status'] == 'match') |
                                             (arg_df['ep_match_status'] == 'extra-rel'),
                                             'system_ke_id'].unique())

    num_ann_arguments = len(task1_ceannotation.arg_df.loc[(task1_ceannotation.
                                                           arg_df['eventprimitive_id'].str.
                                                           contains('EMPTY') == False), 'arg_id'])
    num_ann_arg_match = len(arg_df.loc[(arg_df['ke_match_status'] == 'match') &
                                       (arg_df['reference_ke_id'].str.contains('EMPTY') == False),
                                       'reference_ke_id'].unique())
    num_ann_arg_match_inexact = len(arg_df.loc[(arg_df['ke_match_status'] == 'match-inexact') &
                                               (arg_df['reference_ke_id'].str.contains(
                                                   'EMPTY') == False),
                                               'reference_ke_id'].unique())
    num_ann_arg_match_extrarel = len(arg_df.loc[(arg_df['ke_match_status'] == 'extra-rel') &
                                     (arg_df['reference_ke_id'].str.contains('EMPTY') == False),
                                     'reference_ke_id'].unique())
    num_ann_arg_match_with_inexact = len(arg_df.loc[
        ((arg_df['ke_match_status'] == 'match') |
         (arg_df['ke_match_status'] == 'match-inexact')) &
        (arg_df['reference_ke_id'].str.contains(
            'EMPTY') == False),
        'reference_ke_id'].unique())
    num_ann_arg_match_with_inexact_extrarel = len(arg_df.loc[
        ((arg_df['ke_match_status'] == 'match') |
         (arg_df['ke_match_status'] == 'match-inexact') |
         (arg_df['ke_match_status'] == 'extra-rel')) &
        (arg_df['reference_ke_id'].str.contains(
            'EMPTY') == False),
        'reference_ke_id'].unique())

    num_arguments_assessed = arg_df.loc[pd.notna(arg_df['ke_match_status']), :].shape[0]

    num_arguments_match_exact = arg_df.loc[arg_df['ke_match_status'] == 'match', :].shape[0]
    num_arguments_matchwinexact = arg_df.loc[(arg_df['ke_match_status'] == 'match') |
                                             (arg_df['ke_match_status'] == 'match-inexact'),
                                             :].shape[0]
    num_arguments_matchwextrarel = arg_df.loc[(arg_df['ke_match_status'] == 'match') |
                                              (arg_df['ke_match_status'] == 'match-inexact') |
                                              (arg_df['ke_match_status'] == 'extra-rel'), :].\
        shape[0]
    percentage_arguments_match = num_arguments_match_exact / num_arguments_assessed * 100
    percentage_arguments_matchwinexact = num_arguments_matchwinexact /\
        num_arguments_assessed * 100
    percentage_arguments_matchwextrarel = num_arguments_matchwextrarel /\
        num_arguments_assessed * 100
    precision_match_exact = num_arguments_match_exact / num_arguments_assessed
    recall_match_exact = num_ann_arg_match/num_ann_arguments
    try:
        f1_match_exact = (2*precision_match_exact*recall_match_exact) /\
            (precision_match_exact+recall_match_exact)
    except ZeroDivisionError:
        f1_match_exact = 0
    precision_matchwinexact = num_arguments_matchwinexact / num_arguments_assessed
    recall_matchwinexact = num_ann_arg_match_with_inexact/num_ann_arguments
    try:
        f1_match_matchwinexact = (2*precision_matchwinexact*recall_matchwinexact) /\
            (precision_matchwinexact+recall_matchwinexact)
    except ZeroDivisionError:
        f1_match_matchwinexact = 0
    precision_matchwextrarel = num_arguments_matchwextrarel / num_arguments_assessed
    recall_matchwextrarel = num_ann_arg_match_with_inexact_extrarel/num_ann_arguments
    try:
        f1_match_matchwextrarel = (2*precision_matchwextrarel*recall_matchwextrarel) /\
            (precision_matchwextrarel+recall_matchwextrarel)
    except ZeroDivisionError:
        f1_match_matchwextrarel = 0

    k = 20
    arg_df_k = arg_df.loc[pd.notna(arg_df['ke_match_status']), :]
    arg_df_k = arg_df_k.sort_values(by=["arg_ta2confidence"], ascending=False)
    arg_df_k = arg_df_k.iloc[0:k, :]

    num_arg_assessed_at_20 = arg_df_k.loc[pd.notna(arg_df_k['ke_match_status']), :].shape[0]
    num_arg_match_at_20 = arg_df_k.loc[arg_df_k['ke_match_status'] == 'match', :].shape[0]
    num_arg_inexact_at_20 = arg_df_k.loc[arg_df_k['ke_match_status'] == 'match-inexact', :].shape[0]
    num_arg_extrarel_at_20 = arg_df_k.loc[arg_df_k['ke_match_status'] == 'extra-rel', :].shape[0]
    num_arg_extrairrel_at_20 = \
        arg_df_k.loc[arg_df_k['ke_match_status'] == 'extra-irrel', :].shape[0]

    num_arg_matchwextrarel_at_20 = arg_df_k.loc[(arg_df_k['ke_match_status'] == 'match') |
                                                (arg_df_k['ke_match_status'] == 'match-inexact') |
                                                (arg_df_k['ke_match_status']
                                                == 'extra-rel'), :].shape[0]

    num_ann_arg_match_at_20 = len(arg_df_k.loc[(arg_df_k['ke_match_status'] == 'match') &
                                               (arg_df_k['reference_ke_id'].str.contains(
                                                   'EMPTY') == False),
                                               'reference_ke_id'].unique())
    num_ann_arg_matchwextrarel_at_20 = len(arg_df_k.loc[
        ((arg_df_k['ke_match_status'] == 'match') |
         (arg_df_k['ke_match_status'] == 'match-inexact') |
         (arg_df_k['ke_match_status'] == 'extra-rel')) &
        (arg_df_k['reference_ke_id'].str.contains(
            'EMPTY') == False),
        'reference_ke_id'].unique())
    precision_match_at_20 = num_arg_match_at_20/num_arg_assessed_at_20
    recall_match_at_20 = num_ann_arg_match_at_20/num_ann_arguments
    precision_matchwextrarel_at_20 = num_arg_matchwextrarel_at_20/num_arg_assessed_at_20
    recall_matchwextrarel_at_20 = num_ann_arg_matchwextrarel_at_20/num_ann_arguments

    ke_row = {'file_name': ta2_ceinstance.ce_instance_file_name_base,
              'schema_instance_id': ta2_ceinstance.schema_instance_id,
              'ta1_team': ta2_ceinstance.ta1_team_name,
              'ta2_team': ta2_ceinstance.ta2_team_name,
              'ce': ta2_ceinstance.ce_name,
              'num_arguments': num_arguments,
              'num_ann_arguments': num_ann_arguments,
              'num_ann_arg_match': num_ann_arg_match,
              'num_ann_arg_match_inexact': num_ann_arg_match_inexact,
              'num_ann_arg_match_extrarel': num_ann_arg_match_extrarel,
              'num_arguments_assessed': num_arguments_assessed,
              'num_arg_with_matched_ep': num_arg_with_matched_ep,
              'num_arguments_match_exact': num_arguments_match_exact,
              'num_arguments_matchwinexact': num_arguments_matchwinexact,
              'num_arguments_matchwextrarel': num_arguments_matchwextrarel,
              'percentage_arguments_match': percentage_arguments_match,
              'percentage_arguments_matchwinexact': percentage_arguments_matchwinexact,
              'percentage_arguments_matchwextrarel': percentage_arguments_matchwextrarel,
              'precision_match_exact': precision_match_exact,
              'recall_match_exact': recall_match_exact,
              'f1_match_exact': f1_match_exact,
              'precision_matchwinexact': precision_matchwinexact,
              'recall_matchwinexact': recall_matchwinexact,
              'f1_match_matchwinexact': f1_match_matchwinexact,
              'precision_matchwextrarel': precision_matchwextrarel,
              'recall_matchwextrarel': recall_matchwextrarel,
              'f1_match_matchwextrarel': f1_match_matchwextrarel,
              'num_arg_assessed_at_20': num_arg_assessed_at_20,
              'num_arg_match_at_20': num_arg_match_at_20,
              'num_arg_inexact_at_20': num_arg_inexact_at_20,
              'num_arg_extrarel_at_20': num_arg_extrarel_at_20,
              'num_arg_extrairrel_at_20': num_arg_extrairrel_at_20,
              'num_ann_arg_match_at_20': num_ann_arg_match_at_20,
              'num_ann_arg_matchwextrarel_at_20': num_ann_arg_matchwextrarel_at_20,
              'precision_match_at_20': precision_match_at_20,
              'recall_match_at_20': recall_match_at_20,
              'precision_matchwextrarel_at_20': precision_matchwextrarel_at_20,
              'recall_matchwextrarel_at_20': recall_matchwextrarel_at_20
              }
    return ke_row


def assess_pred_row(ta2_ceinstance, assessment_collection):
    as_pred_df = assessment_collection.pred_plaus_df.copy()
    as_pred_df = as_pred_df.loc[as_pred_df['schema_instance_id']
                                == ta2_ceinstance.schema_instance_id, :]
    as_pred_df['system_ep_ke_id'] = as_pred_df['system_ep_id'] + '_' + as_pred_df['system_ke_id']

    as_pred_arg_df = assessment_collection.pred_arg_df.copy()
    as_pred_arg_df = as_pred_arg_df.loc[as_pred_arg_df['schema_instance_id']
                                        == ta2_ceinstance.schema_instance_id, :]
    as_pred_arg_df['system_ep_ke_id'] = as_pred_arg_df['system_ep_id'] + \
        '_' + as_pred_arg_df['system_ke_id']

    ev_df = ta2_ceinstance.ev_df
    num_events = ev_df.shape[0]
    pred_ev_df = get_ta2_predicted_events(ev_df).copy()
    num_events_predicted = len(pred_ev_df)
    ev_df['system_ep_id'] = ev_df['ev_id'].str.split('/').str[0:2].str.join("/")
    ev_df = ev_df.merge(as_pred_df, how="outer", on=["schema_instance_id", "system_ep_id"])

    num_pred_events_assessed = len(ev_df.loc[pd.notna(ev_df['system_ep_id']) &
                                             (ev_df['system_ke_id'] == "EMPTY_NA"),
                                             'system_ep_id'].unique())
    num_pred_events_assessed_plausible = len(ev_df.loc[
        pd.notna(ev_df['system_ep_id']) &
        (ev_df['system_ke_id'] == "EMPTY_NA") &
        (ev_df['prediction_plausible_status'] == "yes"),
        'system_ep_id'].unique())
    num_pred_events_assessed_implausible = len(ev_df.loc[
        pd.notna(ev_df['system_ep_id']) &
        (ev_df['system_ke_id'] == "EMPTY_NA") &
        (ev_df['prediction_plausible_status'] == "no"),
        'system_ep_id'].unique())
    percentage_pred_events_assessed = num_pred_events_assessed / num_events_predicted * 100
    precision_pred_events_assessed = num_pred_events_assessed_plausible /\
        num_pred_events_assessed * 100
    recall_pred_events_assessed = num_pred_events_assessed_plausible / num_events_predicted
    F1_pred_events_assessed = (2 * precision_pred_events_assessed * recall_pred_events_assessed) /\
        (precision_pred_events_assessed + recall_pred_events_assessed)
    num_pred_events_justfi_yes = len(ev_df.loc[pd.notna(ev_df['system_ep_id']) &
                                               (ev_df['system_ke_id'] == "EMPTY_NA") &
                                               (ev_df['prediction_justification_status'] == "yes"),
                                               'system_ep_id'].unique())
    num_pred_events_justfi_no = len(ev_df.loc[pd.notna(ev_df['system_ep_id']) &
                                              (ev_df['system_ke_id'] == "EMPTY_NA") &
                                              (ev_df['prediction_justification_status'] == "no"),
                                              'system_ep_id'].unique())

    arg_df = ta2_ceinstance.arg_df
    num_arguments = arg_df.shape[0]
    arg_df['system_ep_id'] = arg_df['ev_id'].str.split('/').str[0:2].str.join("/")
    arg_df['system_ke_id'] = arg_df['arg_id'].str.split('/').str[0:2].str.join("/")
    arg_df['system_ep_ke_id'] = arg_df['system_ep_id'] + '_' + arg_df['system_ke_id']
    arg_df = arg_df.merge(as_pred_df, how="outer", on=["schema_instance_id", "system_ep_ke_id"])
    arg_df = arg_df.merge(as_pred_arg_df, how="outer", on=["schema_instance_id", "system_ep_ke_id"])

    num_arguments_predicted = len(arg_df.loc[arg_df['arg_ta2provenance'] == 'kairos:NULL',
                                             'system_ep_ke_id'].unique())
    num_pred_arguments_assessed = len(arg_df.loc[arg_df['system_ep_ke_id'].str.contains('EMPTY_NA'),
                                                 'system_ep_ke_id'].unique())
    num_pred_arguments_assessed_plausible = len(arg_df.loc[
        arg_df['system_ep_ke_id'].str.contains('EMPTY_NA') &
        (arg_df['prediction_justification_status'] == 'yes'),
        'system_ep_ke_id'].unique())
    num_pred_arguments_assessed_implausible = len(arg_df.loc[
        arg_df['system_ep_ke_id'].str.contains('EMPTY_NA') &
        (arg_df['prediction_justification_status'] == 'no'),
        'system_ep_ke_id'].unique())
    percentage_pred_arguments_assessed = num_pred_arguments_assessed / num_arguments_predicted * 100
    try:
        precision_pred_arguments_assessed = num_pred_arguments_assessed_plausible \
            / num_pred_arguments_assessed * 100
    except ZeroDivisionError:
        precision_pred_arguments_assessed = 0
    try:
        recall_pred_arguments_assessed = num_pred_arguments_assessed_plausible \
            / num_arguments_predicted
    except ZeroDivisionError:
        recall_pred_arguments_assessed = 0
    try:
        F1_pred_arguments_assessed = (2 * precision_pred_arguments_assessed
                                        * recall_pred_arguments_assessed) /\
            (precision_pred_arguments_assessed + recall_pred_arguments_assessed)
    except ZeroDivisionError:
        F1_pred_arguments_assessed = 0

    num_pred_arguments_incomplete = len(arg_df.loc[
                                        arg_df['incomplete_missing_status'] == 'incomplete',
                                        'system_ep_ke_id'].unique())
    num_pred_arguments_missing = len(arg_df.loc[arg_df['incomplete_missing_status'] == 'missing',
                                                'system_ep_ke_id'].unique())

    pred_row = {'file_name': ta2_ceinstance.ce_instance_file_name_base,
                'schema_instance_id': ta2_ceinstance.schema_instance_id,
                'ta1_team': ta2_ceinstance.ta1_team_name,
                'ta2_team': ta2_ceinstance.ta2_team_name,
                'ce': ta2_ceinstance.ce_name,
                'num_events': num_events,
                'num_events_predicted': num_events_predicted,
                'num_pred_events_assessed': num_pred_events_assessed,
                'num_pred_events_assessed_plausible': num_pred_events_assessed_plausible,
                'num_pred_events_assessed_implausible': num_pred_events_assessed_implausible,
                'percentage_pred_events_assessed': percentage_pred_events_assessed,
                'precision_pred_events_assessed': precision_pred_events_assessed,
                'recall_pred_events_assessed': recall_pred_events_assessed,
                'F1_pred_events_assessed': F1_pred_events_assessed,
                'num_pred_events_justfi_yes': num_pred_events_justfi_yes,
                'num_pred_events_justfi_no': num_pred_events_justfi_no,
                'num_arguments': num_arguments,
                'num_arguments_predicted': num_arguments_predicted,
                'num_pred_arguments_assessed': num_pred_arguments_assessed,
                'num_pred_arguments_assessed_plausible': num_pred_arguments_assessed_plausible,
                'num_pred_arguments_assessed_implausible': num_pred_arguments_assessed_implausible,
                'percentage_pred_arguments_assessed': percentage_pred_arguments_assessed,
                'precision_pred_arguments_assessed': precision_pred_arguments_assessed,
                'recall_pred_arguments_assessed': recall_pred_arguments_assessed,
                'F1_pred_arguments_assessed': F1_pred_arguments_assessed,
                'num_pred_arguments_incomplete': num_pred_arguments_incomplete,
                'num_pred_arguments_missing': num_pred_arguments_missing}
    return pred_row


def assess_task1_submissions(output_dir, task1_annotation_collection,
                             ta2_task1_collection, assessment_collection):

    ta2_ev_df = pd.DataFrame(columns=["schema_instance_id", "ta1_team_name", "ta2_team_name",
                                      "task", "ce_id",
                                      "precision_class", "precision_complex", "precision_jc",
                                      "precision_text", "precision_topsim", "precision_transe",
                                      "recall_class", "recall_complex", "recall_jc",
                                      "recall_text", "recall_topsim", "recall_transe",
                                      "f1_class", "f1_complex", "f1_jc",
                                      "f1_text", "f1_topsim", "f1_transe"
                                      ])
    ce_rows = []
    ev_rows = []
    ke_rows = []
    pred_rows = []
    ta2_team_list = ta2_task1_collection.ta2dict.keys()

    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    for ta2_team in ta2_team_list:

        if not os.path.isdir(os.path.join(output_dir, ta2_team)):
            os.makedirs(os.path.join(output_dir, ta2_team))

        ta2_instance = ta2_task1_collection.ta2dict[ta2_team]
        ce_list = pd.Series([key.split('.')[0].split('|')[2].lower()
                            for key in ta2_instance.ta2dict.keys()]).unique().tolist()
        for ce in ce_list:
            ta2_ce_items = [(key, value) for key, value
                            in ta2_instance.ta2dict.items()
                            if (ce == key.split('.')[0].split('|')[2].lower())]
            # Now get all of the items in a ce
            # As there are multiple instances per (ta1, ta2, ce), we want to cover them all
            for key, value in ta2_ce_items:
                ta2_ceinstance = value
                instance_split = key.split('.')[0].split('|')
                ce_item = instance_split[2].lower()
                if ce != ce_item:
                    print("Inconsistent: Finding ce item {} with loop of ce {}".format(ce_item, ce))
                # Get the relevant task1 annotaiton from the graphG task 2 event
                task1_ce = ce[0:re.search("[0-9]+", ce).end()]
                task1_ceannotation = task1_annotation_collection.annotation_dict['task1|{}'.format(
                    task1_ce)]

                # Check if assessed. If not, automatically give default row
                # Check for the files:
                schema_instance_id = ta2_ceinstance.schema_instance_id

                if schema_instance_id in assessment_collection.ce_df['schema_instance_id'].\
                        unique().tolist():
                    ce_row = assess_ce_row(ta2_ceinstance, assessment_collection)
                else:
                    ce_row = {'file_name': ta2_ceinstance.ce_instance_file_name_base,
                              'schema_instance_id': ta2_ceinstance.schema_instance_id,
                              'ta1_team': ta2_ceinstance.ta1_team_name,
                              'ta2_team': ta2_ceinstance.ta2_team_name,
                              'ce': ta2_ceinstance.ce_name,
                              'assessment': 'no',
                              'instance_status': 0,
                              'schema_name_relevance': 0}

                if schema_instance_id in assessment_collection.ep_df['schema_instance_id'].\
                        unique().tolist():
                    ep_row = assess_ep_row(task1_ceannotation, ta2_ceinstance,
                                           assessment_collection)
                else:
                    num_events = len(ta2_ceinstance.ev_df)
                    instantiated_ev_df = get_ta2_instantiated_events(ta2_ceinstance.ev_df)
                    num_events_instantiated = len(instantiated_ev_df)
                    num_ann_events = len(task1_ceannotation.ep_df)
                    pred_ev_df = get_ta2_predicted_events(ta2_ceinstance.ev_df).copy()
                    num_events_predicted = len(pred_ev_df)
                    partition_df = task1_ceannotation.ep_partition_df
                    num_ann_hidden_events = len(
                        partition_df.loc[partition_df['partition'] == 'hidden', :])
                    num_ann_exposed_events = len(
                        partition_df.loc[partition_df['partition'] == 'exposed', :])
                    ep_dummy_row = {'file_name': ta2_ceinstance.ce_instance_file_name_base,
                                    'schema_instance_id': schema_instance_id,
                                    'ta1_team': ta2_ceinstance.ta1_team_name,
                                    'ta2_team': ta2_ceinstance.ta2_team_name,
                                    'ce': ce, 'num_events': num_events,
                                    'num_events_instantiated': num_events_instantiated,
                                    'num_events_predicted': num_events_predicted,
                                    'num_events_assessed': 0, 'num_events_instantiated_assessed': 0,
                                    'num_events_predicted_assessed': 0,
                                    'num_events_match': 0, 'num_events_extrarel': 0,
                                    'num_events_extrairrel': 0,
                                    'num_ann_events_matched': 0, 'num_ann_events': num_ann_events,
                                    'num_ann_hidden_events': num_ann_hidden_events,
                                    'num_ann_exposed_events': num_ann_exposed_events,
                                    'num_ann_ev_matchwextrarel': 0,
                                    'per_pred_events_match': 0,
                                    'precision_match': np.nan, 'recall_match': np.nan,
                                    'precision_matchwextrarel': np.nan,
                                    'recall_matchwextrarel': np.nan,
                                    'num_events_assessed_at_20': 0,
                                    'num_events_match_at_20': 0,
                                    'num_events_extrarel_at_20': 0,
                                    'num_events_extrairrel_at_20': 0,
                                    'num_ann_events_matched_at_20': 0,
                                    'precision_match_at_20': np.nan, 'recall_match_at_20': np.nan,
                                    'precision_matchwextrarel_at_20': np.nan,
                                    'num_pred_events_match': 0,
                                    'num_pred_events_extrarel': 0,
                                    'num_pred_events_extrairrel': 0,
                                    'num_pred_matched_hidden_events': 0,
                                    'per_pred_matched_hidden_events': 0,
                                    'f1_match': np.nan,
                                    'f1_matchwextrarel': np.nan,
                                    'f1_match_at_20': np.nan,
                                    'f1_matchwextrarel_at_20': np.nan}
                    ep_row = ep_dummy_row

                if schema_instance_id in assessment_collection.ke_df['schema_instance_id'].\
                        unique().tolist():
                    ke_row = assess_ke_row(task1_ceannotation, ta2_ceinstance,
                                           assessment_collection)
                else:
                    num_arguments = ta2_ceinstance.arg_df.shape[0]
                    num_ann_arguments = len(task1_ceannotation.arg_df.
                                            loc[(task1_ceannotation.arg_df['eventprimitive_id'].str.
                                                 contains('EMPTY') == False), 'arg_id'])
                    ke_row = {'file_name': ta2_ceinstance.ce_instance_file_name_base,
                              'schema_instance_id': ta2_ceinstance.schema_instance_id,
                              'ta1_team': ta2_ceinstance.ta1_team_name,
                              'ta2_team': ta2_ceinstance.ta2_team_name,
                              'ce': ta2_ceinstance.ce_name,
                              'num_arguments': num_arguments,
                              'num_ann_arguments': num_ann_arguments,
                              'num_ann_arg_match': 0,
                              'num_ann_arg_match_inexact': 0,
                              'num_ann_arg_match_extrarel': 0,
                              'num_arguments_assessed': 0,
                              'num_arg_with_matched_ep': 0,
                              'num_arguments_match_exact': 0,
                              'num_arguments_matchwinexact': 0,
                              'num_arguments_matchwextrarel': 0,
                              'percentage_arguments_match': 0,
                              'percentage_arguments_matchwinexact': 0,
                              'percentage_arguments_matchwextrarel': 0,
                              'precision_match_exact': 0,
                              'recall_match_exact': 0,
                              'f1_match_exact': 0,
                              'precision_matchwinexact': 0,
                              'recall_matchwinexact': 0,
                              'f1_match_matchwinexact': 0,
                              'precision_matchwextrarel': 0,
                              'recall_matchwextrarel': 0,
                              'f1_match_matchwextrarel': 0,
                              'num_arg_assessed_at_20': 0,
                              'num_arg_match_at_20': 0,
                              'num_arg_inexact_at_20': 0,
                              'num_arg_extrarel_at_20': 0,
                              'num_arg_extrairrel_at_20': 0,
                              'num_ann_arg_match_at_20': 0,
                              'num_ann_arg_matchwextrarel_at_20': 0,
                              'precision_match_at_20': 0,
                              'recall_match_at_20': 0,
                              'precision_matchwextrarel_at_20': 0,
                              'recall_matchwextrarel_at_20': 0}
                if schema_instance_id in assessment_collection.pred_plaus_df['schema_instance_id'].\
                        unique().tolist():
                    pred_row = assess_pred_row(ta2_ceinstance, assessment_collection)
                else:
                    num_events = len(ta2_ceinstance.ev_df)
                    pred_ev_df = get_ta2_predicted_events(ta2_ceinstance.ev_df).copy()
                    num_events_predicted = len(pred_ev_df)
                    num_arguments = ta2_ceinstance.arg_df.shape[0]
                    num_arguments_predicted = len(ta2_ceinstance.arg_df.loc[
                        ta2_ceinstance.arg_df['arg_ta2provenance'] ==
                        'kairos:NULL', 'arg_id'].unique())
                    pred_dummy_row = {'file_name': ta2_ceinstance.ce_instance_file_name_base,
                                      'schema_instance_id': schema_instance_id,
                                      'ta1_team': ta2_ceinstance.ta1_team_name,
                                      'ta2_team': ta2_ceinstance.ta2_team_name,
                                      'ce': ce,
                                      'num_events': num_events,
                                      'num_events_predicted': num_events_predicted,
                                      'num_pred_events_assessed': 0,
                                      'num_pred_events_assessed_plausible': 0,
                                      'num_pred_events_assessed_implausible': 0,
                                      'percentage_pred_events_assessed': 0,
                                      'precision_pred_events_assessed': 0,
                                      'recall_pred_events_assessed': 0,
                                      'F1_pred_events_assessed': 0,
                                      'num_pred_events_justfi_yes': 0,
                                      'num_pred_events_justfi_no': 0,
                                      'num_arguments': num_arguments,
                                      'num_arguments_predicted': num_arguments_predicted,
                                      'num_pred_arguments_assessed': 0,
                                      'num_pred_arguments_assessed_plausible': 0,
                                      'num_pred_arguments_assessed_implausible': 0,
                                      'percentage_pred_arguments_assessed': 0,
                                      'precision_pred_arguments_assessed': 0,
                                      'recall_pred_arguments_assessed': 0,
                                      'F1_pred_arguments_assessed': 0,
                                      'num_pred_arguments_incomplete': 0,
                                      'num_pred_arguments_missing': 0}
                    pred_row = pred_dummy_row

                # Change to Append
                ce_rows.append(ce_row)
                ev_rows.append(ep_row)
                ke_rows.append(ke_row)
                pred_rows.append(pred_row)

    ta2_ce_df = pd.DataFrame(ce_rows)
    ta2_ev_df = pd.DataFrame(ev_rows)
    ta2_ke_df = pd.DataFrame(ke_rows)
    ta2_pred_df = pd.DataFrame(pred_rows)

    # re-compute the recall & F1 for match with extrarel
    max_num_events_extrarel = ta2_ev_df.groupby(['ce'])['num_events_extrarel'].agg('max')
    max_ev_df = max_num_events_extrarel.reset_index()
    max_ev_df.rename(columns={'num_events_extrarel': 'ce_num_events_extrarel'}, inplace=True)
    ta2_ev_df = ta2_ev_df.merge(max_ev_df, how="left")

    ta2_ev_df['recall_matchwextrarel'] = (ta2_ev_df['num_ann_events_matched'] +
                                          ta2_ev_df['num_events_extrarel']) /\
        (ta2_ev_df['num_ann_events'] +
         ta2_ev_df['ce_num_events_extrarel'])
    ta2_ev_df['f1_matchwextrarel'] = 2*(ta2_ev_df['precision_matchwextrarel'] *
                                        ta2_ev_df['recall_matchwextrarel']) /\
        (ta2_ev_df['precision_matchwextrarel'] +
         ta2_ev_df['recall_matchwextrarel'])
    # We want consistent stats, so we do not have recall matchwextrarel_at_20, so we remove f1 score
    ta2_ev_df['f1_matchwextrarel_at_20'] = np.nan

    # Write scores to file
    ta2_ce_df.to_csv(os.path.join(output_dir, "TA2_Assessed_CE_stats.csv"), index=False)
    ta2_ev_df.to_csv(os.path.join(output_dir, "TA2_Assessed_Event_stats.csv"), index=False)
    a_ta2_ev_df = ta2_ev_df.loc[ta2_ev_df['num_events_assessed'] > 0, ].copy()
    a_ta2_ev_df.sort_values(by=["ta1_team", "ta2_team", "ce", "schema_instance_id"], inplace=True)
    a_ta2_ev_df.to_csv(os.path.join(output_dir, "TA2_Assessed_Event_stats_assessed_only.csv"),
                       index=False)
    ta2_ke_df.to_csv(os.path.join(output_dir, "TA2_Assessed_Arguments_stats.csv"), index=False)
    a_ta2_ke_df = ta2_ke_df.loc[ta2_ke_df['num_arguments_assessed'] > 0, ].copy()
    a_ta2_ke_df.sort_values(by=["ta1_team", "ta2_team", "ce", "schema_instance_id"], inplace=True)
    a_ta2_ke_df.to_csv(os.path.join(output_dir, "TA2_Assessed_Arguments_stats_assessed_only.csv"),
                       index=False)
    ta2_pred_df.to_csv(os.path.join(output_dir, "TA2_Assessed_Prediction_stats.csv"), index=False)
    return
