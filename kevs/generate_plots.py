import pandas as pd
import os
import matplotlib.pyplot as plt
import seaborn as sns


def save_fig(path, fig_id, tight_layout=True, fig_extension="png", resolution=300):
    output_path = os.path.join(path, fig_id + "." + fig_extension)
    if tight_layout:
        plt.tight_layout()
    plt.savefig(output_path, format=fig_extension, dpi=resolution)


def generate_task1_plots(assessment_dir):
    output_dir = os.path.join(assessment_dir, "Plot")
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)

    # turn off warning
    pd.set_option('mode.chained_assignment', None)

    """Generate CE plots"""
    ce_df = pd.read_csv(os.path.join(assessment_dir, "TA2_Assessed_CE_stats.csv"))
    ce_df = ce_df.loc[ce_df['assessment'] == 'yes', :]
    ce_df['instance_pass'] = 0
    ce_df['schema_name_relevance'] = ce_df['schema_name_relevance'].astype(int)
    ce_df.loc[ce_df['instance_status'] == 'yes', 'instance_pass'] = 100
    ce_df['TA1-TA2'] = ce_df['ta1_team'] + "-" + ce_df['ta2_team']
    ce_df.sort_values(by=['ta2_team', 'ta1_team'], inplace=True)

    hier_ce_df = ce_df.loc[ce_df['instance_status'] == 'yes', :].copy()

    # handle 'EMPTY_NA' issues
    hier_ce_df.loc[hier_ce_df['hierarchy_Q1_depth_breadth_1'] == 'EMPTY_NA',
                   'hierarchy_Q1_depth_breadth_1'] = 0
    hier_ce_df.loc[hier_ce_df['hierarchy_Q1_depth_breadth_2'] == 'EMPTY_NA',
                   'hierarchy_Q1_depth_breadth_2'] = 0
    hier_ce_df.loc[hier_ce_df['hierarchy_Q1_depth_breadth_3'] == 'EMPTY_NA',
                   'hierarchy_Q1_depth_breadth_3'] = 0
    hier_ce_df.loc[hier_ce_df['hierarchy_Q2_intuitive_labels_1'] == 'EMPTY_NA',
                   'hierarchy_Q2_intuitive_labels_1'] = 0
    hier_ce_df.loc[hier_ce_df['hierarchy_Q2_intuitive_labels_2'] == 'EMPTY_NA',
                   'hierarchy_Q2_intuitive_labels_2'] = 0
    hier_ce_df.loc[hier_ce_df['hierarchy_Q2_intuitive_labels_3'] == 'EMPTY_NA',
                   'hierarchy_Q2_intuitive_labels_3'] = 0
    hier_ce_df.loc[hier_ce_df['hierarchy_Q3_temporal_1'] == 'EMPTY_NA',
                   'hierarchy_Q3_temporal_1'] = 0
    hier_ce_df.loc[hier_ce_df['hierarchy_Q3_temporal_2'] == 'EMPTY_NA',
                   'hierarchy_Q3_temporal_2'] = 0
    hier_ce_df.loc[hier_ce_df['hierarchy_Q3_temporal_3'] == 'EMPTY_NA',
                   'hierarchy_Q3_temporal_3'] = 0

    hier_ce_df = hier_ce_df.astype({
        'hierarchy_Q1_depth_breadth_1': 'int',
        'hierarchy_Q1_depth_breadth_2': 'int',
        'hierarchy_Q1_depth_breadth_3': 'int',
        'hierarchy_Q2_intuitive_labels_1': 'int',
        'hierarchy_Q2_intuitive_labels_2': 'int',
        'hierarchy_Q2_intuitive_labels_3': 'int',
        'hierarchy_Q3_temporal_1': 'int',
        'hierarchy_Q3_temporal_2': 'int',
        'hierarchy_Q3_temporal_3': 'int',
    })

    hier_ce_df['hier_avg_db'] = (hier_ce_df['hierarchy_Q1_depth_breadth_1'] +
                                 hier_ce_df['hierarchy_Q1_depth_breadth_2'] +
                                 hier_ce_df['hierarchy_Q1_depth_breadth_3']) / 3
    hier_ce_df['hier_avg_int'] = (hier_ce_df['hierarchy_Q2_intuitive_labels_1'] +
                                  hier_ce_df['hierarchy_Q2_intuitive_labels_2'] +
                                  hier_ce_df['hierarchy_Q2_intuitive_labels_3']) / 3
    hier_ce_df['hier_avg_temp'] = (hier_ce_df['hierarchy_Q3_temporal_1'] +
                                   hier_ce_df['hierarchy_Q3_temporal_2'] +
                                   hier_ce_df['hierarchy_Q3_temporal_3']) / 3

    # bar plot: Aggregated CE matching
    sns.barplot(data=ce_df, x="TA1-TA2", y="instance_pass", palette="colorblind",
                errorbar=None)
    plt.ylabel("Instance Pass Percentage (%)")
    save_fig(output_dir, "CE_instance_pass_bar")
    plt.clf()

    # box plot: Aggregated Schema Name relevance
    sns.boxplot(data=ce_df, x="TA1-TA2", y="schema_name_relevance", palette="colorblind")
    plt.ylabel("Schema Name Relevance")
    save_fig(output_dir, "CE_schema_name_box")
    plt.clf()

    # bar plot: Aggregated Hierarchical Stats
    sns.barplot(data=hier_ce_df, x="TA1-TA2", y="hier_avg_db", palette="colorblind",
                errorbar="sd", capsize=0.2, errwidth=0.5)
    plt.ylabel("Average Hierarchy Depth/Breath")
    plt.ylim(1, 5)
    save_fig(output_dir, "CE_hierarhcy_depth_breath_bar")
    plt.clf()

    sns.barplot(data=hier_ce_df, x="TA1-TA2", y="hier_avg_int", palette="colorblind",
                errorbar="sd", capsize=0.2, errwidth=0.5)
    plt.ylabel("Average Hierarchy Intuitiveness")
    plt.ylim(1, 5)
    save_fig(output_dir, "CE_hierarhcy_intuitiveness_bar")
    plt.clf()

    sns.barplot(data=hier_ce_df, x="TA1-TA2", y="hier_avg_temp", palette="colorblind",
                errorbar="sd", capsize=0.2, errwidth=0.5)
    plt.ylabel("Average Hierarchy Temopral")
    plt.ylim(1, 5)
    save_fig(output_dir, "CE_hierarhcy_temporal_bar")
    plt.clf()

    """Generate Event plots"""
    ev_df = pd.read_csv(os.path.join(assessment_dir,
                                     "TA2_Assessed_Event_stats_assessed_only.csv"))
    ev_df['TA1-TA2'] = ev_df['ta1_team'] + "-" + ev_df['ta2_team']
    ev_df['ce_type'] = 'coup'
    ev_df.loc[ev_df['ce'].isin(['ce2201_abridged', 'ce2202_abridged', 'ce2203_abridged']),
              'ce_type'] = 'spill'
    ev_df.loc[ev_df['ce'].isin(['ce2204_abridged', 'ce2205_abridged', 'ce2206_abridged']),
              'ce_type'] = 'riot'
    ev_df.loc[ev_df['ce'].isin(['ce2207_abridged', 'ce2208_abridged', 'ce2209_abridged']),
              'ce_type'] = 'disease'
    ev_df.loc[ev_df['ce'].isin(['ce2210_abridged', 'ce2211_abridged', 'ce2212_abridged']),
              'ce_type'] = 'terror'
    ev_df.sort_values(by=['ta2_team', 'ta1_team'], inplace=True)

    # bar plot: event extraction F1 score all CEs
    sns.barplot(data=ev_df, x="TA1-TA2", y="f1_matchwextrarel", palette="colorblind",
                errorbar="sd", capsize=0.2, errwidth=0.5)
    plt.ylabel("F1 score")
    save_fig(output_dir, "Event_f1_all_CE_bar")
    plt.clf()

    # bar plot: event extraction F1 score per CE type
    ax = sns.barplot(data=ev_df, x="TA1-TA2", y="f1_matchwextrarel", hue='ce_type',
                     palette="colorblind",
                     errorbar="sd", capsize=0.2, errwidth=0.5)
    plt.ylabel("F1 score")
    sns.move_legend(ax, "lower center", bbox_to_anchor=(0.5, 1), ncol=5,
                    title=None, frameon=False)
    save_fig(output_dir, "Event_f1_per_CE_bar")
    plt.clf()

    # bar plot: Hierarchy extraction F1 score all CEs
    ev_df['precision_hierarchy'] = ev_df['precision_hierarchy'] * 100
    sns.barplot(data=ev_df, x="TA1-TA2", y="precision_hierarchy", palette="colorblind",
                errorbar="sd", capsize=0.2, errwidth=0.5)
    plt.ylabel("Hierarchy Correctness (%)")
    save_fig(output_dir, "Hier_f1_all_CE_bar")
    plt.clf()

    # bar plot: Hierarchy extraction F1 score per CE type
    ax = sns.barplot(data=ev_df, x="TA1-TA2", y="precision_hierarchy", hue='ce_type',
                     palette="colorblind",
                     errorbar=("sd", 0.5), capsize=0.2, errwidth=0.5)
    plt.ylabel("Hierarchy Correctness (%)")
    sns.move_legend(ax, "lower center", bbox_to_anchor=(0.5, 1),  ncol=5,
                    title=None, frameon=False)
    save_fig(output_dir, "Hier_f1_per_CE_bar")
    plt.clf()

    """Generate Argument plots"""
    arg_df = pd.read_csv(os.path.join(assessment_dir,
                                      "TA2_Assessed_Arguments_stats_assessed_only.csv"))

    arg_df['TA1-TA2'] = arg_df['ta1_team'] + "-" + arg_df['ta2_team']
    arg_df['ce_type'] = 'coup'
    arg_df.loc[arg_df['ce'].isin(['ce2201_abridged', 'ce2202_abridged', 'ce2203_abridged']),
               'ce_type'] = 'spill'
    arg_df.loc[arg_df['ce'].isin(['ce2204_abridged', 'ce2205_abridged', 'ce2206_abridged']),
               'ce_type'] = 'riot'
    arg_df.loc[arg_df['ce'].isin(['ce2207_abridged', 'ce2208_abridged', 'ce2209_abridged']),
               'ce_type'] = 'disease'
    arg_df.loc[arg_df['ce'].isin(['ce2210_abridged', 'ce2211_abridged', 'ce2212_abridged']),
               'ce_type'] = 'terror'
    arg_df.sort_values(by=['ta2_team', 'ta1_team'], inplace=True)

    # bar plot: Aggregated F1 scores (from instantiated event)
    sns.barplot(data=arg_df, x="TA1-TA2", y="f1_args_from_ins_matchwextrarel",
                palette="colorblind",
                errorbar="sd", capsize=0.2, errwidth=0.5)
    plt.ylabel("F1 score")
    save_fig(output_dir, "Args_f1_ins_ev_all_CE_bar")
    plt.clf()

    # bar plot: Aggregated Precision scores (from instantiated event)
    sns.barplot(data=arg_df, x="TA1-TA2", y="precision_args_from_ins_matchwextrarel",
                palette="colorblind",
                errorbar="sd", capsize=0.2, errwidth=0.5)
    plt.ylabel("Precision")
    save_fig(output_dir, "Args_pr_ins_ev_all_CE_bar")
    plt.clf()

    # bar plot: Aggregated Recall scores (from instantiated event)
    sns.barplot(data=arg_df, x="TA1-TA2", y="recall_args_from_ins_matchwextrarel",
                palette="colorblind",
                errorbar="sd", capsize=0.2, errwidth=0.5)
    plt.ylabel("Recall")
    save_fig(output_dir, "Args_rc_ins_ev_all_CE_bar")
    plt.clf()

    "Generate Task2 plots (temporarily)"
    graphg_df = pd.read_csv(os.path.join(assessment_dir,
                                         "tmp_task2_graphg_f1.csv"))
    graphg_df["TA1-TA2"] = graphg_df['TA1'] + "-" + graphg_df['TA2']
    graphg_df.sort_values(by=['TA2', 'TA1'], inplace=True)

    # bar plot
    ax = sns.barplot(data=graphg_df, x="TA1-TA2", y="F1", palette="colorblind",
                     hue="Phase",
                     errorbar="sd", capsize=0.2, errwidth=0.5)
    plt.ylabel("F1 score")
    sns.move_legend(ax, "lower center", bbox_to_anchor=(0.5, 1), ncol=5,
                    title=None, frameon=False)
    save_fig(output_dir, "Task2_graphg_f1_bar")
    plt.clf()
