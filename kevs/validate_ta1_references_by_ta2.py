import os
import pandas as pd


def validate_ta1_references_by_ta2(output_dir, ta1_collection, ta2_collection):

    ta2_team_list = ta2_collection.ta2dict.keys()
    ta1_link_summary_df = pd.DataFrame(columns=["schema_instance_id", "ta1_team_name",
                                                "ta2_team_name",
                                                "num_mislinked_ta2_ref", "num_unused_ta1_ref",
                                                "num_ta2_kairosnull_events",
                                                "num_used_ta1_ref",
                                                "num_rel_extra_ta2_ref",
                                                "num_rel_unused_ta1_ref",
                                                "num_rel_ta2_kairosnull_events",
                                                "num_rel_used_ta1_ref",
                                                "num_temporalrel_extra_ta2_ref",
                                                "num_temporalrel_unused_ta1_ref",
                                                "num_temporalrel_ta2_kairosnull_events",
                                                "num_temporalrel_used_ta1_ref"])
    ref_output_dir = os.path.join(output_dir, "TA1_Reference_Checks")

    for ta2_team in ta2_team_list:
        ta2_instance = ta2_collection.ta2dict[ta2_team]

        full_output_dir = os.path.join(output_dir, "TA1_Reference_Checks", ta2_team)
        if not os.path.isdir(full_output_dir):
            os.makedirs(full_output_dir)

        for key, value in ta2_instance.ta2dict.items():
            ta2_ceinstance = value
            instance_split = key.split('.')[0].split('|')
            ce_instance_file_name_base = ta2_ceinstance.ce_instance_file_name_base
            ta1_team_name = instance_split[0]
            # Get the TA1 Library
            ta1_library = ta1_collection.ta1dict[ta1_team_name]
            ta1_library.ev_df.rename(columns={'ev_id': 'ta1_ev_id'}, inplace=True)
            ta1_ref_link_df = ta1_library.ev_df.loc[:, ['ta1_team_name', "ta1_ev_id"]].merge(
                ta2_ceinstance.ev_df.loc[:, ['ta2_team_name',
                                             'ev_ta1ref', 'ev_id', 'schema_instance_id']],
                how="outer", left_on="ta1_ev_id", right_on="ev_ta1ref"
            )
            ta1_library.rel_df.rename(columns={'rel_id': 'ta1_rel_id'}, inplace=True)
            ta2_ceinstance.rel_df['rel_ta1ref'] = ta2_ceinstance.rel_df['rel_ta1ref'].astype(
                'object')
            ta1_ref_rel_link_df = ta1_library.rel_df.loc[:, ['ta1_team_name', "ta1_rel_id"]].merge(
                ta2_ceinstance.rel_df.loc[:, ['ta2_team_name',
                                              'rel_ta1ref', 'rel_id', 'schema_instance_id']],
                how="outer", left_on="ta1_rel_id", right_on="rel_ta1ref"
            )
            ta1_library.temporalrel_df.rename(columns={'rel_id': 'ta1_rel_id'}, inplace=True)
            ta2_ceinstance.temporalrel_df['rel_ta1ref'] = \
                ta2_ceinstance.temporalrel_df['rel_ta1ref'].astype('object')
            ta1_ref_temporalrel_link_df = \
                ta1_library.temporalrel_df.loc[:, ['ta1_team_name', "ta1_rel_id"]].\
                merge(ta2_ceinstance.temporalrel_df.loc[:, ['ta2_team_name',
                                                            'rel_ta1ref',
                                                            'rel_id',
                                                            'schema_instance_id']],
                      how="outer", left_on="ta1_rel_id", right_on="rel_ta1ref")
            # Write the ta1_ref_link to file
            ce_fpath = os.path.join(full_output_dir,
                                    "{}_ta1ref_links.csv".format(ce_instance_file_name_base))
            ta1_ref_link_df.to_csv(ce_fpath, index=False)
            rel_fpath = os.path.join(full_output_dir,
                                     "{}_ta1ref_rel_links.csv".format(ce_instance_file_name_base))
            ta1_ref_rel_link_df.to_csv(rel_fpath, index=False)
            temporalrel_fpath = os.path.join(full_output_dir,
                                             "{}_ta1ref_temporalrel_links.csv".
                                             format(ce_instance_file_name_base))
            ta1_ref_temporalrel_link_df.to_csv(temporalrel_fpath, index=False)

            # Append summary stats to summary data frame
            num_mislinked_ta2_ref = len(ta1_ref_link_df.loc[
                                        pd.isna(ta1_ref_link_df['ta1_ev_id']) &
                                        (ta1_ref_link_df['ev_ta1ref'] != "kairos:NULL"), :])
            num_unused_ta1_ref = len(ta1_ref_link_df.loc[
                                     pd.isna(ta1_ref_link_df['ev_ta1ref']), :])
            num_ta2_kairosnull_events = len(ta1_ref_link_df.loc[
                                            ta1_ref_link_df['ev_ta1ref'] == "kairos:NULL", :])
            num_used_ta1_ref = len(ta1_ref_link_df.loc[
                                   pd.notna(ta1_ref_link_df['ta1_ev_id']) &
                                   pd.notna(ta1_ref_link_df['ev_ta1ref']) &
                                   (ta1_ref_link_df['ev_ta1ref'] != "kairos:NULL"), :])
            num_rel_extra_ta2_ref = len(ta1_ref_rel_link_df.loc[
                                        pd.isna(ta1_ref_rel_link_df['ta1_rel_id']) &
                                        (ta1_ref_rel_link_df['rel_ta1ref'] != "kairos:NULL"), :])
            num_rel_unused_ta1_ref = len(ta1_ref_rel_link_df.loc[
                pd.isna(ta1_ref_rel_link_df['rel_ta1ref']), :])
            num_rel_ta2_kairosnull_events = len(ta1_ref_rel_link_df.loc[
                ta1_ref_rel_link_df['rel_ta1ref'] == "kairos:NULL", :])
            num_rel_used_ta1_ref = len(ta1_ref_rel_link_df.loc[
                pd.notna(ta1_ref_rel_link_df['ta1_rel_id']) &
                pd.notna(ta1_ref_rel_link_df['rel_ta1ref']) &
                (ta1_ref_rel_link_df['rel_ta1ref'] != "kairos:NULL"), :])
            num_temporalrel_extra_ta2_ref = len(ta1_ref_temporalrel_link_df.loc[
                pd.isna(ta1_ref_temporalrel_link_df['ta1_rel_id']) &
                (ta1_ref_temporalrel_link_df['rel_ta1ref'] != "kairos:NULL"), :])
            num_temporalrel_unused_ta1_ref = len(ta1_ref_temporalrel_link_df.loc[
                pd.isna(ta1_ref_temporalrel_link_df['rel_ta1ref']), :])
            num_temporalrel_ta2_kairosnull_events = len(ta1_ref_temporalrel_link_df.loc[
                ta1_ref_temporalrel_link_df['rel_ta1ref'] == "kairos:NULL", :])
            num_temporalrel_used_ta1_ref = len(ta1_ref_temporalrel_link_df.loc[
                pd.notna(ta1_ref_temporalrel_link_df['ta1_rel_id']) &
                pd.notna(ta1_ref_temporalrel_link_df['rel_ta1ref']) &
                (ta1_ref_temporalrel_link_df['rel_ta1ref'] != "kairos:NULL"), :])
            ta1_link_summary_df = \
                pd.concat([ta1_link_summary_df,
                           pd.DataFrame([{"schema_instance_id": ta2_ceinstance.schema_instance_id,
                                          "ta1_team_name": ta1_team_name,
                                          "ta2_team_name": ta2_team,
                                          "num_mislinked_ta2_ref": num_mislinked_ta2_ref,
                                          "num_unused_ta1_ref": num_unused_ta1_ref,
                                          "num_ta2_kairosnull_events":
                                              num_ta2_kairosnull_events,
                                          "num_used_ta1_ref": num_used_ta1_ref,
                                          "num_rel_extra_ta2_ref": num_rel_extra_ta2_ref,
                                          "num_rel_unused_ta1_ref": num_rel_unused_ta1_ref,
                                          "num_rel_ta2_kairosnull_events":
                                              num_rel_ta2_kairosnull_events,
                                          "num_rel_used_ta1_ref": num_rel_used_ta1_ref,
                                          "num_temporalrel_extra_ta2_ref":
                                              num_temporalrel_extra_ta2_ref,
                                          "num_temporalrel_unused_ta1_ref":
                                              num_temporalrel_unused_ta1_ref,
                                          "num_temporalrel_ta2_kairosnull_events":
                                              num_temporalrel_ta2_kairosnull_events,
                                          "num_temporalrel_used_ta1_ref":
                                              num_temporalrel_used_ta1_ref}])],
                          ignore_index=True)

    link_summary_path = os.path.join(ref_output_dir, "ta2_reference_check_summary.csv")
    ta1_link_summary_df.to_csv(link_summary_path, index=False)
