import os
import pandas as pd


class Assessment:
    """
    An object allowing for the importing of the Assessment Data
    """

    def __init__(self):
        self.ce_df = pd.DataFrame()
        self.ep_df = pd.DataFrame()
        self.ke_df = pd.DataFrame()
        self.corr_df = pd.DataFrame()
        self.pred_arg_df = pd.DataFrame()
        self.pred_plaus_df = pd.DataFrame()

    def import_assessment_data(self, assessment_dir):
        """
        Method that imports all TA1 Libraries from extractions and labels them.
        Args:
            assessment_dir: The path to the assessment data files (includes within the data folder)

        Returns:
        """
        # We have six files; read them and then merge them with the mapping frame
        mapping_df = pd.read_table(os.path.join(assessment_dir, "resin_file_mapping.tab"), sep="\t")

        # We have six files; read them and then merge them with the mapping frame
        self.ce_df = pd.read_table(os.path.join(assessment_dir, "KAIROS_phase2a_ce_matching.tab"),
                                   sep="\t")
        self.ce_df = self.ce_df.merge(mapping_df, how="left")
        if len(self.ce_df.loc[pd.isna(self.ce_df['file_name']), 'file_name']) > 0:
            self.ce_df.loc[pd.isna(self.ce_df['file_name']), 'file_name'] = \
                self.ce_df.loc[pd.isna(self.ce_df['file_name']), 'json_id']
        self.ep_df = pd.read_table(os.path.join(assessment_dir, "KAIROS_phase2a_ep_alignment.tab"),
                                   sep="\t")
        self.ep_df = self.ep_df.merge(mapping_df, how="left")
        if len(self.ep_df.loc[pd.isna(self.ep_df['file_name']), 'file_name']) > 0:
            self.ep_df.loc[pd.isna(self.ep_df['file_name']), 'file_name'] = \
                self.ep_df.loc[pd.isna(self.ep_df['file_name']), 'json_id']
        self.ke_df = pd.read_table(os.path.join(assessment_dir, "KAIROS_phase2a_ke_analysis.tab"),
                                   sep="\t")
        self.ke_df = self.ke_df.merge(mapping_df, how="left")
        if len(self.ke_df.loc[pd.isna(self.ke_df['file_name']), 'file_name']) > 0:
            self.ke_df.loc[pd.isna(self.ke_df['file_name']), 'file_name'] = \
                self.ke_df.loc[pd.isna(self.ke_df['file_name']), 'json_id']
        self.corr_df = pd.read_table(os.path.join(assessment_dir, "KAIROS_phase2a_correctness.tab"),
                                     sep="\t")
        self.corr_df = self.corr_df.merge(mapping_df, how="left")
        if len(self.corr_df.loc[pd.isna(self.corr_df['file_name']), 'file_name']) > 0:
            self.corr_df.loc[pd.isna(self.corr_df['file_name']), 'file_name'] = \
                self.corr_df.loc[pd.isna(self.corr_df['file_name']), 'json_id']
        self.pred_plaus_df = pd.read_table(
            os.path.join(assessment_dir, "KAIROS_phase2a_prediction_plausibility.tab"), sep="\t")
        self.pred_plaus_df = self.pred_plaus_df.merge(mapping_df, how="left")
        if len(self.pred_plaus_df.loc[pd.isna(self.pred_plaus_df['file_name']), 'file_name']) > 0:
            self.pred_plaus_df.loc[pd.isna(self.pred_plaus_df['file_name']), 'file_name'] = \
                self.pred_plaus_df.loc[pd.isna(self.pred_plaus_df['file_name']), 'json_id']
        self.pred_arg_df = pd.read_table(
            os.path.join(assessment_dir, "KAIROS_phase2a_prediction_arg_analysis.tab"), sep="\t")
        self.pred_arg_df = self.pred_arg_df.merge(mapping_df, how="left")
        if len(self.pred_arg_df.loc[pd.isna(self.pred_arg_df['file_name']), 'file_name']) > 0:
            self.pred_arg_df.loc[pd.isna(self.pred_arg_df['file_name']), 'file_name'] = \
                self.pred_arg_df.loc[pd.isna(self.pred_arg_df['file_name']), 'json_id']

        # Now construct a schema_instance_id from these fields
        # Using the same convention as we do for our extractions
        # So that we can easily link with our TA2
        self.ce_df['und_str'] = "_"
        self.ep_df['und_str'] = "_"
        self.ke_df['und_str'] = "_"
        self.corr_df['und_str'] = "_"
        self.pred_plaus_df['und_str'] = "_"
        self.pred_arg_df['und_str'] = "_"

        self.ce_df['schema_instance_id'] = self.ce_df['file_name'] + \
            self.ce_df['und_str'] + self.ce_df['instance_id'].str.split('/').str[1]
        self.ep_df['schema_instance_id'] = self.ep_df['file_name'] + \
            self.ep_df['und_str'] + self.ep_df['instance_id'].str.split('/').str[1]
        self.ke_df['schema_instance_id'] = self.ke_df['file_name'] + \
            self.ke_df['und_str'] + self.ke_df['instance_id'].str.split('/').str[1]
        self.corr_df['schema_instance_id'] = self.corr_df['file_name'] + \
            self.corr_df['und_str'] + self.corr_df['instance_id'].str.split('/').str[1]
        self.pred_plaus_df['schema_instance_id'] = self.pred_plaus_df['file_name'] + \
            self.pred_plaus_df['und_str'] + self.pred_plaus_df['instance_id'].str.split('/').str[1]
        self.pred_arg_df['schema_instance_id'] = self.pred_arg_df['file_name'] + \
            self.pred_arg_df['und_str'] + self.pred_arg_df['instance_id'].str.split('/').str[1]

        self.ce_df.drop(columns=["und_str"], inplace=True)
        self.ep_df.drop(columns=["und_str"], inplace=True)
        self.ke_df.drop(columns=["und_str"], inplace=True)
        self.corr_df.drop(columns=["und_str"], inplace=True)
        self.pred_plaus_df.drop(columns=["und_str"], inplace=True)
        self.pred_arg_df.drop(columns=["und_str"], inplace=True)
