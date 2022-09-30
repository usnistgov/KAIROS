import pandas as pd
import os

import kevs.extract_elements_from_json
import kevs.produce_event_trees
from kevs import load_json_data as load


# Initiate TA1 Data Frames
def init_ta1_schema_dataframe() -> pd.DataFrame:
    schema_df = pd.DataFrame(columns=['file_name', 'ta1_team_name', 'sdfVersion', 'schema_id',
                                      'schema_name',
                                      'schema_version', 'schema_description', 'schema_template',
                                      'schema_comment',
                                      'schema_primitives', 'event_list', 'entity_list',
                                      'relation_list'])

    return schema_df


def init_ta1_ev_dataframe() -> pd.DataFrame:
    ev_df = pd.DataFrame(columns=['ta1_team_name', 'schema_id', 'ev_id', 'ev_name', 'ev_type',
                                  'ev_qnode',
                                  'ev_qlabel',
                                  'ev_children_gate', 'ev_description', 'ev_goal',
                                  'ev_minDuration', 'ev_maxDuration',
                                  'ev_TA1explanation', 'ev_privateData', 'ev_comment',
                                  'ev_aka', 'ev_template',
                                  'ev_repeatable', 'ev_child_list', 'ev_arg_list'])

    return ev_df


def init_ta1_children_dataframe() -> pd.DataFrame:
    children_df = pd.DataFrame(columns=['ta1_team_name', 'schema_id', 'ev_id', 'child_id',
                                        'child_children_gate',
                                        'child_optional',
                                        'child_comment', 'child_importance', 'child_outlinks'])

    return children_df


def init_ta1_arg_dataframe() -> pd.DataFrame:
    arg_df = pd.DataFrame(columns=['ta1_team_name', 'schema_id', 'ev_id', 'arg_id',
                                   'arg_role_name', 'arg_entity'])

    return arg_df


def init_ta1_ent_dataframe() -> pd.DataFrame:
    ent_df = pd.DataFrame(columns=['ta1_team_name', 'schema_id', 'ent_id', 'ent_name',
                                   'ent_qnode', 'ent_qlabel',
                                   'ent_comment'])

    return ent_df


def init_ta1_rel_dataframe() -> pd.DataFrame:
    rel_df = pd.DataFrame(columns=['ta1_team_name', 'schema_id', 'rel_id', 'rel_name',
                                   'rel_relationSubject', 'rel_relationPredicate',
                                   'rel_relationObject', 'rel_inEvent'])
    return rel_df


class TA1Library:
    """
    As there is one TA1 file per TA1 Library, we have a single class for a TA1 Library
    """

    def __init__(self, ta1_team_name):
        """
        Initialize Object
        """
        self.ta1_team_name = ta1_team_name
        self.base_file_name = "{}-library".format(self.ta1_team_name).lower()
        self.ev_df = init_ta1_ev_dataframe()
        self.arg_df = init_ta1_arg_dataframe()
        self.ent_df = init_ta1_ent_dataframe()
        self.rel_df = init_ta1_rel_dataframe()
        self.temporalrel_df = init_ta1_rel_dataframe()
        self.children_df = init_ta1_children_dataframe()
        self.schema_df = init_ta1_schema_dataframe()
        # Unique TA1 Library ID
        self.library_id = ""
        self.schema_id = ""
        self.ta1_tree = None

    def extract_contents_from_sdf_file(self, json_dict: dict, file_name: str):
        """

        Args:
            json_dict:
            file_name:

        Returns:

        """
        kevs.extract_elements_from_json.\
            extract_ta1_elements_from_json_file(self.ta1_team_name, self.base_file_name, self,
                                                json_dict, file_name)

    def extract_contents_from_sdf(self, sdf_collection_dir):
        """

        Args:
            json_collection_dir:

        Returns:

        """
        json_dict = load.load_json_directory(self.ta1_team_name, sdf_collection_dir)

        # Re-empty library
        self.ev_df = init_ta1_ev_dataframe()
        self.arg_df = init_ta1_arg_dataframe()
        self.ent_df = init_ta1_ent_dataframe()
        self.rel_df = init_ta1_rel_dataframe()
        self.temporalrel_df = init_ta1_rel_dataframe()
        self.children_df = init_ta1_children_dataframe()
        self.schema_df = init_ta1_schema_dataframe()

        # Because there is the assumption that there is only one file in the dictionary, this
        # method has been shortened
        for file_name in json_dict:
            self.extract_contents_from_sdf_file(json_dict, file_name)

    def import_extractions_from_files(self, ta1_extraction_dir: str):
        """

        Args:
            ta1_extraction_dir:


        Returns:

        """
        self.schema_df = pd.read_csv(os.path.join(ta1_extraction_dir,
                                     os.path.basename(self.base_file_name) + '_schema.csv'))
        self.ev_df = pd.read_csv(os.path.join(ta1_extraction_dir,
                                 os.path.basename(self.base_file_name) + '_ev.csv'))
        self.arg_df = pd.read_csv(os.path.join(ta1_extraction_dir,
                                  os.path.basename(self.base_file_name) + '_arg.csv'))
        try:
            self.children_df = pd.read_csv(os.path.join(ta1_extraction_dir,
                                           os.path.basename(self.base_file_name) + '_children.csv'))
        except FileNotFoundError:
            pass
        try:
            self.ent_df = pd.read_csv(os.path.join(ta1_extraction_dir,
                                      os.path.basename(self.base_file_name) + '_ent.csv'))
        except FileNotFoundError:
            pass
        try:
            self.rel_df = pd.read_csv(os.path.join(
                ta1_extraction_dir, os.path.basename(self.base_file_name) + '_rel.csv'))
        except FileNotFoundError:
            pass
        try:
            self.temporalrel_df = pd.read_csv(os.path.join(
                ta1_extraction_dir, os.path.basename(self.base_file_name) + '_temporalrel.csv'))
        except FileNotFoundError:
            pass
        # Need to get a unique library ID
        self.library_id = ""
        # Get the schema id from the first value of the extracted event
        self.schema_id = self.ev_df.iloc[0, :]['schema_id']

    def write_extractions_to_files(self, score_directory: str) -> None:
        """
        Writes the extracted objects to Files
        Returns:

        """
        if not os.path.isdir(score_directory):
            os.makedirs(score_directory)
        if not os.path.isdir(os.path.join(score_directory, self.ta1_team_name)):
            os.makedirs(os.path.join(score_directory, self.ta1_team_name))
        if len(self.schema_df) > 0:
            self.schema_df.to_csv(os.path.join(score_directory, self.ta1_team_name,
                                               os.path.basename(self.base_file_name)
                                               + '_schema.csv'),
                                  index=False)
        if len(self.ev_df) > 0:
            self.ev_df.to_csv(os.path.join(score_directory, self.ta1_team_name,
                                           os.path.basename(self.base_file_name)
                                           + '_ev.csv'),
                              index=False)
        if len(self.arg_df) > 0:
            self.arg_df.to_csv(os.path.join(score_directory, self.ta1_team_name,
                                            os.path.basename(self.base_file_name) + '_arg.csv'),
                               index=False)
        if len(self.children_df) > 0:
            self.children_df.to_csv(os.path.join(score_directory, self.ta1_team_name,
                                                 os.path.basename(self.base_file_name)
                                                 + '_children.csv'),
                                    index=False)
        if len(self.ent_df) > 0:
            self.ent_df.to_csv(os.path.join(score_directory, self.ta1_team_name,
                                            os.path.basename(self.base_file_name) + '_ent.csv'),
                               index=False)
        if len(self.rel_df) > 0:
            self.rel_df.to_csv(os.path.join(score_directory, self.ta1_team_name,
                                            os.path.basename(self.base_file_name) + '_rel.csv'),
                               index=False)
        if len(self.temporalrel_df) > 0:
            self.temporalrel_df.\
                to_csv(os.path.join(score_directory, self.ta1_team_name,
                                    os.path.basename(self.base_file_name) + '_temporalrel.csv'),
                       index=False)

    def produce_event_tree(self, output_dir):
        tree_output_dir = os.path.join(output_dir, self.ta1_team_name, "Event_Trees")
        if not os.path.isdir(tree_output_dir):
            os.makedirs(tree_output_dir)
        self.ta1_tree = kevs.produce_event_trees.create_ta1_event_tree(self, tree_output_dir)


class TA1Collection:
    """
    Container for a dictionary of TA1 Libraries with key <ta1_team_name>
    and value of the TA1Library
    """

    def __init__(self):
        """
        Initialize Object
        """
        self.ta1dict = dict()

    def extract_contents_from_sdf(self, sdf_collection_dir):
        """

        Args:
            json_collection_dir:

        Returns:

        """
        pass

    def import_extractions_from_file_collection(self, collection_dir):
        """
        Method that imports all TA1 Libraries from extractions and labels them.
        Args:
            collection_dir:

        Returns:
        """
        # Strategy: Read each file in the "Extracted Events Directory". Use that filename
        # to obtain the right TA1 library and then load the others
        # Loop over all of the different teams from the collection directory
        for file_dir in os.listdir(collection_dir):
            if os.path.isdir(os.path.join(collection_dir, file_dir)):
                ta1_team = file_dir
                team_directory = os.path.join(collection_dir, ta1_team)
                try:
                    ta1_library = TA1Library(ta1_team)
                    ta1_library.import_extractions_from_files(team_directory)
                    self.ta1dict['{}'.format(ta1_team)] = ta1_library
                except FileNotFoundError as e:
                    print("Warning: TA1 Library: " + ta1_team +
                          " is missing files and was not imported. Exception Message:")
                    print(e)

    def write_extractions_to_file_collection(self, score_dir):
        """
        Write Extractions of all TA1 Library objects to their respective files

        Args:
            score_dir:

        Returns:

        """
        for key, value in self.ta1dict.items():
            key_items = key.split('|')
            ta1_team_name = key_items[0]
            value.write_extraction_to_file(score_dir, ta1_team_name)

    def produce_event_trees(self, output_dir):
        for key, value in self.ta1dict.items():
            ta1_library = value
            ta1_library.produce_event_tree(output_dir)
