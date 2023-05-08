import pandas as pd
import os

import kevs.extract_elements_from_json
import kevs.produce_event_trees

from kevs import load_json_data as load

"""
change/add field names (wd_node, wd_label, wd_description, temporal, child)
"""
# Initialize TA2 Data Frames


def init_ta2_schema_dataframe() -> pd.DataFrame:
    schema_df = pd.DataFrame(columns=['file_name', 'ta2_team_name', 'ta1_team_name', 'sdfVersion',
                                      'schema_instance_id', 'schema_id', 'schema_name',
                                      'schema_version', 'schema_description', 'schema_template',
                                      'schema_comment',
                                      'instance_id', 'instance_id_short',
                                      'instance_name', 'instance_confidence',
                                      'instance_description', 'instance_ta1ref',
                                      'schema_primitives', 'event_list', 'entity_list',
                                      'relation_list',
                                      'ce_id', 'provenance_list'])

    return schema_df


def init_ta2_ev_dataframe() -> pd.DataFrame:
    ev_df = pd.DataFrame(columns=['ta2_team_name', 'ta1_team_name', 'schema_instance_id',
                                  'schema_id', 'instance_id', 'instance_id_short', 'ce_id', 'task',
                                  'instance_name', 'ev_id', 'ev_name', 'ev_type', 'ev_wd_node',
                                  'ev_wd_label', 'ev_wd_description',
                                  'ev_ta2wd_node', 'ev_ta2wd_label', 'ev_ta2wd_description'
                                  'ev_ta1ref',
                                  'ev_ta2_ce_instance', 'ev_description', 'ev_goal',
                                  'ev_ta1explanation', 'ev_privateData', 'ev_comment',
                                  'ev_aka', 'ev_template',
                                  'ev_repeatable', 'ev_child_list', 'ev_arg_list',
                                  'ev_provenance', 'ev_confidence', 'ev_confidence_val',
                                  'ev_prediction_provenance',
                                  'ev_duration', 'ev_earliestStartTime', 'ev_latestStartTime',
                                  'ev_earliestEndTime', 'ev_latestEndTime',
                                  'ev_absoluteTime', 'ev_modality', 'ev_outlinks'])

    return ev_df


def init_ta2_children_dataframe() -> pd.DataFrame:
    children_df = pd.DataFrame(columns=['ta2_team_name', 'ta1_team_name', 'schema_instance_id',
                                        'schema_id', 'ev_id', 'child_ev_id'])

    return children_df


def init_ta2_arg_dataframe() -> pd.DataFrame:
    arg_df = pd.DataFrame(columns=['ta2_team_name', 'ta1_team_name', 'schema_instance_id',
                                   'schema_id', 'instance_id',  'ev_id',
                                   'arg_id', 'arg_role_name', 'arg_entity',
                                   'arg_value_id', 'arg_ta2entity',
                                   'arg_ta2confidence',
                                   'arg_ta2provenance'])

    return arg_df


def init_ta2_ent_dataframe() -> pd.DataFrame:
    ent_df = pd.DataFrame(columns=['ta2_team_name', 'ta1_team_name', 'schema_instance_id',
                                   'schema_id', 'instance_id',  'ent_id', 'ent_name',
                                   'ent_wd_node', 'ent_wd_label', 'ent_wd_description',
                                   'ent_ta2wd_node', 'ent_ta2wd_label',
                                   'ent_ta2wd_description'])

    return ent_df


def init_ta2_rel_dataframe() -> pd.DataFrame:
    rel_df = pd.DataFrame(columns=['ta2_team_name', 'ta1_team_name', 'schema_instance_id',
                                   'schema_id', 'instance_id',  'rel_id', 'rel_name',
                                   'rel_relationSubject', 'rel_relationPredicate',
                                   'rel_relationObject',
                                   'rel_relationProvenance', 'rel_relationSubject_prov',
                                   'rel_relationObject_prov', 'rel_ta1ref',
                                   'rel_confidence', 'rel_inEvent'])

    return rel_df


class TA2CEInstance:
    """
    Object that stores one Instantation of a TA1-TA2 System pair on a single "instance"
    within a SDF File of a Complex Event
    """

    def __init__(self, ta1_team_name, ta2_team_name, is_task2, ce_name, instance_id, instance_name,
                 schema_id, file_name):
        """
        Initialize Object
        """
        # Right now, extraction is required to get the ta1_team_name, so it is blank for now
        self.ta1_team_name = ta1_team_name
        self.ta2_team_name = ta2_team_name
        self.is_task2 = is_task2
        self.ce_name = ce_name
        self.instance_id = instance_id
        self.instance_name = instance_name
        self.instance_id_short = str(instance_id.split('/')[1])
        self.schema_id = schema_id
        self.file_name = file_name
        task_str = "task1"
        if self.is_task2:
            task_str = "task2"
        self.schema_instance_id = "{}-{}-{}-{}.json_{}".format(ta1_team_name.lower(),
                                                               ta2_team_name.lower(),
                                                               task_str, ce_name,
                                                               self.instance_id_short)
        self.ce_instance_file_name_base = "{}-{}-{}-{}-{}".format(ta1_team_name,
                                                                  ta2_team_name,
                                                                  task_str, ce_name,
                                                                  self.instance_id_short)
        self.ce_instance_file_name_base = self.ce_instance_file_name_base.\
            replace("/", "-").replace(":", "-").lower()
        self.ev_df = init_ta2_ev_dataframe()
        self.arg_df = init_ta2_arg_dataframe()
        self.ent_df = init_ta2_ent_dataframe()
        self.rel_df = init_ta2_rel_dataframe()
        self.temporalrel_df = init_ta2_rel_dataframe()
        self.children_df = init_ta2_children_dataframe()
        self.schema_df = init_ta2_schema_dataframe()
        self.ta2_tree = None

    def extract_contents_from_sdf(self, ta2_team_name: str,
                                  instance_schema: dict,
                                  json_dict: dict, file_name: str):
        """

        Args:
            ta2_team_name:
            instance_schema:
            json_dict:
            file_name:
            instance_id:

        Returns:

        """
        kevs.extract_elements_from_json.\
            extract_ta2_elements_from_json_instance(ta2_team_name, self, instance_schema,
                                                    json_dict, file_name, self.instance_id,
                                                    self.instance_name)

    def import_extractions_from_files(self, ta2_extraction_dir: str):
        """

        Args:
            ta2_extraction_dir:

        Returns:

        """
        self.schema_df = pd.read_csv(os.path.join(ta2_extraction_dir, 'Extracted_Schemas',
                                                  os.path.basename(self.ce_instance_file_name_base)
                                                  + '_schema.csv'))
        self.ev_df = pd.read_csv(os.path.join(ta2_extraction_dir, 'Extracted_Events',
                                              os.path.basename(self.ce_instance_file_name_base)
                                              + '_ev.csv'))
        self.arg_df = pd.read_csv(os.path.join(ta2_extraction_dir, 'Extracted_Arguments',
                                               os.path.basename(self.ce_instance_file_name_base)
                                               + '_arg.csv'))
        try:
            self.children_df = pd.read_csv(os.path.join(ta2_extraction_dir, 'Extracted_Children',
                                                        os.path.basename(
                                                            self.ce_instance_file_name_base)
                                                        + '_children.csv'))
        except FileNotFoundError:
            pass
        try:
            self.ent_df = pd.read_csv(os.path.join(ta2_extraction_dir, 'Extracted_Entities',
                                                   os.path.basename(self.ce_instance_file_name_base)
                                                   + '_ent.csv'))
        except FileNotFoundError:
            pass
        try:
            self.rel_df = pd.read_csv(os.path.join(ta2_extraction_dir, 'Extracted_Relations',
                                                   os.path.basename(self.ce_instance_file_name_base)
                                                   + '_rel.csv'))
        except FileNotFoundError:
            pass
        try:
            self.temporalrel_df = pd.read_csv(os.path.join(ta2_extraction_dir,
                                                           'Extracted_TemporalRelations',
                                                           os.path.basename(
                                                               self.ce_instance_file_name_base)
                                                           + '_temporalrel.csv'))
        except FileNotFoundError:
            pass
        # Need to get the TA1 ID
        self.ta1_team_name = self.schema_df.iloc[0, :]['ta1_team_name']

    def write_extractions_to_files(self, score_directory: str) -> None:
        """
        Writes the extracted objects to Files
        Returns:

        """
        # create schema, event, argument, children directories if not exist
        if not os.path.isdir(score_directory):
            os.makedirs(score_directory)
        if not os.path.isdir(os.path.join(score_directory, self.ta2_team_name)):
            os.makedirs(os.path.join(score_directory, self.ta2_team_name))
        if not os.path.isdir(os.path.join(score_directory, self.ta2_team_name,
                                          "Extracted_Schemas")):
            os.makedirs(os.path.join(score_directory, self.ta2_team_name, "Extracted_Schemas"))
        if not os.path.isdir(os.path.join(score_directory, self.ta2_team_name,
                                          "Extracted_Events")):
            os.makedirs(os.path.join(score_directory, self.ta2_team_name, "Extracted_Events"))
        if not os.path.isdir(os.path.join(score_directory, self.ta2_team_name,
                                          "Extracted_Arguments")):
            os.makedirs(os.path.join(score_directory, self.ta2_team_name, "Extracted_Arguments"))
        if not os.path.isdir(os.path.join(score_directory, self.ta2_team_name,
                                          "Extracted_Children")):
            os.makedirs(os.path.join(score_directory, self.ta2_team_name, "Extracted_Children"))
        if not os.path.isdir(os.path.join(score_directory, self.ta2_team_name,
                                          "Extracted_Entities")):
            os.makedirs(os.path.join(score_directory, self.ta2_team_name, "Extracted_Entities"))
        if not os.path.isdir(os.path.join(score_directory, self.ta2_team_name,
                                          "Extracted_Relations")):
            os.makedirs(os.path.join(score_directory, self.ta2_team_name, "Extracted_Relations"))
        if not os.path.isdir(os.path.join(score_directory, self.ta2_team_name,
                                          "Extracted_TemporalRelations")):
            os.makedirs(os.path.join(score_directory, self.ta2_team_name,
                                     "Extracted_TemporalRelations"))
        if len(self.schema_df) > 0:
            self.schema_df.to_csv(os.path.join(score_directory, self.ta2_team_name,
                                               'Extracted_Schemas',
                                               os.path.basename(self.ce_instance_file_name_base)
                                               + '_schema.csv'), index=False)
        if len(self.ev_df) > 0:
            self.ev_df.to_csv(os.path.join(score_directory, self.ta2_team_name, 'Extracted_Events',
                                           os.path.basename(self.ce_instance_file_name_base)
                                           + '_ev.csv'), index=False)
        if len(self.arg_df) > 0:
            self.arg_df.to_csv(os.path.join(score_directory, self.ta2_team_name,
                                            'Extracted_Arguments',
                                            os.path.basename(self.ce_instance_file_name_base)
                                            + '_arg.csv'), index=False)
        if len(self.children_df) > 0:
            self.children_df.to_csv(os.path.join(score_directory, self.ta2_team_name,
                                                 'Extracted_Children',
                                                 os.path.basename(self.ce_instance_file_name_base)
                                                 + '_children.csv'), index=False)
        if len(self.ent_df) > 0:
            self.ent_df.to_csv(os.path.join(score_directory, self.ta2_team_name,
                                            'Extracted_Entities',
                                            os.path.basename(self.ce_instance_file_name_base)
                                            + '_ent.csv'), index=False)
        if len(self.rel_df) > 0:
            self.rel_df.to_csv(os.path.join(score_directory, self.ta2_team_name,
                                            'Extracted_Relations',
                                            os.path.basename(self.ce_instance_file_name_base)
                                            + '_rel.csv'), index=False)
        if len(self.temporalrel_df) > 0:
            self.temporalrel_df.to_csv(os.path.join(score_directory, self.ta2_team_name,
                                                    'Extracted_TemporalRelations',
                                                    os.path.basename(
                                                        self.ce_instance_file_name_base)
                                                    + '_temporalrel.csv'), index=False)

    def produce_event_tree(self, output_dir, include_all_events):
        tree_output_dir = os.path.join(output_dir, self.ta2_team_name, "Event_Trees")
        if not os.path.isdir(tree_output_dir):
            os.makedirs(tree_output_dir)
        self.ta2_tree = kevs.produce_event_trees.create_ta2_event_tree(self, tree_output_dir,
                                                                       include_all_events)


class TA2Instantiation:
    """
    Container for all Instances of a single TA2 system
    (not a single TA1-TA2 pair). Contains a dictionary of TA2Instantiations with key
    <ta1_team_name>|<ta2_team_name>|<ce_name>|<instance_id>
    and value of the TA2CEInstance, where
    <complete_file_name> =  ta1_team_name + '-' + ta2_team_name + '-' +
    task<1/2> + '-' + 'ce_name' + instance_id.replace("/", "-")
    """

    def __init__(self, ta2_team_name, is_task2=False):
        """
        Initialize Object
        """
        self.ta2dict = dict()
        self.ta2_team_name = ta2_team_name
        self.is_task2 = is_task2
        self.ev_df = init_ta2_ev_dataframe()
        self.arg_df = init_ta2_arg_dataframe()
        self.ent_df = init_ta2_ent_dataframe()
        self.rel_df = init_ta2_rel_dataframe()
        self.temporalrel_df = init_ta2_rel_dataframe()
        self.children_df = init_ta2_children_dataframe()
        self.schema_df = init_ta2_schema_dataframe()

    def extract_contents_from_sdf(self, sdf_collection_dir):
        """

        Args:
            sdf_collection_dir:

        Returns:

        """
        json_dict = load.load_json_directory(self.ta2_team_name, sdf_collection_dir)

        # Reinitiate dictionary and data frames to be empty
        self.ta2dict = dict()
        self.ev_df = init_ta2_ev_dataframe()
        self.arg_df = init_ta2_arg_dataframe()
        self.ent_df = init_ta2_ent_dataframe()
        self.rel_df = init_ta2_rel_dataframe()
        self.temporalrel_df = init_ta2_rel_dataframe()
        self.children_df = init_ta2_children_dataframe()
        self.schema_df = init_ta2_schema_dataframe()

        for file_name in json_dict:
            # initiate schema, event, argument, and children dataframes
            # Parse the information from the filename
            # All TA2 File Submissions follow <ta1>-<ta2>-<task>-<ce>
            file_split = file_name.split('.')[0].split('-')
            ta1_team_name = file_split[0].upper()
            ce_name = file_split[3].lower()
            schema = json_dict[file_name]
            for instance_schema in schema['instances']:
                instance_name = instance_schema['name']
                instance_id = instance_schema['@id']
                schema_id = None
                if '@id' in schema.keys():
                    schema_id = schema['@id']
                # initiate schema, event, argument, and children dataframes
                ta2_ceinstance = TA2CEInstance(ta1_team_name, self.ta2_team_name,
                                               self.is_task2, ce_name, instance_id, instance_name,
                                               schema_id, file_name)
                ta2_ceinstance.extract_contents_from_sdf(self.ta2_team_name,
                                                         instance_schema, json_dict,
                                                         file_name)

                # save schema, event, argument, children, entity, and relation
                # dataframes to csv files
                self.ta2dict['{}|{}|{}|{}'.format(ta2_ceinstance.ta1_team_name, self.ta2_team_name,
                                                  ta2_ceinstance.ce_name,
                                                  instance_id)] = ta2_ceinstance
                # Now append the data frames to form the group data frames for the ta2_instantiation
                self.ev_df = pd.concat([self.ev_df, ta2_ceinstance.ev_df], ignore_index=True)
                self.arg_df = pd.concat([self.arg_df, ta2_ceinstance.arg_df], ignore_index=True)
                self.ent_df = pd.concat([self.ent_df, ta2_ceinstance.ent_df], ignore_index=True)
                self.rel_df = pd.concat([self.rel_df, ta2_ceinstance.rel_df], ignore_index=True)
                self.temporalrel_df = pd.concat([self.temporalrel_df,
                                                 ta2_ceinstance.temporalrel_df], ignore_index=True)
                self.children_df = pd.concat([self.children_df, ta2_ceinstance.children_df],
                                             ignore_index=True)
                self.schema_df = pd.concat([self.schema_df, ta2_ceinstance.schema_df],
                                           ignore_index=True)

    def import_extractions_from_files(self, team_directory):
        """
        Method that imports all TA1 Libraries from extractions and labels them.
        Args:
            team_directory:

        Returns:
        """
        # Strategy: Read each file in the "Extracted Events Directory". Use that filename
        # to obtain the right TA1 library and then load the others
        # Loop over all of the different teams from the collection directory
        team_ev_directory = os.path.join(team_directory, 'Extracted_Events')
        for file_name in os.listdir(team_ev_directory):
            if file_name.startswith('.') or (not file_name.endswith('.csv')):
                continue
            # This assumes a filename of
            # <ta1_team_name>-<ta2_team_name>-<task1/2>-<ce_name>-<instance_id>_ev.csv
            file_split = file_name.split('.')[0].split('-')
            ta1_team_name = file_split[0].upper()
            ta2_team_name = file_split[1].upper()
            is_task2 = (file_split[2].lower() == "task2")
            ce_name = file_split[3].lower()
            # We must read the events file to get the instance name
            temp_ev_df = pd.read_csv(os.path.join(team_ev_directory, file_name))
            schema_id = temp_ev_df.iloc[0, :]['schema_id']
            instance_name = temp_ev_df.iloc[0, :]['instance_name']
            instance_id = temp_ev_df.iloc[0, :]['instance_id']
            # Get json_file_name and pass it as the file name
            json_file_name = file_name.replace("_ev.csv", ".json")
            ta2_ceinstance = TA2CEInstance(ta1_team_name, ta2_team_name, is_task2, ce_name,
                                           instance_id, instance_name, schema_id, json_file_name)
            ta2_ceinstance.import_extractions_from_files(team_directory)
            # Get the ta1_team by extracting the event
            self.ta2dict['{}|{}|{}|{}'.format(ta2_ceinstance.ta1_team_name, self.ta2_team_name,
                                              ce_name, instance_id)] = ta2_ceinstance

        # Now import the group data frames
        # System level readings
        self.ev_df = pd.read_csv(os.path.join(team_directory, "System_Level_Extractions",
                                              "extracted_sys_events.csv"))
        self.arg_df = pd.read_csv(os.path.join(team_directory, "System_Level_Extractions",
                                               "extracted_sys_arg.csv"))
        try:
            self.rel_df = pd.read_csv(os.path.join(team_directory, "System_Level_Extractions",
                                                   "extracted_sys_rel.csv"))
        except FileNotFoundError:
            pass
        try:
            self.temporalrel_df = pd.read_csv(os.path.join(team_directory,
                                                           "System_Level_Extractions",
                                                           "extracted_sys_temporalrel.csv"))
        except FileNotFoundError:
            pass
        try:
            self.ent_df = pd.read_csv(os.path.join(team_directory, "System_Level_Extractions",
                                                   "extracted_sys_ent.csv"))
        except FileNotFoundError:
            pass
        try:
            self.children_df = pd.read_csv(os.path.join(team_directory, "System_Level_Extractions",
                                                        "extracted_sys_children.csv"))
        except FileNotFoundError:
            pass
        self.schema_df = pd.read_csv(os.path.join(team_directory, "System_Level_Extractions",
                                                  "extracted_sys_schema.csv"))

    def write_extractions_to_files(self, score_dir):
        """
        Write Extractions of all TA2 Instantiation objects to their respective files

        Args:
            score_dir:

        Returns:

        """

        # create schema, event, argument, children directories if not exist
        kevs.extract_elements_from_json.create_extraction_directories(score_dir, self.ta2_team_name)
        for key, value in self.ta2dict.items():
            value.write_extractions_to_files(score_dir)
        # Now write the group File Extractions
        # Now write any system-specific output
        if len(self.schema_df) > 0:
            self.schema_df. \
                to_csv(os.path.join(score_dir, self.ta2_team_name, 'System_Level_Extractions',
                                    'extracted_sys_schema.csv'),
                       index=False)
        if len(self.ev_df) > 0:
            self.ev_df. \
                to_csv(os.path.join(score_dir, self.ta2_team_name, 'System_Level_Extractions',
                                    'extracted_sys_events.csv'),
                       index=False)
        if len(self.arg_df) > 0:
            self.arg_df. \
                to_csv(os.path.join(score_dir, self.ta2_team_name, 'System_Level_Extractions',
                                    'extracted_sys_arg.csv'),
                       index=False)
        if len(self.children_df) > 0:
            self.children_df. \
                to_csv(os.path.join(score_dir, self.ta2_team_name, 'System_Level_Extractions',
                                    'extracted_sys_children.csv'),
                       index=False)
        if len(self.ent_df) > 0:
            self.ent_df. \
                to_csv(os.path.join(score_dir, self.ta2_team_name, 'System_Level_Extractions',
                                    'extracted_sys_ent.csv'),
                       index=False)
        if len(self.rel_df) > 0:
            self.rel_df. \
                to_csv(os.path.join(score_dir, self.ta2_team_name, 'System_Level_Extractions',
                                    'extracted_sys_rel.csv'),
                       index=False)
        if len(self.temporalrel_df) > 0:
            self.temporalrel_df. \
                to_csv(os.path.join(score_dir, self.ta2_team_name, 'System_Level_Extractions',
                                    'extracted_sys_temporalrel.csv'),
                       index=False)

    def produce_event_trees(self, output_dir, include_all_events):
        for key, value in self.ta2dict.items():
            ta2_ce_instance = value
            ta2_ce_instance.produce_event_tree(output_dir, include_all_events)


class TA2Collection:
    """
    Container for a dictionary of TA2Instantion with key that has one entry per system
    <ta2_team_name>
    and value of the TA2Instantiation
    """

    def __init__(self, is_task2=False):
        """
        Initialize Object
        """
        self.ta2dict = dict()
        self.is_task2 = is_task2

    def extract_contents_from_sdf(self, sdf_collection_dir):
        """

        Args:
            json_collection_dir:

        Returns:

        """
    pass

    def import_extractions_from_file_collection(self, collection_dir, extract_for_graph_g=False):
        """
        Method that imports all TA1 Libraries from extractions and labels them.
        Args:
            collection_dir:
            extract_for_graph_g: bool of whether or not to extract for graph_g

        Returns:
        """
        # Strategy: Read each file in the "Extracted Events Directory". Use that filename
        # to obtain the right TA1 library and then load the others
        # Loop over all of the different teams from the collection directory
        for file_dir in os.listdir(collection_dir):
            if os.path.isdir(os.path.join(collection_dir, file_dir)):
                ta2_team = file_dir
                team_directory = os.path.join(collection_dir, ta2_team)
                ta2_instance = TA2Instantiation(ta2_team, self.is_task2)
                try:
                    ta2_instance.import_extractions_from_files(team_directory)
                    # Get the ta1_team by extracting the event
                    self.ta2dict['{}'.format(ta2_team)] = ta2_instance
                except FileNotFoundError as e:
                    print("Warning: TA2 Team: " + ta2_team +
                          " is missing files and was not imported. Exception Message:")
                    print(e)

    def write_extractions_to_file_collection(self, score_dir):
        """
        Write Extractions of all TA2 Instantiation objects to their respective files

        Args:
            collection_dir:

        Returns:

        """
        for key, value in self.ta2dict.items():
            key_items = key.split('|')
            ta2_team_name = key_items[1]
            file_name = key_items[2]
            value.write_extraction_to_file(score_dir, ta2_team_name, file_name)

    def produce_event_trees(self, output_dir, include_all_events):
        for key, value in self.ta2dict.items():
            ta2_instantiation = value
            ta2_instantiation.produce_event_trees(output_dir, include_all_events)
