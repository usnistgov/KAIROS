import os
import pandas as pd


class CEAnnotation:

    def __init__(self, complex_event, is_task2=False):
        self.complex_event = complex_event
        self.is_task2 = is_task2
        self.ep_df = pd.DataFrame()
        self.arg_df = pd.DataFrame()
        self.temporal_df = pd.DataFrame()
        self.entity_qnode_df = pd.DataFrame()
        self.ep_sel_df = pd.DataFrame()
        self.er_sel_df = pd.DataFrame()
        self.ep_partition_df = pd.DataFrame()

    def import_annotation(self, ce_annotation_dir):
        """

        Args:
            ce_annotation_dir:

        Returns:

        """
        if os.path.exists(ce_annotation_dir + '/' +
                          self.complex_event + '_events_selected.xlsx'):
            self.ep_sel_df = pd.read_excel(ce_annotation_dir + '/' +
                                           self.complex_event + '_events_selected.xlsx')
        self.ep_df = pd.read_excel(ce_annotation_dir + '/' +
                                   self.complex_event + '_events.xlsx')
        self.arg_df = pd.read_excel(ce_annotation_dir + '/' +
                                    self.complex_event + '_arguments.xlsx')
        self.temporal_df = pd.read_excel(ce_annotation_dir + '/' +
                                         self.complex_event + '_temporal.xlsx')
        self.entity_qnode_df = pd.read_excel(ce_annotation_dir + '/' +
                                             self.complex_event + '_kb_linking.xlsx')
        self.ep_partition_df = pd.read_excel(ce_annotation_dir + '/' +
                                             self.complex_event + '_partition.xlsx')
        self.er_sel_df = pd.DataFrame()
        if os.path.exists(ce_annotation_dir + '/' +
                          self.complex_event + '_relations_selected.xlsx'):
            self.er_sel_df = pd.read_excel(ce_annotation_dir + '/' +
                                           self.complex_event + '_relations_selected.xlsx')

    def export_graph_g(self, output_dir):
        """

        Args:
            output_dir:

        Returns:

        """
        pass


class Annotation:
    """
    A collection of Complex Event (CE) Annotations,
    for a task. Contains a dictionary of Annotation objects
    with key
    <task_number>|<ce>
    """

    def __init__(self, is_task2=False):
        self.annotation_dict = {}
        self.is_task2 = is_task2

    def import_all_annotation(self, annotation_dir):
        """
        Method that imports all TA1 Libraries from extractions and labels them.
        Args:
            annotation_dir:
            is_task2:

        Returns:
        """
        # Strategy: Read each file in the "Extracted Events Directory". Use that filename
        # to obtain the right TA1 library and then load the others
        # Loop over all of the different teams from the collection directory
        task_str = "task1"
        if self.is_task2:
            task_str = "task2"
        for file_dir in os.listdir(annotation_dir):
            if os.path.isdir(os.path.join(annotation_dir, file_dir)):
                ce = file_dir
                ce_directory = os.path.join(annotation_dir, ce)
                try:
                    ce_annotation = CEAnnotation(ce, self.is_task2)
                    ce_annotation.import_annotation(ce_directory)
                    self.annotation_dict['{}|{}'.format(task_str, ce)] = ce_annotation
                except FileNotFoundError as e:
                    print("Warning: Annotation Event: " + ce + " for " + task_str +
                          " is missing files and was not imported. Exception Message:")
                    print(e)

    def export_graph_g_collection(self, output_dir):
        """

        Args:
            output_dir:

        Returns:

        """
        pass
