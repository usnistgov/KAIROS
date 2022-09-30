import pandas as pd
import os
import ast

import json
import requests
import itertools
import time

# code taken from https://github.com/usc-isi-i2/kgtk-similarity


def call_semantic_similarity(input_file, url):
    file_name = os.path.basename(input_file)
    files = {
        'file': (file_name, open(input_file, mode='rb'), 'application/octet-stream')
    }
    resp = requests.post(url, files=files, params={'similarity_types': 'all'})
    s = json.loads(resp.json())
    return pd.DataFrame(s)


# adapted from https://github.com/usc-isi-i2/kgtk-similarity
# See note: (to limit CPU resources, at most 100 pairs will be compared in a single request)
def get_bulk_qnode_sim_scores(qnode_in_pair_df, qnode_directory, qnode_sim_fp,
                              use_cached_queries=True):
    qnode_pair_df = qnode_in_pair_df.copy(deep=True)
    qnode_sim_cached_df = pd.read_csv(qnode_sim_fp, sep='\t')
    url = 'https://kgtk.isi.edu/similarity_api'
    temp_fp = os.path.join(qnode_directory, "temp_nist_sim_api_input_file.tsv")
    qnode_sim_df = pd.DataFrame(columns=['q1', 'q2', 'complex', 'q1_label', 'q2_label',
                                         'transe', 'text', 'class', 'jc', 'topsim'])
    added_entries = False
    # To avoid repeat computations for debugging, use cached queries whenever possible

    # Remove EMPTY_TBD Queries from cache since they are stored as a blank
    qnode_pair_df = qnode_pair_df.loc[qnode_pair_df['q2'] != "EMPTY_TBD", :]

    # To DO: Fix a caching bug to make qnode_pair_df empty as there seems to be an error
    # outside of the coding
    if use_cached_queries:
        qnode_sim_df = pd.merge(qnode_pair_df, qnode_sim_cached_df, how="left")
        qnode_check_df = qnode_sim_df.loc[pd.isna(qnode_sim_df['topsim']), ["q1", "q2"]]
        qnode_check_df.reset_index(drop=True, inplace=True)
        qnode_sim_df = qnode_sim_df.loc[pd.notna(qnode_sim_df['topsim']), :]
    else:
        qnode_check_df = qnode_pair_df

    curr_start = 0
    while curr_start < qnode_check_df.shape[0]:
        # So we do not spam the query server
        time.sleep(0.01)
        if curr_start + 19 <= qnode_check_df.shape[0]:
            temp_df = qnode_check_df.loc[curr_start:curr_start+19, ]
        else:
            temp_df = qnode_check_df.loc[curr_start:, ]
        # Write temp file for each query
        temp_df.to_csv(temp_fp, index=False, sep='\t')
        qnode_temp_df = call_semantic_similarity(temp_fp, url)
        added_entries = True
        qnode_sim_df = pd.concat([qnode_sim_df, qnode_temp_df], ignore_index=True)
        curr_start += 20

    if added_entries:
        # Remove any duplicates from the enhanced file
        qnode_sim_df = qnode_sim_df.drop_duplicates().reset_index(drop=True)
        # Remove any duplicates from the cached file
        qnode_sim_cached_df = pd.concat([qnode_sim_cached_df, qnode_sim_df], ignore_index=True). \
            drop_duplicates().reset_index(drop=True)
        qnode_sim_cached_df.to_csv(qnode_sim_fp, index=False, sep='\t')

    return qnode_sim_df


def clean_qnode_field(qnode: str):
    if '[' in qnode:
        new_qnode = ast.literal_eval(qnode)
    else:
        new_qnode = [qnode]
    new_qnode = [str.replace("wd:", "").replace("wiki:", "") for str in new_qnode]
    return new_qnode


def process_ta2_qnode_fields(ta2_df, id_field_name="id", qnode_field_name="qnode",
                             qnode_backup_field_name="ev_qnode"):
    """
    Method to process the qnode field in a ta2 dataframe. Requires an "id" field and a "qnode"
    field. They can be specified by the parameters id_field_name and qnode_field_name to
    support different data frames.
    Args:
        ta2_df:
        id_field_name:
        qnode_field_name:
        qnode_backup_field_name:

    Returns:
        A data frame with "id", and "qnode_proc", where the qnodes are processed.
        If an event id has multiple qnodes, multiple qnodes appear here.
    """
    ta2_proc_df = ta2_df.loc[:, [id_field_name, qnode_field_name,
                                 qnode_backup_field_name]].copy(deep=True)

    ta2_proc_df.rename(columns={id_field_name: "id", qnode_field_name: "qnode_orig"},
                       inplace=True)
    # First, use the backup qnode field to replace the qnode field
    ta2_proc_df["qnode_orig"] = ta2_proc_df.loc[:, "qnode_orig"].\
        fillna(ta2_proc_df[qnode_backup_field_name])
    ta2_proc_df.drop(columns=[qnode_backup_field_name], inplace=True)
    # Now drop what is still missing
    ta2_proc_df.dropna(axis=0, inplace=True)
    ta2_proc_df['qnode_proc'] = ta2_proc_df['qnode_orig'].apply(clean_qnode_field)
    ta2_proc_df = ta2_proc_df.explode("qnode_proc")
    return ta2_proc_df.loc[:, ["id", "qnode_proc"]]


def process_annotation_qnode_fields(annotation_df, id_field_name="id", qnode_field_name="qnode"):
    """

    Args:
        annotation_df:
        id_field_name:
        qnode_field_name:

    Returns:

    """
    annotation_proc_df = annotation_df.loc[:, [id_field_name, qnode_field_name]].copy(deep=True)
    annotation_proc_df.rename(columns={id_field_name: "id", qnode_field_name: "qnode_proc"},
                              inplace=True)
    return annotation_proc_df


def produce_qnode_pair_df(ta2_proc_qnode_df, ann_proc_qnode_df):
    """
    Takes in a ta2 data frame and an annotation dataframe, and produces a qnode pairs file.
    The qnode pairs file is suitable for the ISI API, as documented at
    https://github.com/usc-isi-i2/kgtk-similarity. Uses processed qnode files from method
    process_ta2_qnode_fields and process_annotation_qnode_fields
    Args:
        ta2_proc_qnode_df:
        ann_proc_qnode_df:
    Returns:
        A data frame with fields q1 (for the ta2 ev_qnode) and q2 (for the annotation ev qnode)
    """
    ta2_qnode_list = ta2_proc_qnode_df['qnode_proc']
    ann_qnode_list = ann_proc_qnode_df['qnode_proc']

    prod_df = pd.DataFrame(itertools.product(ta2_qnode_list, ann_qnode_list), columns=["q1", "q2"])
    # Drop duplicates
    prod_df = prod_df.drop_duplicates().reset_index(drop=True)
    return prod_df
