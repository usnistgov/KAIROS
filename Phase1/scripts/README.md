This folder contains script files that support `score_task1.py` and 
`score_task2.py`.

Date: 02/03/2021

The folder includes the following main Python scripts:
- Script: `score_task1_event_argument.py`
  (argument part is waiting for LDC argument assessment examples...)
  - Description:  Extracts events from TA2 task1 outputs as dataframes.
    Then extracted events are assigned with assessments according to LDC assessment results
    and id mapping provided by CACI.
    After that, event stats are computed based on the assessed event dataframes and LDC annotations.
    Finally, the event precision, recall, and F measure score is calculated based on the stats.
  - Inputs:
    - Files: TA2 task1 outputs, LDC annotations, LDC assessments, CACI id mappings
    - Configuration: config.ini in root folder, which specifies paths to input directories
      - for input directory/file organization, please refer to 
        [Setup](../README.md) part in root README.md
    - Variables: ta2_team_name, mode, ta2_ext, event_assess
      - for details please refer to [CLI options](../README.md) part in root README.md
  - Outputs:
    - task1_(file/schema)_score_event_argument\_[TA2].xlsx in 
      task1_score_directory/[TA2]/ folder
    - extracted TA2 task1 event dataframes in task1_score_directory/[TA2]/Event_arguments/ folder
    - assessed TA2 task1 event dataframes in task1_score_directory/[TA2]/Assessed_events/ folder
    
- Script: `score_task1_order.py`
  - Description:  Extracts orders from TA2 task1 outputs as dataframes.
    Then extracted orders are assigned with assessments according to assessed events
    and LDC temporal annotations.
    Additionally, reference order pairs are calculated based on LDC temporal annotations.
    After that, order stats are computed based on the assessed order dataframes and reference order pairs.
    Finally, the order precision, recall, and F measure score is calculated based on the stats.
  - Inputs:
    - Files: TA2 task1 outputs, LDC annotations, Assessed events produced by `score_task1_event_argument.py`
    - Configuration: config.ini in root folder, which specifies paths to input directories
      - for input directory/file organization, please refer to 
        [Setup](../README.md) part in root README.md
    - Variables: ta2_team_name, mode, ta2_ext, ann_ext
      - for details please refer to [CLI options](../README.md) part in root README.md
  - Outputs:
    - task1_(file/schema)_score_order\_[TA2].xlsx in 
      task1_score_directory/[TA2]/ folder
    - extracted TA2 task1 order dataframes in task1_score_directory/[TA2]/Orders/ folder
      
- Script: `score_task1_relation.py` (waiting for LDC relation assessment examples)

- Script: `score_task2_event_argument.py`
  - Description:  Extracts events and arguments from Graph G, TA1 schema libraries,
    and TA2 task2 outputs as dataframes. Then event and argument stats are computed 
    based on the extracted dataframes. Finally, the event and argument recall score and 
    diagnosis are calculated based on the stats.
  - Inputs:
    - Files: TA1 schema libraries, TA2 task2 outputs, Graph G, LDC annotations
    - Configuration: config.ini in root folder, which specifies paths to input directories
      - for input directory/file organization, please refer to 
        [Setup](../README.md) part in root README.md
    - Variables: ta2_team_name, graph_g_ext, ta1_ext, ta2_ext, mode
      - for details please refer to [CLI options](../README.md) part in root README.md
  - Outputs:
    - task2_(file/schema)_score_event_argument\_[TA2].xlsx in 
      task2_score_directory/[TA2]/ folder
    - task2\_(file/schema)_diagnosis_event_argument\_[TA2].xlsx files in 
      task2_score_directory/[TA2]/Diagnosis/ folder
    - extracted TA2 task2 event and argument dataframes in 
      task2_score_directory/[TA2]/Event_arguments/ folder
      
- Script: `score_task2_order.py`
  - Description: Extracts orders from Graph G and TA2 task2 outputs as dataframes. 
    Then order stats are computed based on the extracted order dataframes, 
    extracted event dataframes that is produced by `score_task2_event_argument.py`, 
    LDC annotations, and reference order pairs.
    Reference order pairs are computed based on the LDC temporal annotations.
    Finally, the order recall score is calculated based on the stats and reference order pairs.
  - Inputs:
    - Files: TA2 task2 outputs, Graph G, LDC annotations
    - Configuration: config.ini in root folder, which specifies paths to input directories
      - for input directory/file organization, please refer to 
        [Setup](../README.md) part in root README.md
    - Variables: ta2_team_name, graph_g_ext, ta2_ext, mode
      - for details please refer to [CLI options](../README.md) part in root README.md
  - Outputs:
    - task2_(file/schema)_score_order\_[TA2].xlsx in task2_score_directory/[TA2]/ folder
    - extracted TA2 task2 orders in task2_score_directory/[TA2]/Orders/ folder
    
- Script: `score_task2_relation.py`
  - Description: Extracts relations from Graph G and TA2 task2 outputs as dataframes. 
    Then relation stats are computed based on the extracted relation dataframes,
    extracted event dataframes that is produced by `score_task2_event_argument.py`,
    and LDC annotations. 
    Finally, the relation recall score is calculated based on the stats.
  - Inputs:
    - Files: TA2 task2 outputs, Graph G, LDC annotations
    - Configuration: config.ini in root folder, which specifies paths to input directories
      - for input directory/file organization, please refer to 
        [Setup](../README.md) part in root README.md
    - Variables: ta2_team_name, graph_g_ext, ta2_ext, mode
      - for details please refer to [CLI options](../README.md) part in root README.md
  - Outputs:
    - task2_(file/schema)_score_relation\_[TA2].xlsx in task2_score_directory/[TA2]/ folder
    - extracted TA2 task2 relations in task2_score_directory/[TA2]/Relations/ folder
    
- Script: `extract_event_argument.py`
  - Description: Load data from Graph G or TA1/TA2 output json files, 
    extract event (step) and argument (participant) related information 
    using pandas dataframe and save as csv files
  - Inputs:
    - Files: Graph G files, TA1 schema libraries, TA2 task1/task2 outputs
    - Variables: ta2_team_name, input_directory, output_directory
  - Outputs:
    - extracted event and argument csv files in task1(task2)_score_directory/[TA2]/Event_arguments/ folder
    - extracted event and argument csv files in task2_score_directory/TA1_library/[TA1]/Event_arguments/ folder
    - separate extracted event and argument csv files in graph_g_directory/target_ce/ folder
    
- Script: `extract_order.py`
  - Description: Load data from Graph G or TA2 output json files, 
    extract order related information using pandas dataframe and save as csv files. 
    Also computes reference order pairs based on the LDC temporal annotations.
  - Inputs:
    - Files: Graph G files, TA2 task1/task2 outputs, LDC temporal annotations
    - Variables: ta2_team_name, input_directory, output_directory
  - Outputs:
    - extracted order csv files in task1(task2)_score_directory/[TA2]/Orders/ folder
    - extracted order csv files in graph_g_directory/target_ce/ folder
    - reference order pair xlsx files in annotation_directory/target_ce/ folder
    
- Script: `extract_relation.py`
  - Description: Load data from Graph G or TA2 output json files, 
    extract relation information using pandas dataframe and save as csv files
  - Inputs:
    - Files: Graph G files, TA2 task1/task2 outputs
    - Variables: ta2_team_name, input_directory, output_directory
  - Outputs:
    - extracted relation csv files in task1(task2)_score_directory/[TA2]/Relations/ folder
    - extracted relation csv files in graph_g_directory/target_ce/ folder
    