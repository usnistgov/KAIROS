# KAIROS Scorer for Phase 1 Evaluation

Date: 02/02/2021
<br>Version: v0.5.2

## Note to Readers:

To be transparent with stakeholders about the direction in which the KAIROS evaluation 
and metrics are headed, NIST releases this very early version of the scorer. This 
software in its present form is released for illustrative purposes only and should 
not be relied upon for system performance measurement or optimization, and is not 
intended to be perceived as a functional version of the scorer.

This software is the very first draft of the Phase I KAIROS evaluation scorer. Many 
aspects of the KAIROS program that are required by the scorer are rapidly evolving 
and are still underspecified. As a result, the scorer is likely to undergo significant 
changes in the coming weeks. Some underdefined aspects, such as assessment format 
and output, are completely made up with random values. These aspects are fast evolving 
and will be addressed in following versions of the scorer as they become better defined. 
At present, this software does not have any unit or integration tests and is likely 
to have bugs.

## Introduction

This package includes the draft scorer of Evaluation Task1 and Task2 for the 
Events (ev) and their corresponding Arguments (arg), Orders, and Relations (rel).

The scorer is designed for the KAIROS Phase 1 Evaluation. 
The release includes the following Python script `score_task1.py` and `score_task2.py`:
 - Script: `score_task2.py`
   - Description:
     - Main entrypoint of the task1 scorer. 
       Executes task1 scorers for event_argument, order, and relation sequentially. 
   - Inputs:
     - Files: TA2 task1 outputs, LDC assessment, CACI id mapping, and LDC annotation
     - Configuration: config.ini
        - specifies paths to input directories
        - for input directory/file organization, please refer to Setup part
     - Variables: ta2_team_name, mode, ta2_ext, event_assess, and ann_ext. Details can be found in the
       CLI options part.
   - Outputs:
     - task1_(file/schema)\_score_(event_argument/order/relation)_[TA2].xlsx in 
       task1_score_directory/[TA2]/ folder
     - extracted event_argument, order, relation, and assessed event csv files in 
       task1_score_directory/[TA2]/(Event_arguments/Orders/Relations/Assessed_events) folder
 - Script: `score_task2.py`
   - Description:
     - Main entrypoint of the task2 scorer. 
       Executes task2 scorers for event_arguments, orders, and relations sequentially. 
   - Inputs:
     - Files: TA1 schema libraries, TA2 task2 outputs, Graph G, LDC annotation
     - Configuration: config.ini
        - specifies paths to input directories
        - for input directory/file organization, please refer to Setup part
     - Variables: ta2_team_name, mode, graph_g_ext, ta1_ext, ta2_ext. Details can be found in the
       CLI options part.
   - Outputs:
     - task2_(file/schema)\_score_(event_argument/order/relation)_[TA2].xlsx in 
       task2_score_directory/[TA2]/ folder 
     - task2\_(file/schema)_diagnosis_event_argument\_[TA2].xlsx files in 
       task2_score_directory/[TA2]/Diagnosis/ folder
     - extracted event_argument, order, relation csv files in 
       task2_score_directory/[TA2]/(Event_arguments/Orders/Relations) folder
   
Scripts folder contains python scripts that are used by `score_task2.py` and `score_task2.py`. 
See the [Scripts README](scripts/README.md) for more information.
<br> Helper folder includes score-irrelative python scripts that analyze the stats 
and keywords of TA2 outputs. See the [Helper README](helpers/README.md) for more information.

Future versions will address:
- unit tests, integration tests, debugging
- code restructuring, and extensive command line arguments
- thorough documentation

## Setup:
<br> The scorer is a set of Python scripts that require:
<br> Python version 3.7.0 or later,
<br> Numpy version 1.18.1 or later,
<br> Pandas version 1.0.1 or later
<br> xlrd version 1.2.0,
<br> openpyxl version 3.0.5 or later,
<br> tqdm version 4.48.2 or later

A config.ini file is required to specify the location of input directories and output directories.
<br>Input directories are: ta1_output_directory, ta2_task1_output_directory, 
ta2_task2_output_directory, annotation_directory, graph_g_directory, assessment_directory, 
and caci_directory.
<br>Output directories are: task1_score_directory, task2_score_directory, 
task1_statistical_analysis_directory, and task2_score_analysis_directory.
  - ta1_output_directory: contains the TA1 schema libraries in SDF.
    - hierarchy should be ta1_output_directory/[TA1]/json_files
      - e.g., ta1_output_directory/CMU/cmu-schemalib.json
  - ta2_task1_output_directory: contains the TA2 task1 output files in SDF.
    - hierarchy should be ta2_task1_output_directory/[TA2]/json_files
      - e.g., ta2_task1_output_directory/IBM/cmu-ibm-ce1005-task1.json
  - ta2_task2_output_directory: contains the TA2 task2 output files in SDF.
    - hierarchy should be ta2_task2_output_directory/[TA2]/json_files
      - e.g., ta2_task2_output_directory/IBM/cmu-ibm-ce1005-task2.json
  - annotation_directory: contains LDC annotation files.
    - hierarchy should be annotation_directory/complexEvent/spread_sheets
      - e.g., annotation_directory/ce1005/ce1005_events.xlsx
  - graph_g_directory: contains graph G json files.
    - hierarchy should be graph_g_directory/json_files
      - e.g., graph_g_directory/ce1005_GraphG.json
  - task1_score_directory: stores task1 score results and 
    extracted event, argument, order, and relation csv files
  - task2_score_directory: stores task2 score and diagnosis results, and 
    extracted event, argument, order, and relation csv files
  - task1_statistical_analysis_directory: stores statistical analysis results 
    of TA2 task1 outputs 
  - task2_score_analysis_directory: stores score analysis results of TA2 task2 outputs

'config.ini' file must be located in the root directory of the scorer.

## Command Line Interface (CLI) Options:
- `-t, --ta2_team_name` - Required; Type: str; 
Currently supported ta2 team names are:
  - task1: CMU, IBM, JHU, and RESIN
  - task2: CMU, IBM, JHU, RESIN_PRIMARY, RESIN_RELAXED_ALL, and RESIN_RELAXED_ATTACK 
The scorer turns the ta2_team_name to uppercase. So cmu, CMU, Cmu are all valid.
- `-m, --mode` - Optional; Type: str; Default: evaluation;
Decides which mode to enter. There are two modes supported: evaluation and test.
Change the mode would change the input and output directory paths of the scorer.
- `-gge, --graph_g_ext` - Optional; Type: bool; Default: True;
Decides whether to extract knowledge elements from the graph g json files to serve as references. 
Only if graph g extraction is already done and no changes occurred, 
users can set -gge as false/f/no/n/0 to reduce run time.
- `-t1e, --ta1_ext` - Optional; Type: bool; Default: True;
Decides whether to extract knowledge elements from the ta1 output json files. 
Only if ta1 output extraction is already done and no changes occurred, 
users can set -t1e as false/f/no/n/0 to reduce run time.
- `-t2e, --ta1_ext` - Optional; Type: bool; Default: True;
Decides whether to extract knowledge elements from the ta1 output json files. 
Only if ta2 output extraction is already done and no changes occurred, 
users can set -t1e as false/f/no/n/0 to reduce run time.
- `-ea, --event_assess`: Optional: Type: bool, Default: True;
Decides whether to assign assessments to events.
Only if ta2 output events are already assessed and no changes occurred to 
ta2 outputs and LDC assessments, users can set -ea as false/f/no/n/0 to reduce run time.
- `-ae, --ann_ext`: Optional: Type: bool, Default: True;
Decides whether to extract annotation order pairs from temporal annotations.
Only if annotation order pairs are extracted and no changes happened to 
LDC annotation temporals, users can set -ae as false/f/no/n/0 to reduce run time.

## How to Run:
<br> Bash example - `bash example_run.sh`
<br> CLI - Below are example commands that would run the scoring script:<br>
- Task1
  - `python3 score_task_1.py -t cmu`
  - `python3 score_task_1.py -t ibm -ae false`
  - `python3 score_task_1.py -t jhu -m evaluation -ae false`
  - `python3 score_task_1.py -t resin -m evaluation -ae true`
- Task2
  - `python3 score_task_2.py -t cmu`
  - `python3 score_task_2.py -t ibm -gge false -t1e false`
  - `python3 score_task_2.py -t jhu -gge false -t1e false -t2e true`
  - `python3 score_task_2.py -t resin_primary -gge false -t1e false`
  - `python3 score_task_2.py -t resin_relaxed_all -gge false -t1e false`
  - `python3 score_task_2.py -t resin_relaxed_attack -gge false -t1e false`
  - `python3 score_task_2.py -m test -t test`

## Running Tests

The test data is external to this repository and must be obtained prior to running tests.

Code coverage of tests can be produced while running all local tests with the commands

```bash
coverage run --branch --source=. -m pytest -s tests/ -v
coverage report -m
coverage html
```

## Building Documentation

The HTML rendered documentation of the scoring software can be built with

```bash
sphinx-apidoc -fMeT -o docs/source scripts 
sphinx-apidoc -fMeT -o docs/source helpers 
sphinx-build -av --color -b html docs/source docs/build 
```

Then open the docs/build/index.html with a browser to view the documentation.

## Authors:
Xiongnan Jin <xiongnan.jin@nist.gov>
<br> Oleg Aulov <oleg.aulov@nist.gov>
<br> Peter Fontana <peter.fontana@nist.gov>

## Copyright:

This software was developed by employees of the National Institute of Standards 
and Technology (NIST), an agency of the Federal Government and is being made
available as a public service. Pursuant to title 17 United States Code Section
105, works of NIST employees are not subject to copyright protection in the
United States.  This software may be subject to foreign copyright.  Permission
in the United States and in foreign countries, to the extent that NIST may hold
copyright, to use, copy, modify, create derivative works, and distribute this 
software and its documentation without fee is hereby granted on a non-exclusive 
basis, provided that this notice and disclaimer of warranty appears in all
copies. 

THE SOFTWARE IS PROVIDED 'AS IS' WITHOUT ANY WARRANTY OF ANY KIND, EITHER
EXPRESSED, IMPLIED, OR STATUTORY, INCLUDING, BUT NOT LIMITED TO, ANY WARRANTY
THAT THE SOFTWARE WILL CONFORM TO SPECIFICATIONS, ANY IMPLIED WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND FREEDOM FROM
INFRINGEMENT, AND ANY WARRANTY THAT THE DOCUMENTATION WILL CONFORM TO THE
SOFTWARE, OR ANY WARRANTY THAT THE SOFTWARE WILL BE ERROR FREE.  IN NO EVENT
SHALL NIST BE LIABLE FOR ANY DAMAGES, INCLUDING, BUT NOT LIMITED TO, DIRECT,
INDIRECT, SPECIAL OR CONSEQUENTIAL DAMAGES, ARISING OUT OF, RESULTING FROM, OR 
IN ANY WAY CONNECTED WITH THIS SOFTWARE, WHETHER OR NOT BASED UPON WARRANTY,
CONTRACT, TORT, OR OTHERWISE, WHETHER OR NOT INJURY WAS SUSTAINED BY PERSONS OR
PROPERTY OR OTHERWISE, AND WHETHER OR NOT LOSS WAS SUSTAINED FROM, OR AROSE OUT
OF THE RESULTS OF, OR USE OF, THE SOFTWARE OR SERVICES PROVIDED HEREUNDER.

## Disclaimer

Certain commercial entities, equipment, or materials may be identified in this document in order to describe an experimental
procedure or concept adequately. Such identification is not intended to imply recommendation or endorsement by the National
Institute of Standards and Technology, nor is it intended to imply that the entities, materials, or equipment mentioned are
necessarily the best available for the purpose. All copyrights and trademarks are properties of their respective owners.