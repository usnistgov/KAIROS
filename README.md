# README.md

The KAIROS Evaluation Software (kevs) is NIST's software suite to evaluate 
KAIROS TA1 and TA2 Systems.

## Installing and Uninstalling the software

This package was tested in python 3.7.6.

The package is pip installable. One can install it with

```bash
pip install .
```

This command will install the package _kevs_. See requirements.txt for dependent modules. 
Having _pytest_, _coverage_, _flake8_, and _sphinx_ are required to run the testing,
lint checking, and documentation. This software has been tested on python 3.7.6.

One can uninstall the package with

```bash
pip uninstall -y kevs
```

For KAIROS Participants only, outside of this repository is a sample test input directory and
test output directory. LDC annotations go into the `input_subdir/eval_phase_subdir` 
directory `input_subdir/eval_phase_subdir`/`data/Annotation/Task1`. Edited versions of the annotaiton for Graph G Generation
are in `input_subdir/eval_phase_subdir`/`data/Annotation/Task1` TA1 SDF Files are placed in `input_subdir/eval_phase_subdir`/`TA1_Submissions`
in a folder where the folder is the name of the TA1 Library. Similarly, TA2 Task 1 SDF Submissions are placed in
`input_subdir/eval_phase_subdir`/`TA2_Task1_Submissions` with one folder per TA2 System. The TA2 Task 2 SDF Submissions are placed in
`input_subdir/eval_phase_subdir`/`TA2_Task1_Submissions` with one folder per TA2 System.

All scorer output will be placed in subdirectories within `output_subdir`. Those directories
will be created by the scorer as necessary.

Also, each scorer execution may delete files created in previous scoring runs, therefore it
is recommended to copy any desired output to a different location.

The statistics in the files TA1_stats.xlsx and TA2_stats.xlsx are documented in the file
`code_stats_header_documentation.xlsx` in this repository.

## Running the Scripts

Update the `config.ini` file to point to the directories for the scorer inputs and outputs.
In particular, please change the `root_dir` to the proper directory in all of the modes.
The remaining folders can remain as default values or changed if desired `input_subdir`, `output_subdir`, `eval_phase_subdir`, and `include_all_events`.

After updating the file, go into the `execution_scripts` directory and run

```bash
python run_all.py -f config.ini -m "Default"
```

This command will run the scorer in the "Default" mode for the specified Configuration File. The "Default" mode is used for scoring the Phase IIa evaluation submissions. There is a separate config mode 
"Test" that uses a different `input_subdir` and `output_subdir` that is used for scorer testing purposes.

The run_all.py command runs all of the functions called in the different py 
files in the `execution_scripts` folder.  The order of execution is specified by the number `yy` in the filename of the script `script_yy_<name>.py`.
Each script is executed can be run separately
with the same arguments as the run_all.py file. For instance,

```bash
python script_02_extract_ta1_and_ta2_sdf.py -f config.ini -m "Default"
```

runs the script to convert the TA2 SDF output into the NIST's intermediate table form on which the scoring is later performed. If the outputs are not pre-generated
the execution scripts will need to be run in order.

In order to skip the Graph G generation during the scorer execution, execute the following command

```bash
python run_scoring.py -f config.ini -m "Default"
```

### Description of Each Script

Below is the description of each scoring script. The run_all script runs each of these scripts in turn

```bash
python script_01_generate_graph_g.py -f config.ini -m "Default"
```

This script takes the annotations with the selected events and relations (specified as input)
and produces the Graph G output. The Graph G output is in a TA2 SDF.


```bash
python script_02_extract_ta1_and_ta2_sdf.py -f config.ini -m "Default"
```

This script takes TA1 Libraries and TA2 Instantiations and converts them to NIST's intermediate form,
which is refered to as SDF extractions. These SDF extractions are a collection of spreadsheets, and the
outputs are stored in `TA1_Extractions`, `TA2_Task1_Extractions`, `TA2_Task2_Extractions`,
and `Graph_G_Extractions`. These spreadsheets are used in the follow up stages of the scorer for analysis.

```bash
python script_03_generate_ta1_and_ta2_stats.py -f config.ini -m "Default"
```

This script takes the SDF extraction spreadsheets and produces preliminary statistics on TA1 and TA2 Submissions

```bash
python script_04_score_ke_matches.py -f config.ini -m "Default"
```

This script performs the automated matching and scoring of events. Although designed for Task 2,
this script can also automatically score any valid TA2 output.

```bash
python script_05_score_task1_assessments -f config.ini -m "Default"
```

This script provides the Task 1 Scores using LDC Assessments.

```bash
python script_06_generate_ta1_coverage_stats -f config.ini -m "Default"
```

This script provides various TA1 coverage statistics.

```bash
python script_07_generate_graph_g_stats -f config.ini -m "Default"
```

This script provides various Graph G statistics.

```bash
python script_08_analyze_ta1_and_ta2_extractions.py -f config.ini -m "Default"
```

This script converts the TA2 system output into a human readable "Event Tree" that visualizes the hierarchy of events.



## Running Continuous Integration (CI) Locally 

### Testing

To run the tests and generate a coverage report, execute the commands in the source directory

```bash
coverage run --branch --source=./kevs -m pytest -s tests/ -v
coverage report -m
coverage html
```

To run the tests without the coverage, execute 
```bash
pytest -s tests/
```

## Example CACI Validator Commands

Below are shell commands to launch and run the validator from CACI.

To launch the standalone validator, in a separate window, go to the validation directory
from CACI and run

```bash
docker-compose -f clotho-validation.yml up
```

Then to use the standalone validator, go to the directory of the json file to validate and 
run

```bash
curl -X POST "http://localhost:8008/json-ld/ksf/validate" -H  "accept: application/json" -H  "Content-Type: application/ld+json" --data-binary "@ce2002full_GraphG.json" | json_pp
```

where --data-binary "@<filename>" identifies the json file to be validated. The output is 
then displayed in the json form.

Alternatively, the CACI API can be queried with

```bash
curl -X POST "https://validation.kairos.nextcentury.com/json-ld/ksf/validate" -H  "accept: application/json" -H  "Content-Type: application/ld+json" --data-binary "@graphg-graphg-task2-ce2002.json" | json_pp
```
