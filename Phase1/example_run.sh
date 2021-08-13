#!/usr/bin/env bash
# task1 evaluation mode
python3 score_task1.py -t cmu
python3 score_task1.py -t ibm -ae false
python3 score_task1.py -t jhu -m evaluation -ae false
python3 score_task1.py -t resin -m evaluation -ae true
# task2 evaluation mode
python3 score_task2.py -t cmu
python3 score_task2.py -t ibm -gge false -t1e false
python3 score_task2.py -t jhu -gge false -t1e false -t2e true
python3 score_task2.py -t resin_primary -gge false -t1e false
python3 score_task2.py -t resin_relaxed_all -gge false -t1e false
python3 score_task2.py -t resin_relaxed_attack -gge false -t1e false
# task2 test mode
python3 score_task2.py -m test -t test
