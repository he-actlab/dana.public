# README #

Usage:
./measure.py [ssd | hdd | nvme] [config json file] [postgres | greenplum]


### Directory Structure ###

measure.py: madlib measurement script
scripts: directory that includes helpder scripts for measure.py
config_set1.json, config_set2.json: json files for setting 1 and 2
external libraries: directory that contains 3rd party libraries (liblinear and dmmwitted) and corresponding measurement scripts

### Not included: Dataset files (Too big > 40G) ###
