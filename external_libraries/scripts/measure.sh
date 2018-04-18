#!/bin/bash

DATASETS=/home/joon/dana.software/external_libraries/datasets/liblinear
LibLinearMulticore=/home/joon/dana.software/external_libraries/liblinear-multicore-2.20
DIMMWitted=/home/joon/dana.software/external_libraries/dimmwitted
LibLinear=/home/joon/dana.software/external_libraries/liblinear-2.20
PGExport=/home/joon/dana.software/external_libraries/datasets/pgexport

threads=(1 2 4 8 16)
repeat=1

if [ -z $1 ]; then
    echo [Error] Must specify liblinear or dimm
    exit
fi

# # Data Export
# TABLES=(logistic_387944_2000 svm_678392_1740)
# for table in ${TABLES[@]};
# do
#     echo Table: $table
#     runtime_acc=0
#     for (( i=0; i<${repeat}; i++));
#     do
# 	#echo repeat $i...
# 	psql -U postgres -d benchmarks -c '\timing' -c "copy ${table} to '/home/postgres/${table}.csv' DELIMITER ',' CSV HEADER;" | tail -1 | grep -o -E '[0-9]*\.[0-9]+' > export_out
# 	export_time=$(tail -1 export_out)
# 	#echo "duration (ms): ${export_time}"
# 	runtime_acc=$(awk "BEGIN {print $runtime_acc + $export_time; exit}")
#     done
#     #average=$((runtime_acc / repeat))
#     average=$(awk "BEGIN {print $runtime_acc / $repeat; exit}")
#     echo average: ${average}
# done
# exit

# # Data Transform
# #FILES=(logistic-svm-realdataset_pgexport.csv logistic-realdataset2_pgexport.csv logistic-svm-realdataset_pgexport.csv)
# FILES=(logistic_387944_2000_pgexport.csv svm_67832_1740_pgexport.csv)
# for file in ${FILES[@]};
# do
#     echo Transforming pgexport file: ${file}...
#     runtime_acc=0
#     for (( i=0; i<${repeat}; i++));
#     do
# 	./data_transform_liblinear.py ${PGExport}/$file > transform_out
# 	export_time=$(tail -1 transform_out)
# 	#echo "duration (ms): ${export_time}"
# 	runtime_acc=$(awk "BEGIN {print $runtime_acc + $export_time; exit}")
#     done
#     #average=$((runtime_acc / repeat))
#     average=$(awk "BEGIN {print $runtime_acc / $repeat; exit}")
#     echo average: ${average}
# done

# for liblinear
#file=(logistic-svm-realdataset_pgexport_liblinear_output logistic-realdataset2_pgexport_liblinear_output logistic-svm-realdataset_pgexport_liblinear_output logistic_387944_2000_pgexport_liblinear_output svm_67832_1740_pgexport_liblinear_output)
file=(logistic_387944_2000_pgexport_liblinear_output)
for trainfile in ${file[@]}
#for trainfile in $(ls $DATASETS);
do
    echo Datafile: ${trainfile}
    for thread in ${threads[@]};
    do
	echo num threads: ${thread}
	runtime_acc=0
	# just echoing command to be run
	echo ${LibLinearMulticore}/train -s 0 -e 1 -n ${thread} ${DATASETS}/${trainfile}
	echo average over ${repeat} runs
	for (( i=0; i<${repeat}; i++));
	do
	    echo repeat $i...
	    #${LibLinearMulticore}/train -s 0 -e 0.0001 -n ${thread} ${DATASETS}/${trainfile} > out

	    # for liblinear
	    ${LibLinearMulticore}/train -s 0 -e 1 -n ${thread} ${DATASETS}/${trainfile} > out
	    
	    lastline=$(tail -1 out)
	    echo "duration (us): ${lastline}"
	    ((runtime_acc += ${lastline}))
	    #runtimes+=($(${LibLinear}/train -s 0 -e 0.0001 ${DATASETS}/${trainfile} | tail -1))
	done
	# echo printing runtimes...
	# printf '%s\n' "${runtimes[@]}"
	average=$((runtime_acc / repeat))
	echo average: ${average}
	echo
    done
    echo 
done

# for dimmwitted
# file=(logistic-svm-realdataset_pgexport_liblinear_output logistic-realdataset2_pgexport_liblinear_output logistic-svm-realdataset_pgexport_liblinear_output logistic_387944_2000_pgexport_liblinear_output svm_67832_1740_pgexport_liblinear_output)
# for trainfile in ${file[@]}
# #for trainfile in $(ls $DATASETS);
# do
#     echo Datafile: ${trainfile}...
#     runtime_acc=0
#     # just echoing command to be run
#     echo average over ${repeat} runs
#     for (( i=0; i<${repeat}; i++));
#     do
# 	echo repeat $i...
# 	#${LibLinearMulticore}/train -s 0 -e 0.0001 -n ${thread} ${DATASETS}/${trainfile} > out

# 	# for liblinear
# 	#${LibLinearMulticore}/train -s 2 -n ${thread} ${DATASETS}/${trainfile} > out
# 	# for DIMMWitted
# 	echo ${DIMMWitted}/dw-lr-train -s 0.01 -e 1 -r 0 ${DATASETS}/${trainfile}
# 	${DIMMWitted}/dw-lr-train -s 0.01 -e 1 -r 0 ${DATASETS}/${trainfile} > out
	
# 	lastline=$(tail -1 out)
# 	echo "duration (us): ${lastline}"
# 	runtime_acc=$(awk "BEGIN {print $runtime_acc + $lastline; exit}")
# 	#((runtime_acc += ${lastline}))
#     done
# # 	runtime_acc=$(awk "BEGIN {print $runtime_acc + $export_time; exit}")
# #     done
# #     #average=$((runtime_acc / repeat))
# #     average=$(awk "BEGIN {print $runtime_acc / $repeat; exit}")
# #     echo average: ${average}
    
#     # echo printing runtimes...
#     # printf '%s\n' "${runtimes[@]}"
#     average=$(awk "BEGIN {print $runtime_acc / $repeat; exit}")
#     #average=$((runtime_acc / repeat))
#     echo average: ${average}
#     echo
#     echo 
# done

