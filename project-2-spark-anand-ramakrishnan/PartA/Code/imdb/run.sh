#! /bin/bash

rm -r -f output
hadoop fs -rm -r -f input/
hadoop fs -rm -r -f output/
hadoop fs -copyFromLocal input/
spark-submit --deploy-mode cluster wordcount.py
hadoop fs -copyToLocal output/ 
