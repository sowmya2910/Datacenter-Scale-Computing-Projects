#! /bin/bash

rm -r -f output
hadoop fs -rm -r -f input/
hadoop fs -rm -r -f output/
hadoop fs -copyFromLocal input/
hadoop jar WordCount/wordcount.jar WordCount input/ output/
hadoop fs -copyToLocal output/ 
