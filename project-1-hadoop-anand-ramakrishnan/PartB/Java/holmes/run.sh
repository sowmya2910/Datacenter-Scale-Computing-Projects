#! /bin/bash

rm -r -f output
hadoop fs -rm -r -f input/
hadoop fs -rm -r -f outputWordCount/
hadoop fs -rm -r -f output/
hadoop fs -copyFromLocal input/
hadoop jar ../wordcount.jar WordCount input/ outputWordCount/
hadoop jar ../topfrequency.jar TopKValues outputWordCount/ output/
hadoop fs -copyToLocal output/ 
