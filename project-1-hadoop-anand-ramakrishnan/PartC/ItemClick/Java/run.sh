#! /bin/bash

rm -r -f output
hadoop fs -rm -r -f input/
hadoop fs -rm -r -f output/
hadoop fs -copyFromLocal input/
hadoop jar ItemClick.jar ItemClick input/ output/
hadoop fs -copyToLocal output/ 
