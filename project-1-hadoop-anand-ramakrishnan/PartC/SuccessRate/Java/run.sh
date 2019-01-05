#! /bin/bash

rm -r -f output
hadoop fs -rm -r -f input/
hadoop fs -rm -r -f output/
hadoop fs -copyFromLocal input/
hadoop jar SuccessRate.jar SuccessRate input/ output/
hadoop fs -copyToLocal output/ output/
