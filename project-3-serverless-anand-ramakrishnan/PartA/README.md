#### This project gave us an insight into and made us gain hands-on experience with the AWS Lambda Serverless Technology.
#### This project is a combined effort of both of its members - Archana Anand and Sowmya Ramakrishnan. We worked together for Lambda as well as the wordcount part.
#### Python 3.6 is used as the Lambda function programming language.

#### Whenever a tweet text file is placed/uploaded in an S3 Bucket (this is done using the upload.py code already present/available), the corresponding Lambda function is called which performs processing on it and saves the output in a destination S3 Bucket. All unprocessed tweet files are thus processed (non-alphabetic characters, stop-words removed, all letters converted to lowercase) and the destination bucket contains all output text files at the end of the 35 minutes (duration it takes to upload files through code). The filename of the modified tweet is the same as its filename in the input S3 Bucket. Thus, the destination bucket contains 2000 tweet text files named 1.txt - 2000.txt, the difference in the upload timestamps being ~35 minutes/the time it cook to run the upload.py code. A wordcount operation is then performed on all of the 2000 text files in the destination bucket using the AWS EMR Hadoop Framework - giving the wordcount.jar file, input (processedtweetsproject3) and output (proj3wordcount) s3 buckets - to render a single output file containing words sorted alphabetically and their count/frequency.  

#### Buckets have been named as follows:

#### csci5253-fall2018-project3-anand-ramakrishnan : The bucket to which all tweets are uploaded when the upload.py code is run.
#### processedtweetsproject3 : The destination bucket to which all processed tweet text files are uploaded/sent to by the Lambda function.
#### Both of these buckets have been made public to enable checks/tests.

#### The Part A folder contains the following files:

#### output.tar.gz - The archived file containing all of the transformed tweets in the destination S3 Bucket.
#### wordcount.txt - The wordcount file containing words in alphabetical order and their frequency of appearance in the processed tweet files.

#### The Part A/Code folder contains the following files:

#### A README.md containing the nuances of the Lambda function used and details of deployment.
#### lambda_function.py - The lambda code in Python 3.6 used for tweet processing and upload to destination bucket.
#### run.sh - A shell script that contains commands to run wordcount on the tweet files (present in /input directory) and output the result. The permission has been changed to 'a+x' so that it is executable by everybody.
#### A WordCount folder that contains the wordcount.jar file used to perform wordcount operation on the tweet files, and the bin and src folders contain the Java classes and source codes respectively.
