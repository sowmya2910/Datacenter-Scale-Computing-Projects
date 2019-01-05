As part of this exercise, a WordCount class was written which performs a simple word count operation using Mapper and Reducer functions on the 4 mentioned datasets. These were then tested on a Hadoop EMR Cluster which was accessed through the command line interface. (SSH using Key-pair)

The constraints were fulfilled as mentioned in the exercise - these are taken care of in the Mapper class. All output was converted to lower-case, noise was removed, stopwords were listed using the dictionary class and removed if present from the final output. 

While testing, the IMDB quotes and Sherlock Holmes files were downloaded from the given links and uploaded to HDFS and then provided as an (input) argument to the Hadoop cluster. 
The Project Gutenburg and Yelp datasets were called from their S3 bucket locations, and owing to size constraints, only the top 2000 occurrences were collected. This was done using a TopKValues Class which gets the top 2000 occurrences, sorting their frequencies in descending order. The idea was to multiply the frequency by -1 in mapper and put it as key. Then in reducer take the top 2000 sent by mapper. 

In the PartB directory, 4 sub-directories - gutenburg, imdb, holmes and yelp were created. 
The part-* file obtained as an output from each of the cluster runs for each of these datasets was then archived as a tar.gz  named "output" and uploaded.

Another sub-directory - Java - was created. 
In it, the two .jar files, namely - wordcount.jar and topfrequency.jar used to run the cluster were uploaded, along with the source codes (bin and src folders containing the class and java files respectively). 

A Java project was created, and not a Maven project - hence, there is no pom.xml file present.

In the Java sub-directory, 4 further sub-directories were created - gutenburg, imdb, holmes and yelp - and a shell script was put in each that contains all the commands necessary to import data, copy the files to HDFS, run Hadoop analysis and output the result. The input files were assumed to be present in the Input/ sub-directory within the current directory, one for each inout (except for gutenburg and yelp which were S3 Bucket links). 


