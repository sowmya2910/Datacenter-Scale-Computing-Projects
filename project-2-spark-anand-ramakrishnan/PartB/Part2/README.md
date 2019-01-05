
This part was thought and written by Archana Anand with efforts and contribution from Sowmya Ramakrishnan.

Logic:

Firstly, the reviews table is taken and split into 5 dataframes using the filter API, based on the 'overall' (rating) value which ranges from 1 to 5. 

The reviewText field is taken as data in each dataframe using the Select API.

A new RDD is taken for each dataframe after checking for alphabets and spaces, and converting all text to lowercase.

The RDD is then sent to flatMap function which does pre-processing on data and removes stop-words, if present.

The top 500 words is then taken for each rating, parallelized and saved as output text files in designated Bucket-x folders. 

