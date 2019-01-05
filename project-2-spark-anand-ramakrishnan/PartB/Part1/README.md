
This part was thought and written by Archana Anand. 

Logic:

First I took the review table and grouped the table by 'asin' at the same time taking mean of overall and count of asin entries for similarly grouped asin using 'agg' function

Once I got the count, I filtered the dataframe above by count > 99

Then I took inner join with metadata table on 'asin'

For sorting on 'overall' I took order by. 

Took the distinct Item's highest element from each categories and then take the highest element from any category not already taken
