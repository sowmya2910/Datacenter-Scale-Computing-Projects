
Logic:

The code reads the input text file using spark context and sends the rdd to flatmap function Func.

Func does all the preprocessing on data (remove special characters, convert to lower, split etc) and returns to the flatmap.

The map then takes these split lines and forms the tuple of word,1.

reduceByKey matches similar keys and adds its value (1 in our case).

We then get sorted by value by using TakeOrdered function which returns top 2000 entries.

