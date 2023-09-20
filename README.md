# This repo contains tasks that worked with MapReduce.

# WordCount.java   WordCountImproved.java

WordCount.java and WordCountImproved.java are basically doing the same thing which calculate the occurance of each distinct word.

WordCountImproved.java updates the mapper so it produces (Text, LongWritable) pairs, so we can work with 64-bit integers.
Also, the improved version removes inner class IntSumReducer and use the pre-made LongSumReducer instead.

# RedditAverage.java

This file demonstrate how MapReduce can work with JSON object properly. The input to our mapper will be lines (from TextInputFormat) of JSON-encoded data. 
In the mapper, we will need to parse the JSON into actual data we can work with.

`LongPairWritable.java` is a supplementary class which takes two Long values as the pair.
The reducer takes the mapper's key/value output (Text , LongPairWritable) and calculates the average for each subreddit. 
The combiner shuffle pairs with the same key values to improve efficiency. 
For example, 
`canada  (1,1)
canada  (1,9)
canada  (1,8)
canada  (1,1)`

The combiner will output `canada  (4,19)`


