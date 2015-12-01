# Twitter-Map2

This is the second assignment of Cloud Computing. In this assignment, we incorporate SNS and SQS.
Basically, the tweetGet.java take Twitter posts from Twitter API stream and stored them in DynamoDB directly. As they are being stored in the database, the program also starts pushing messages into SQS. At the same time, 5 independent workers are started to read from SQS. As theworkders reading, they invoke thirdparty API, Alchemy, to analyze sentiment of messages, which gives positive, negative, neutral as the result. Having gotten these results, the workers push and subscribe the messages to one of the server's endpoint which is 9063 in our case. In this case, the server will receive the sentiment result and display on the heatmap in different color based on their sentiment. Positive: Green to Red. Negative: Blue

