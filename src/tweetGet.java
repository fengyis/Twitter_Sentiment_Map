import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.List;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import org.json.JSONObject;

import com.mashape.unirest.http.*;

public class tweetGet {
	static int count = 1;
	static String myQueueUrl;
	static int numberOfWorkerPools = 5;
	
	public static void main(String[] args) throws IOException {
		//test
		AWSCredentials credentials = new PropertiesCredentials(
				tweetGet.class.getResourceAsStream("AwsCredentials.properties"));
		//AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
		
		DynamoDB dynamoDB = DynamoDBHandler.createDynamoDB(credentials);
		String tableName = "Assignment1";
		DynamoDBHandler.createTable(dynamoDB, tableName);

		Table table = dynamoDB.getTable(tableName);
		
		int startFlag = 1;
		Item item = table.getItem("Id", startFlag);
		while (item != null) {
			startFlag++;
			item = table.getItem("Id", startFlag);
		}
		count = startFlag;
		
		AmazonSQS sqs = new AmazonSQSClient(credentials);
		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		sqs.setRegion(usEast1);
		
		// Create a client
		AmazonSNSClient sns = new AmazonSNSClient(credentials);

		// Create a topic
		CreateTopicRequest snsCreateReq = new CreateTopicRequest()
			.withName("test1");
		CreateTopicResult snsCreateRes = sns.createTopic(snsCreateReq);
		
		try {
			
			// Create a queue
			System.out.println("Creating a new SQS queue called MyQueue.\n");
			CreateQueueRequest createQueueRequest = new CreateQueueRequest("MyQueue");
			myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
			
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it " +
					"to Amazon SQS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered " +
					"a serious internal problem while trying to communicate with SQS, such as not " +
					"being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
		
		ExecutorService executor = Executors.newFixedThreadPool(numberOfWorkerPools);
		for (int i = 0; i < numberOfWorkerPools; i++) {
			Runnable worker = new WorkerThread(" " + i, myQueueUrl, sqs, sns, snsCreateRes);
			executor.execute(worker);
		}
		//executor.shutdown();
		//while (!executor.isTerminated()){}
		System.out.println("Finished all threads");
	
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
			.setOAuthConsumerKey("NRJRPz0CRVcG3OW3nG2wtLFHQ")
			.setOAuthConsumerSecret("B1WKd0oZcFu7BuPCMbFuKaNLFmxc0SId0s8n3HYjsp6Fod4vSr")
			.setOAuthAccessToken("2789071561-qkSFovr7RWyW9p53O6hF5sIGceqVGgd4coaEtAh")
			.setOAuthAccessTokenSecret("TOq43alGBDHXw7NxAAhY9ou0a602HNfumA8woVqplQzYQ");
		 
		TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		StatusListener listener = new StatusListener() {
			@Override
			public void onStatus(Status status) {
				if (status.getGeoLocation() != null) {
					
					Date date = new Date();
									
					Item item = new Item()
						.withPrimaryKey("Id", count)
						.withString("UserName", status.getUser().getScreenName())
						.withDouble("Latitude", status.getGeoLocation().getLatitude())
						.withDouble("Longitude", status.getGeoLocation().getLongitude())
						.withString("CreatedTime", date.toString())
						.withLong("Milliseconds", date.getTime())
						.withString("Text", status.getText());
					table.putItem(item);
					try {
						if (status.getLang().equals("en") || status.getLang().equals("fr") || status.getLang().equals("de") || status.getLang().equals("it") || status.getLang().equals("es") || status.getLang().equals("ru")) {
							//System.out.println(status.getGeoLocation().getLatitude() + status.getText());
							sqs.sendMessage(new SendMessageRequest(myQueueUrl, status.getGeoLocation().getLatitude() + " " + status.getGeoLocation().getLongitude() + " " + status.getText()));
						}
					} catch (Exception e) {
						System.err.println(e);
					}
					count++;
					System.out.println(count);
				}
			}
	
			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				//System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
			}
	
			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				//System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
			}
	
			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				//System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
			}
	
			@Override
			public void onStallWarning(StallWarning warning) {
				//System.out.println("Got stall warning:" + warning);
			}
	
			@Override
			public void onException(Exception ex) {
				//ex.printStackTrace();
			}
		};
		twitterStream.addListener(listener);
		twitterStream.sample();
	}
	
	static class WorkerThread implements Runnable{
		private String command;
		private String myQueueUrl;
		private AmazonSQS sqs;
		private AmazonSNSClient sns;
		private CreateTopicResult snsCreateRes;
		

		public WorkerThread(String s,String myQueueUrl,AmazonSQS sqs, AmazonSNSClient sns, CreateTopicResult snsCreateRes){
			this.command = s;
			this.myQueueUrl = myQueueUrl;
			this.sqs = sqs;
			this.sns = sns;
			this.snsCreateRes = snsCreateRes;
		}
		
		public void run(){
			System.out.println(Thread.currentThread().getName()+" Start. Command = "+command);
			//processCommand();
			try {
				while (true) {
					sqsGet();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			System.out.println(Thread.currentThread().getName()+" End.");
		}
		
		void sqsGet() throws Exception{
			
			try {
				// Receive messages
				//	String body = null;
				//System.out.println("Receiving messages from MyQueue.\n");
				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
				List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
				for (Message message : messages) {
//					body = message.getBody();
//					System.out.println("  Message");
//					System.out.println("    MessageId:     " + message.getMessageId());
//					System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
//					System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
					System.out.println("    Body:          " + message.getBody());
//					for (Entry<String, String> entry : message.getAttributes().entrySet()) {
//						System.out.println("  Attribute");
//						System.out.println("    Name:  " + entry.getKey());
//						System.out.println("    Value: " + entry.getValue());
//		       	 	}
				
					sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, message.getReceiptHandle()));
				 
					String[] parts = message.getBody().split(" ", 3);
				
					double latitude = Double.parseDouble(parts[0]);
					double longitude = Double.parseDouble(parts[1]);
					String[] textParts = parts[2].split(" ");
				
					String tweetText = null;
					for (int i = 0; i < textParts.length; i++){
						if (i == 0) {
							tweetText = textParts[i];
							continue;
						}
						tweetText = tweetText + "+" + textParts[i];
					}
					System.out.println("********"+tweetText+"*********");
				
					HttpResponse<JsonNode> response = Unirest.get("https://alchemy.p.mashape.com/text/TextGetTextSentiment?outputMode=json&showSourceText=false&text="+tweetText)
						.header("X-Mashape-Key", "4DDVDdPIizmshUk5yhc7nU3SPBopp1O09imjsnxQTVLXF0m6Bp")
						.header("Accept", "text/plain")
						.asJson();
					//"asvtS4OvhGmsh7GMI7uDnFTXQ5vop1FMS2njsn9az6XYqaRjNi"
				
					JSONObject myObj = response.getBody().getObject();
					System.out.println("*****myObj*****"+myObj.toString()+"******myObj****");
					JSONObject doc = (JSONObject) myObj.get("docSentiment");
					String sentiment = doc.get("type").toString();
					System.out.println("*****sentiment*****"+sentiment+"******sentiment****");
				
				
					// Publish to a topic
					PublishRequest publishReq = new PublishRequest()
							.withTopicArn(snsCreateRes.getTopicArn())
							.withMessage(latitude + " " + longitude + " " + sentiment);
					sns.publish(publishReq);
				
					System.out.println("Finish a cycle.");				
				}
				
			 
			} catch (Exception e) {
				//e.printStackTrace();
			}
		}
	}
}