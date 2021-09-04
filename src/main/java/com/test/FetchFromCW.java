package com.test;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.joda.time.DateTime;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * AWS log messages are read from CloudWatch Log streams and batches are sent to S3 buckets.
 * The log event messages are kept in S3 in the following pattern:
 *
 * <I>targetS3BucketName/streamName/timeOfLamdbaInvocation/batch</I>
 * <p>
 * prev-timestamp.txt is in <I>targetS3BucketName/streamName
 */
public class FetchFromCW implements RequestHandler<Map<String, String>, String> {
  public static final String DATE_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.sss";
  Gson gson = new GsonBuilder().setPrettyPrinting().create();
  AmazonS3 s3Client;
  AWSLogs logsClient;
  DynamoDB dynamoDB;

  @Override
  public String handleRequest(Map<String, String> event, Context context) {
    Util.logEnvironment(event, context, gson);

    final LambdaLogger logger = context.getLogger();
    if (!initClients(logger)) {
      return "503 Service Unavailable";
    }

    int size = Integer.valueOf(event.get("size")).intValue();
    logger.log("size: " + size + "\n");
    String logGroupName = event.get("logGroupName");
    logger.log("logGroupName: " + logGroupName + "\n");
    String targetS3BucketName = event.get("targetS3BucketName");
    logger.log("targetS3BucketName: " + targetS3BucketName + "\n");
    String targetDynamoDBTable = event.get("targetDynamoDBTable");
    logger.log("targetDynamoDBTable: " + targetDynamoDBTable + "\n");

    DescribeLogStreamsRequest describeLogStreamsRequest = new DescribeLogStreamsRequest()
        .withLogGroupName(logGroupName)
        .withDescending(true)
        .withOrderBy(OrderBy.LastEventTime);
    logger.log(describeLogStreamsRequest.toString() + "\n");

    DescribeLogStreamsResult describeLogStreamsResult = null;
    List<LogStream> logStreams = new ArrayList<>();

    do {
      if (describeLogStreamsResult != null) {
        describeLogStreamsRequest.setNextToken(describeLogStreamsResult.getNextToken());
      }
      describeLogStreamsResult = logsClient.describeLogStreams(describeLogStreamsRequest);
      logStreams.addAll(describeLogStreamsResult.getLogStreams());
    }
    while (describeLogStreamsResult.getNextToken() != null &&
        !describeLogStreamsResult.getNextToken().isEmpty());

    logger.log("logStreams count: " + logStreams.size() + "\n");
    for (LogStream logStream : logStreams) {
      String logStreamName = logStream.getLogStreamName();
      logger.log(logStreamName + "\n");
      // Get all the events in the stream since last invocation
      long prevTimestamp;
      try {
        prevTimestamp = getPrevTimestamp(targetS3BucketName, logStreamName);
        logger.log("End timestamp from previous invocation is " + prevTimestamp + "\n");
      }
      catch (Exception e) {
        // If it's not found in S3, default to 0
        prevTimestamp = 0;
      }
      DateTime startTime = new DateTime(prevTimestamp);
      logger.log("Fetching at most " + size + " events newer than " + startTime.toString(DATE_PATTERN) + "\n");
      GetLogEventsRequest getLogEventsRequest = new GetLogEventsRequest()
          .withLogGroupName(logGroupName)
          .withStartTime(prevTimestamp) // Earlier than this time are *not* included
          .withLimit(size) // Fetch up to <size>
          .withStartFromHead(true)
          .withLogStreamName(logStreamName);

      List<OutputLogEvent> logEventsInStream = logsClient.getLogEvents(getLogEventsRequest).getEvents();
      int eventCount = logEventsInStream.size();
      logger.log("There are " + eventCount + " events in the stream\n");
      if (eventCount == 0) {
        return "200 OK";
      }

      BatchMetaInf metaInf = new BatchMetaInf();
      metaInf.setCount(eventCount);
      metaInf.setStart(logEventsInStream.get(0).getTimestamp());
      metaInf.setEnd(logEventsInStream.get(eventCount - 1).getTimestamp());

      // Prepare the batch
      StringBuffer batch = new StringBuffer();
      int totalSize = 0;
      for (OutputLogEvent logEvent : logEventsInStream) {
        StringBuilder sb = new StringBuilder();
        long timestamp = logEvent.getTimestamp().longValue();
        DateTime dateTime = new DateTime(timestamp);
        String message = logEvent.getMessage();
        sb.append(dateTime.toString(DATE_PATTERN)).append(": ").append(message);
        logger.log(sb + "\n");
        // Append to the total batch
        batch.append(sb + "\n");
        totalSize += logEvent.getMessage().length();
      }
      metaInf.setSize(totalSize);
      if (!batch.toString().isEmpty()) {
        // Batch is ready
        try {
          DateTime now = new DateTime(); // Invocation time
          String bucketName = targetS3BucketName + "/" + logStreamName + "/" + now.toString(DATE_PATTERN);
          // Send the batch to the S3 bucket
          String batchKey = "Batch-" + new DateTime(metaInf.getStart()).toString(DATE_PATTERN);
          s3Client.putObject(bucketName, batchKey, batch.toString());
          // Send the meta information to the S3 bucket
          s3Client.putObject(bucketName, "meta-inf.json", gson.toJson(metaInf));
          // Update prev-timestamp.txt
          logger.log("Updating prev-timestamp from " + prevTimestamp + " to " + (metaInf.getEnd() + 1) + "\n");
          s3Client.putObject(targetS3BucketName + "/" + logStreamName, "prev-timestamp.txt", String.valueOf(metaInf.getEnd() + 1));
          // Update DynamoDB with the meta information
          logger.log("Inserting " + metaInf.toString() + " to DynamoDB\n");
          PutItemOutcome outcome = insertBatchMetaInfToDynamoDb(targetDynamoDBTable, metaInf);
          logger.log("Done inserting to DynamoDb: " + outcome.toString() + "\n");
        }
        catch (SdkClientException e) {
          logger.log("AWS exception: " + e.getMessage() + "\n\n");
          return "503 Service Unavailable";
        }
      }
    } // Next stream
    logger.log("Done!\n");
    return "200 OK";
  }

  private boolean initClients(LambdaLogger logger) {
    final String awsRegion = System.getenv("AWS_REGION");
    // Prepare the client to access S3
    s3Client = AmazonS3ClientBuilder.standard().withRegion(awsRegion).build();
    if (s3Client == null) {
      logger.log("Cannot initiate the AWS S3 client\n");
      return false;
    }
    // Access AWS Log
    logger.log("Creating AWS Log client in region " + awsRegion + "\n");
    try {
      AWSLogsClientBuilder builder = AWSLogsClientBuilder.standard();
      logsClient = builder.withRegion(awsRegion).build();
    }
    catch (Exception e) {
      logger.log("Cannot initiate the AWS Logs client: " + e.getMessage() + "\n\n");
      return false;
    }
    logger.log("AWS Log client created OK\n");
    System.setProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true");
    System.setProperty("com.amazonaws.services.s3.disablePutObjectMD5Validation", "true");

    // Access DynamoDB
    AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard().build();
    dynamoDB = new DynamoDB(dynamoDBClient);
    logger.log("DynamoDB client created OK\n");


    return true;
  }

  private long getPrevTimestamp(String targetS3BucketName, String logStreamName) throws IOException {
    S3Object prevTimestampObject = s3Client.getObject(targetS3BucketName + "/" + logStreamName, "prev-timestamp.txt");
    S3ObjectInputStream inputStream = prevTimestampObject.getObjectContent();
    BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
    String line = br.readLine();
    return Long.valueOf(line).longValue();
  }

  private PutItemOutcome insertBatchMetaInfToDynamoDb(String tableName, BatchMetaInf metaInf) {
    Table table = dynamoDB.getTable(tableName);


    Item item = new Item()
        .withPrimaryKey("end", metaInf.getEnd() / 1000)
        .withNumber("start", metaInf.getStart() / 1000)
        .withNumber("count", metaInf.getCount())
        .withNumber("size", metaInf.getSize());
    // Write the item to the table
    PutItemOutcome outcome = table.putItem(item);
    return outcome;
  }
}
