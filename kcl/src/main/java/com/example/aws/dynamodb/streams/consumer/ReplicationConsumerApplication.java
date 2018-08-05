package com.example.aws.dynamodb.streams.consumer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.example.aws.Util;

/**
 * Sample consumer application for Amazon DynamoDB Streams to replicate records.
 */
public final class ReplicationConsumerApplication {

	private static final String APPLICATION_NAME = System.getProperty("app.name",
			"access-log-dynamodbstreams-replication-consumer-application");
	private static final String SRC_TABLE_NAME = System.getProperty("source.table.name", "access-log");
	private static final String DEST_TABLE_NAME = System.getProperty("dest.table.name", "access-log-replica");
	private static final String REGION = System.getProperty("region", "ap-northeast-1");

	// Initial position in the stream when the application starts up for the first time.
	// Position can be one of LATEST (most recent data) or TRIM_HORIZON (oldest available data)
	private static final InitialPositionInStream INITIAL_POSITION_IN_STREAM = InitialPositionInStream.LATEST;

	public static void main(String[] args) throws UnknownHostException {
		// Set AWS credentials
		AWSCredentialsProvider credentialsProvider = Util.initCredentialsProvider();

		// * Original code for DynamoDB Streams * //
		AmazonDynamoDBStreamsAdapterClient adapterClient = new AmazonDynamoDBStreamsAdapterClient(credentialsProvider,
				new ClientConfiguration());
		adapterClient.setRegion(Region.getRegion(Regions.fromName(REGION)));
		AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard().withRegion(REGION).build();
		String streamArn = dynamoDBClient.describeTable(SRC_TABLE_NAME).getTable().getLatestStreamArn();

		// Set KCL configuration
		String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
		KinesisClientLibConfiguration kclConfiguration = new KinesisClientLibConfiguration(APPLICATION_NAME, streamArn, credentialsProvider,
				workerId);
		kclConfiguration.withInitialPositionInStream(INITIAL_POSITION_IN_STREAM);

		// Start workers
		IRecordProcessorFactory recordProcessorFactory = new ReplicationConsumerFactory(DEST_TABLE_NAME);
		AmazonCloudWatch cloudWatchClient = AmazonCloudWatchClientBuilder.standard().withRegion(REGION).build();
		@SuppressWarnings("deprecation")
		Worker worker = new Worker(recordProcessorFactory, kclConfiguration, adapterClient, dynamoDBClient, cloudWatchClient);
		try {
			System.out.printf("Running %s to process stream %s as worker %s...\n", APPLICATION_NAME, streamArn, workerId);
			worker.run();
		} catch (Throwable t) {
			System.err.println("Caught throwable while processing data.");
			t.printStackTrace();
			System.exit(1);
		}
	}
}
