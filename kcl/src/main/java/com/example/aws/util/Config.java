package com.example.aws.util;

public class Config {

	// Common Configuration
	public static final String REGION = System.getProperty("region", "ap-northeast-1");

	// Amazon Kinesis Data Streams Configuration
	public static final String STREAM_NAME = System.getProperty("stream.name", "tokyo-stream-1");
	public static final String KCL_APPLICATION_NAME = System.getProperty("kcl.app.name", STREAM_NAME + "-display-consumer-application");

	// Amazon Kinesis Data Firehose Configuration
	public static final String DELIVERY_STREAM_NAME = System.getProperty("delivery.stream.name", "tokyo-stream-1");

	// Amazon DynamoDB Streams Configuration
	public static final String SRC_TABLE_NAME = System.getProperty("source.table.name", "access-log");
	public static final String DEST_TABLE_NAME = System.getProperty("dest.table.name", "access-log-replica");
	public static final String DDB_APPLICATION_NAME = System.getProperty("kcl.app.name", SRC_TABLE_NAME + "-dynamodbstreams-replication-consumer-application");

	// Producer Configuration
	public static final long RECORD_INTERVAL_MILLIS = Long.parseLong(System.getProperty("record.interval.millis", "1000"));
	public static final int RECORD_COUNT = Integer.parseInt(System.getProperty("record.count", "3"));

}
