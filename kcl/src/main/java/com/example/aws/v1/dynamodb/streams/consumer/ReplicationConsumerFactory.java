package com.example.aws.v1.dynamodb.streams.consumer;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

/**
 * Used to create new record processors.
 */
public class ReplicationConsumerFactory implements IRecordProcessorFactory {
	private String destTableName;

	/**
	 * Constructor using destTableName field
	 * 
	 * @param destTableName
	 */
	public ReplicationConsumerFactory(String destTableName) {
		super();
		this.destTableName = destTableName;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IRecordProcessor createProcessor() {
		AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard().build();
		return new ReplicationConsumer(dynamoDBClient, destTableName);
	}
}
