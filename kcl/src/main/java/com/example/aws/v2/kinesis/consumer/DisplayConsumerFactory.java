package com.example.aws.v2.kinesis.consumer;

import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

/**
 * Used to create new consumers.
 */
public class DisplayConsumerFactory implements ShardRecordProcessorFactory {
	public ShardRecordProcessor shardRecordProcessor() {
		return new DisplayConsumer();
	}
}
