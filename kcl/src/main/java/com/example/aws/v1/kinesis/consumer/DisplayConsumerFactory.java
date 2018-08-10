package com.example.aws.v1.kinesis.consumer;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

/**
 * Used to create new consumers.
 */
public class DisplayConsumerFactory implements IRecordProcessorFactory {
	/**
	 * {@inheritDoc}
	 */
	@Override
	public IRecordProcessor createProcessor() {
		return new DisplayConsumer();
	}
}
