package com.example.aws.kinesis.consumer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.example.aws.Util;

/**
 * Sample consumer application for Amazon Kinesis Streams to display records.
 */
public final class DisplayConsumerApplication {

	private static final String APPLICATION_NAME = System.getProperty("app.name", "access-log-stream-display-consumer-application");
	private static final String STREAM_NAME = System.getProperty("stream.name", "access-log-stream");
	private static final String REGION = System.getProperty("region", "ap-northeast-1");

	// Initial position in the stream when the application starts up for the first time.
	// Position can be one of LATEST (most recent data) or TRIM_HORIZON (oldest available data)
	private static final InitialPositionInStream INITIAL_POSITION_IN_STREAM = InitialPositionInStream.LATEST;

	/**
	 * Main method to start workers
	 * 
	 * @param args
	 * @throws UnknownHostException
	 */
	public static void main(String[] args) throws UnknownHostException {
		// Set AWS credentials
		AWSCredentialsProvider credentialsProvider = Util.initCredentialsProvider();

		// Set KCL configuration
		String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
		KinesisClientLibConfiguration kclConfiguration = new KinesisClientLibConfiguration(APPLICATION_NAME, STREAM_NAME, credentialsProvider, workerId);
		kclConfiguration.withRegionName(REGION);
		kclConfiguration.withInitialPositionInStream(INITIAL_POSITION_IN_STREAM);

		// Start workers
		IRecordProcessorFactory recordProcessorFactory = new DisplayConsumerFactory();
		IMetricsFactory metricsFactory = new NullMetricsFactory();
		Worker worker = new Worker(recordProcessorFactory, kclConfiguration, metricsFactory);
		try {
			System.out.printf("Running %s to process stream %s as worker %s...\n", APPLICATION_NAME, STREAM_NAME, workerId);
			worker.run();
		} catch (Throwable t) {
			System.err.println("Caught throwable while processing data.");
			t.printStackTrace();
			System.exit(1);
		}
	}
}
