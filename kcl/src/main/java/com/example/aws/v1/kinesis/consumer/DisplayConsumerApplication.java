package com.example.aws.v1.kinesis.consumer;

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
import com.example.aws.util.Config;
import com.example.aws.v1.kinesis.Util;

/**
 * Sample consumer application for Amazon Kinesis Streams to display records.
 */
public final class DisplayConsumerApplication {

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
		KinesisClientLibConfiguration kclConfiguration = new KinesisClientLibConfiguration(Config.KCL_APPLICATION_NAME, Config.STREAM_NAME,
				credentialsProvider, workerId);
		kclConfiguration.withRegionName(Config.REGION);
		kclConfiguration.withInitialPositionInStream(INITIAL_POSITION_IN_STREAM);

		// Start workers
		IRecordProcessorFactory recordProcessorFactory = new DisplayConsumerFactory();
		IMetricsFactory metricsFactory = new NullMetricsFactory();
		Worker worker = new Worker.Builder().recordProcessorFactory(recordProcessorFactory).config(kclConfiguration)
				.metricsFactory(metricsFactory).build();
		try {
			System.out.printf("Running %s to process stream %s as worker %s...\n", Config.KCL_APPLICATION_NAME, Config.STREAM_NAME,
					workerId);
			worker.run();
		} catch (Throwable t) {
			System.err.println("Caught throwable while processing data.");
			t.printStackTrace();
			System.exit(1);
		}
	}
}
