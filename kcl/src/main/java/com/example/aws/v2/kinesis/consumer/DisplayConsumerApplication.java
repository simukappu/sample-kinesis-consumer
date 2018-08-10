package com.example.aws.v2.kinesis.consumer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.aws.util.Config;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.Scheduler;

public class DisplayConsumerApplication {

	private static final Logger LOG = LoggerFactory.getLogger(DisplayConsumerApplication.class);

	/**
	 * Main method to start workers
	 * 
	 * @param args
	 */
	public static void main(String... args) {

		KinesisAsyncClient kinesisClient = KinesisAsyncClient.builder().credentialsProvider(ProfileCredentialsProvider.create())
				.region(Region.of(Config.REGION)).build();
		DynamoDbAsyncClient dynamoClient = DynamoDbAsyncClient.builder().credentialsProvider(ProfileCredentialsProvider.create())
				.region(Region.of(Config.REGION)).build();
		CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder().credentialsProvider(ProfileCredentialsProvider.create())
				.region(Region.of(Config.REGION)).build();
		ConfigsBuilder configsBuilder = new ConfigsBuilder(Config.STREAM_NAME, Config.KCL_APPLICATION_NAME, kinesisClient, dynamoClient,
				cloudWatchClient, UUID.randomUUID().toString(), new DisplayConsumerFactory());

		Scheduler scheduler = new Scheduler(configsBuilder.checkpointConfig(), configsBuilder.coordinatorConfig(),
				configsBuilder.leaseManagementConfig(), configsBuilder.lifecycleConfig(), configsBuilder.metricsConfig(),
				configsBuilder.processorConfig(), configsBuilder.retrievalConfig());

		Thread schedulerThread = new Thread(scheduler);
		schedulerThread.setDaemon(true);
		schedulerThread.start();

		System.out.println("Press enter to shutdown");
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		try {
			reader.readLine();
		} catch (IOException ioex) {
			LOG.error("Caught exception while waiting for confirm.  Shutting down", ioex);
		}

		Future<Boolean> gracefulShutdownFuture = scheduler.startGracefulShutdown();
		LOG.info("Waiting up to 20 seconds for shutdown to complete.");
		try {
			gracefulShutdownFuture.get(20, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			LOG.info("Interrupted while waiting for graceful shutdown. Continuing.");
		} catch (ExecutionException e) {
			LOG.error("Exception while executing graceful shutdown.", e);
		} catch (TimeoutException e) {
			LOG.error("Timeout while waiting for shutdown.  Scheduler may not have exited.");
		}
		LOG.info("Completed, shutting down now.");
	}
}