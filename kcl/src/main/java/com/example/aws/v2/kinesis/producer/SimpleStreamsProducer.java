package com.example.aws.v2.kinesis.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.example.aws.util.Config;
import com.example.aws.util.RecordObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

public class SimpleStreamsProducer {

	public static void main(String[] args) {
		final ObjectMapper mapper = new ObjectMapper();
		KinesisAsyncClient kinesisClient = KinesisAsyncClient.builder().credentialsProvider(ProfileCredentialsProvider.create())
				.region(Region.of(Config.REGION)).build();

		List<RecordObject> recordObjects = new ArrayList<>();
		for (int i = 0; i < Config.RECORD_COUNT; i++) {
			RecordObject recordObject = new RecordObject(String.valueOf(i));
			recordObjects.add(recordObject);
		}

		while (true) {
			List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
			for (int i = 0; i < Config.RECORD_COUNT; i++) {
				RecordObject recordObject = recordObjects.get(i);
				recordObject.incrementRecordCount();
				recordObject.setTimestampToNow();
				try {
					putRecordsRequestEntryList.add(PutRecordsRequestEntry.builder()
							.data(SdkBytes.fromByteArray(mapper.writeValueAsString(recordObject).getBytes()))
							.partitionKey(recordObject.partitionKey).build());
				} catch (JsonProcessingException e) {
					e.printStackTrace();
				}
			}

			PutRecordsRequest putRecordsRequest = PutRecordsRequest.builder().streamName(Config.STREAM_NAME)
					.records(putRecordsRequestEntryList).build();
			PutRecordsResponse putRecordsResponse;
			try {
				putRecordsResponse = kinesisClient.putRecords(putRecordsRequest).get();
				System.out.println("Put Result : " + putRecordsResponse);
			} catch (InterruptedException e) {
				System.out.println("Interrupted, assuming shutdown.");
			} catch (ExecutionException e) {
				System.err.println("Exception while sending data to Kinesis will try again next cycle");
			}

			try {
				Thread.sleep(Config.RECORD_INTERVAL_MILLIS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

}
