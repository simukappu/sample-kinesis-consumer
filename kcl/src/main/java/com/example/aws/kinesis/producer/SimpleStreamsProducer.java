package com.example.aws.kinesis.producer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SimpleStreamsProducer {

	private static final String STREAM_NAME = System.getProperty("stream.name", "tokyo-stream-1");
	private static final String REGION = System.getProperty("region", "ap-northeast-1");
	private static final long RECORD_INTERVAL_MILLIS = Long.parseLong(System.getProperty("record.interval.millis", "1000"));
	private static final int RECORD_COUNT = Integer.parseInt(System.getProperty("record.count", "3"));

	public static void main(String[] args) {
		final ObjectMapper mapper = new ObjectMapper();
		AmazonKinesis kinesisClient = AmazonKinesisClientBuilder.standard().withRegion(REGION).build();

		PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
		putRecordsRequest.setStreamName(STREAM_NAME);

		List<RecordObject> recordObjects = new ArrayList<>();
		for (int i = 0; i < RECORD_COUNT; i++) {
			RecordObject recordObject = new RecordObject(String.valueOf(i));
			recordObjects.add(recordObject);
		}

		while (true) {
			List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
			for (int i = 0; i < RECORD_COUNT; i++) {
				RecordObject recordObject = recordObjects.get(i);
				recordObject.incrementRecordCount();
				recordObject.setTimestampToNow();
				PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
				try {
					putRecordsRequestEntry.setData(ByteBuffer.wrap(mapper.writeValueAsString(recordObject).getBytes()));
				} catch (JsonProcessingException e) {
					e.printStackTrace();
				}
				putRecordsRequestEntry.setPartitionKey(recordObject.partitionKey);
				putRecordsRequestEntryList.add(putRecordsRequestEntry);
			}
			putRecordsRequest.setRecords(putRecordsRequestEntryList);
			PutRecordsResult putRecordsResult = kinesisClient.putRecords(putRecordsRequest);
			System.out.println("Put Result : " + putRecordsResult);

			try {
				Thread.sleep(RECORD_INTERVAL_MILLIS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

}
