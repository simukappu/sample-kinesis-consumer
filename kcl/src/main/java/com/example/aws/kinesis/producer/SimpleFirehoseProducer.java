package com.example.aws.kinesis.producer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SimpleFirehoseProducer {

	private static final String DELIVERY_STREAM_NAME = System.getProperty("delivery.stream.name", "tokyo-stream-1");
	private static final String REGION = System.getProperty("region", "ap-northeast-1");
	private static final long RECORD_INTERVAL_MILLIS = Long.parseLong(System.getProperty("record.interval.millis", "1000"));
	private static final int RECORD_COUNT = Integer.parseInt(System.getProperty("record.count", "3"));

	public static void main(String[] args) {
		final ObjectMapper mapper = new ObjectMapper();
		AmazonKinesisFirehose firehoseClient = AmazonKinesisFirehoseClientBuilder.standard().withRegion(REGION).build();

		PutRecordBatchRequest putRecordBatchRequest = new PutRecordBatchRequest();
		putRecordBatchRequest.setDeliveryStreamName(DELIVERY_STREAM_NAME);

		List<RecordObject> recordObjects = new ArrayList<>();
		for (int i = 0; i < RECORD_COUNT; i++) {
			RecordObject recordObject = new RecordObject(String.valueOf(i));
			recordObjects.add(recordObject);
		}

		while (true) {
			List<Record> recordList = new ArrayList<>();
			for (int i = 0; i < RECORD_COUNT; i++) {
				RecordObject recordObject = recordObjects.get(i);
				recordObject.incrementRecordCount();
				recordObject.setTimestampToNow();
				Record record = new Record();
				try {
					record.setData(ByteBuffer.wrap(mapper.writeValueAsString(recordObject).getBytes()));
				} catch (JsonProcessingException e) {
					e.printStackTrace();
				}
				recordList.add(record);
			}
			putRecordBatchRequest.setRecords(recordList);
			PutRecordBatchResult putRecordBatchResult = firehoseClient.putRecordBatch(putRecordBatchRequest);
			System.out.println("Put Result : " + putRecordBatchResult);

			try {
				Thread.sleep(RECORD_INTERVAL_MILLIS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

}
