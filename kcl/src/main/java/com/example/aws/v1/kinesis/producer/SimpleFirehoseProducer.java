package com.example.aws.v1.kinesis.producer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.example.aws.util.Config;
import com.example.aws.util.RecordObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SimpleFirehoseProducer {

	public static void main(String[] args) {
		final ObjectMapper mapper = new ObjectMapper();
		AmazonKinesisFirehose firehoseClient = AmazonKinesisFirehoseClientBuilder.standard().withRegion(Config.REGION).build();

		PutRecordBatchRequest putRecordBatchRequest = new PutRecordBatchRequest();
		putRecordBatchRequest.setDeliveryStreamName(Config.DELIVERY_STREAM_NAME);

		List<RecordObject> recordObjects = new ArrayList<>();
		for (int i = 0; i < Config.RECORD_COUNT; i++) {
			RecordObject recordObject = new RecordObject(String.valueOf(i));
			recordObjects.add(recordObject);
		}

		while (true) {
			List<Record> recordList = new ArrayList<>();
			for (int i = 0; i < Config.RECORD_COUNT; i++) {
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
				Thread.sleep(Config.RECORD_INTERVAL_MILLIS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

}
