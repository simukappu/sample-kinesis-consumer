package com.example.aws.v1.kinesis.producer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.Shard;
import com.example.aws.util.Config;
import com.example.aws.util.RecordObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DistributedExplicitHashKeyStreamsProducer {

	public static void main(String[] args) {
		final ObjectMapper mapper = new ObjectMapper();
		AmazonKinesis kinesisClient = AmazonKinesisClientBuilder.standard().withRegion(Config.REGION).build();

		// Prepare distributed ExplicitHashKey
		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		describeStreamRequest.setStreamName(Config.STREAM_NAME);
		DescribeStreamResult describeStreamResult = kinesisClient.describeStream(describeStreamRequest);
		List<Shard> shardList = describeStreamResult.getStreamDescription().getShards();
		List<String> explicitHashKeyList = new ArrayList<>();
		while (explicitHashKeyList.size() < Config.RECORD_COUNT) {
			for (Shard shard : shardList) {
				explicitHashKeyList.add(shard.getHashKeyRange().getStartingHashKey());
				if (explicitHashKeyList.size() >= Config.RECORD_COUNT) {
					break;
				}
			}
		}

		PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
		putRecordsRequest.setStreamName(Config.STREAM_NAME);

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
				PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
				try {
					putRecordsRequestEntry.setData(ByteBuffer.wrap(mapper.writeValueAsString(recordObject).getBytes()));
				} catch (JsonProcessingException e) {
					e.printStackTrace();
				}
				putRecordsRequestEntry.setPartitionKey(recordObject.partitionKey);
				putRecordsRequestEntry.setExplicitHashKey(explicitHashKeyList.get(i));
				putRecordsRequestEntryList.add(putRecordsRequestEntry);
			}
			putRecordsRequest.setRecords(putRecordsRequestEntryList);
			PutRecordsResult putRecordsResult = kinesisClient.putRecords(putRecordsRequest);
			System.out.println("Put Result : " + putRecordsResult);

			try {
				Thread.sleep(Config.RECORD_INTERVAL_MILLIS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

}
