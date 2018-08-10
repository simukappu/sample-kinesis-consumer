package com.example.aws.v1.dynamodb.streams.consumer;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.example.aws.v1.kinesis.consumer.DisplayConsumer;

/**
 * Replicate records to another table and checkpoints progress.
 */
public class ReplicationConsumer implements IRecordProcessor {

	private static final Log LOG = LogFactory.getLog(DisplayConsumer.class);
	private String shardId;

	private final AmazonDynamoDB dynamoDBClient;
	private final String destTableName;
	private String keyAttribute = null;

	// Backoff and retry settings
	private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
	private static final int NUM_RETRIES = 10;

	// Checkpoint about once a minute
	private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
	private long nextCheckpointTimeInMillis;

	private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

	/**
	 * Constructor using destTableName field
	 * 
	 * @param dynamoDBClient
	 * @param destTableName
	 */
	public ReplicationConsumer(AmazonDynamoDB dynamoDBClient, String destTableName) {
		super();
		this.dynamoDBClient = dynamoDBClient;
		this.destTableName = destTableName;
		this.keyAttribute = dynamoDBClient.describeTable(destTableName).getTable().getKeySchema().stream()
				.filter(keySchemaElement -> keySchemaElement.getKeyType().equals(KeyType.HASH.name())).findFirst().get().getAttributeName();
		System.out.println("Configured to replicate items to table [" + this.destTableName + "] with key attribute [" + this.keyAttribute + "]");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void initialize(String shardId) {
		LOG.info("Initializing record processor for shard: " + shardId);
		this.shardId = shardId;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
		LOG.info("Processing " + records.size() + " records from " + shardId);

		// Process records and perform all exception handling.
		processRecordsWithRetries(records);

		// Checkpoint once every checkpoint interval.
		if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
			checkpoint(checkpointer);
			nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
		LOG.info("Shutting down record processor for shard: " + shardId);
		// Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
		if (reason == ShutdownReason.TERMINATE) {
			checkpoint(checkpointer);
		}
	}

	/**
	 * Checkpoint with retries.
	 * 
	 * @param checkpointer
	 */
	private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
		LOG.info("Checkpointing shard " + shardId);
		for (int i = 0; i < NUM_RETRIES; i++) {
			try {
				checkpointer.checkpoint();
				break;
			} catch (ShutdownException se) {
				// Ignore checkpoint if the processor instance has been shutdown (fail over).
				LOG.info("Caught shutdown exception, skipping checkpoint.", se);
				break;
			} catch (ThrottlingException e) {
				// Backoff and re-attempt checkpoint upon transient failures
				if (i >= (NUM_RETRIES - 1)) {
					LOG.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
					break;
				} else {
					LOG.info("Transient issue when checkpointing - attempt " + (i + 1) + " of " + NUM_RETRIES, e);
				}
			} catch (InvalidStateException e) {
				// This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
				LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
				break;
			}
			try {
				Thread.sleep(BACKOFF_TIME_IN_MILLIS);
			} catch (InterruptedException e) {
				LOG.debug("Interrupted sleep", e);
			}
		}
	}

	/**
	 * Process records performing retries as needed. Skip "poison pill" records.
	 * 
	 * @param records
	 *            Data records to be processed.
	 */
	private void processRecordsWithRetries(List<Record> records) {
		for (Record record : records) {
			boolean processedSuccessfully = false;
			for (int i = 0; i < NUM_RETRIES; i++) {
				try {
					processSingleRecord(record);
					processedSuccessfully = true;
					break;
				} catch (Throwable t) {
					LOG.warn("Caught throwable while processing record " + record, t);
				}

				// backoff if we encounter an exception.
				try {
					Thread.sleep(BACKOFF_TIME_IN_MILLIS);
				} catch (InterruptedException e) {
					LOG.debug("Interrupted sleep", e);
				}
			}

			if (!processedSuccessfully) {
				LOG.error("Couldn't process record " + record + ". Skipping the record.");
			}
		}
	}

	/**
	 * Process a single record.
	 * 
	 * @param record
	 *            The record to be processed.
	 */
	private void processSingleRecord(Record record) {
		// * Add your own record processing logic here *

		String data = null;
		try {
			// For this app, we interpret the payload as UTF-8 chars.
			data = decoder.decode(record.getData()).toString();

			// Replicate records
			if (record instanceof RecordAdapter) {
				com.amazonaws.services.dynamodbv2.model.Record streamRecord = ((RecordAdapter) record).getInternalObject();
				StreamRecord ddbStreamRecord = streamRecord.getDynamodb();

				switch (streamRecord.getEventName()) {
				case "INSERT":
					dynamoDBClient.putItem(new PutItemRequest().withTableName(destTableName).withItem(ddbStreamRecord.getNewImage()));
					System.out.println("---\nRecord inserted.\n" + ddbStreamRecord.getNewImage());
					break;
				case "MODIFY":
					dynamoDBClient.putItem(new PutItemRequest().withTableName(destTableName).withItem(ddbStreamRecord.getNewImage()));
					System.out.println("---\nRecord updated.\nFrom: " + ddbStreamRecord.getOldImage() + "To  : "
							+ new PutItemRequest().withTableName(destTableName).withItem(ddbStreamRecord.getNewImage()));
					break;
				case "REMOVE":
					Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
					key.put(keyAttribute, new AttributeValue().withN(ddbStreamRecord.getKeys().get(keyAttribute).getN()));
					dynamoDBClient.deleteItem(new DeleteItemRequest().withTableName(destTableName).withKey(key));
					System.out.println("---\nRecord deleted.\n" + ddbStreamRecord.getOldImage());
					break;
				}
			}
		} catch (CharacterCodingException e) {
			LOG.error("Malformed data: " + data, e);
		}
	}
}
