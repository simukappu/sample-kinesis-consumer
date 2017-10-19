package com.example.aws.kinesis.consumer;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Display records and checkpoints progress.
 */
public class DisplayConsumer implements IRecordProcessor {

	private static final Log LOG = LogFactory.getLog(DisplayConsumer.class);
	private String shardId;

	// Backoff and retry settings
	private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
	private static final int NUM_RETRIES = 10;

	// Checkpoint about once a minute
	private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
	private long nextCheckpointTimeInMillis;

	private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
	private final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSxxxxx");
	private final ObjectMapper mapper = new ObjectMapper();

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
			// Assume this record including time field and log its age.
			JsonNode jsonData = mapper.readTree(data);
			long recordCreateTime = ZonedDateTime.parse(jsonData.get("time").asText(), dtf).toInstant().toEpochMilli();
			long approximateArrivalTimestamp = record.getApproximateArrivalTimestamp().getTime();
			long currentTime = System.currentTimeMillis();
			long ageOfRecordInMillis = currentTime - recordCreateTime;
			long ageOfRecordInMillisFromArrival = currentTime - approximateArrivalTimestamp;

			System.out.println("---\nShard: " + shardId + ", PartitionKey: " + record.getPartitionKey() + ", SequenceNumber: " + record.getSequenceNumber()
					+ "\nCreated " + ageOfRecordInMillis + " milliseconds ago. Arrived " + ageOfRecordInMillisFromArrival + " milliseconds ago.\n" + data);
		} catch (CharacterCodingException e) {
			LOG.error("Malformed data: " + data, e);
		} catch (IOException e) {
			LOG.info("Record does not match sample record format. Ignoring record with data; " + data);
		}
	}
}
