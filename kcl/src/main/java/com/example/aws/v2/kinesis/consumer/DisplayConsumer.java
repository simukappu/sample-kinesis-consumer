package com.example.aws.v2.kinesis.consumer;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

/**
 * Display records and checkpoints progress.
 */
public class DisplayConsumer implements ShardRecordProcessor {

	private static final boolean IF_TIME_FIELD_ENABLED = Boolean.parseBoolean(System.getProperty("if.time.field.enabled", "false"));
	private static final String TIME_FIELD_NAME = System.getProperty("time.field.name", "time");
	private static final String TIME_FIELD_FORMAT = System.getProperty("time.field.format", "yyyy-MM-dd'T'HH:mm:ss.SSSxxxxx");

	private static final Log LOG = LogFactory.getLog(DisplayConsumer.class);
	private String shardId;

	private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
	private final DateTimeFormatter dtf = DateTimeFormatter.ofPattern(TIME_FIELD_FORMAT);
	private final ObjectMapper mapper = new ObjectMapper();

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void initialize(InitializationInput initializationInput) {
		this.shardId = initializationInput.shardId();
		LOG.info("Initializing record processor for shard: " + initializationInput.shardId());
		LOG.info("- Initializing @ Sequence: " + initializationInput.extendedSequenceNumber());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void processRecords(ProcessRecordsInput processRecordsInput) {

		try {
			LOG.info("Processing " + processRecordsInput.records().size() + " records from " + shardId);
			processRecordsInput.records().forEach(record -> processSingleRecord(record));
		} catch (Throwable t) {
			LOG.error("Caught throwable while processing records.  Aborting");
			Runtime.getRuntime().halt(1);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void leaseLost(LeaseLostInput leaseLostInput) {
		LOG.info("Lost lease, so terminating. shardId = " + shardId);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void shardEnded(ShardEndedInput shardEndedInput) {
		try {
			LOG.info("Reached shard end checkpointing. shardId = " + shardId);
			shardEndedInput.checkpointer().checkpoint();
		} catch (ShutdownException | InvalidStateException e) {
			LOG.error("Exception while checkpointing at shard end.  Giving up", e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
		try {
			LOG.info("Scheduler is shutting down, checkpointing. shardId = " + shardId);
			shutdownRequestedInput.checkpointer().checkpoint();
		} catch (ShutdownException | InvalidStateException e) {
			LOG.error("Exception while checkpointing at requested shutdown.  Giving up", e);
		}
	}

	/**
	 * Process a single record.
	 * 
	 * @param record
	 *            The record to be processed.
	 */
	private void processSingleRecord(KinesisClientRecord record) {
		String data = null;
		try {
			// For this app, we interpret the payload as UTF-8 chars.
			data = decoder.decode(record.data()).toString();

			// Assume this record including time field and log its age.
			JsonNode jsonData = mapper.readTree(data);
			long approximateArrivalTimestamp = record.approximateArrivalTimestamp().toEpochMilli();
			long currentTime = System.currentTimeMillis();
			long ageOfRecordInMillisFromArrival = currentTime - approximateArrivalTimestamp;
			if (IF_TIME_FIELD_ENABLED) {
				long recordCreateTime = ZonedDateTime.parse(jsonData.get(TIME_FIELD_NAME).asText(), dtf).toInstant().toEpochMilli();
				long ageOfRecordInMillis = currentTime - recordCreateTime;
				System.out.println("---\nShard: " + shardId + ", PartitionKey: " + record.partitionKey() + ", SequenceNumber: "
						+ record.sequenceNumber() + "\nCreated " + ageOfRecordInMillis + " milliseconds ago. Arrived "
						+ ageOfRecordInMillisFromArrival + " milliseconds ago.\n" + data);
			} else {
				System.out.println("---\nShard: " + shardId + ", PartitionKey: " + record.partitionKey() + ", SequenceNumber: "
						+ record.sequenceNumber() + "\nArrived " + ageOfRecordInMillisFromArrival + " milliseconds ago.\n" + data);
			}
		} catch (CharacterCodingException e) {
			LOG.error("Malformed data: " + data, e);
		} catch (IOException e) {
			LOG.info("Record does not match sample record format. Ignoring record with data; " + data);
		}
	}
}
