package com.example.aws.util;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class RecordObject {
	private static final String TIME_FIELD_FORMAT = System.getProperty("time.field.format", "yyyy-MM-dd'T'HH:mm:ss.SSSxxxxx");

	final DateTimeFormatter dtf = DateTimeFormatter.ofPattern(TIME_FIELD_FORMAT).withZone(ZoneId.of("UTC"));
	// final DateTimeFormatter dtf = DateTimeFormatter.ofPattern(TIME_FIELD_FORMAT).withZone(ZoneId.of("Asia/Tokyo"));
	public String partitionKey = "";
	public long recordCount = 0L;
	public String timestampString = "";

	public RecordObject(String partitionKey) {
		super();
		this.partitionKey = partitionKey;
	}

	public long incrementRecordCount() {
		recordCount++;
		return recordCount;
	}

	public String setTimestampToNow() {
		timestampString = dtf.format(Instant.now());
		return timestampString;
	}
}
