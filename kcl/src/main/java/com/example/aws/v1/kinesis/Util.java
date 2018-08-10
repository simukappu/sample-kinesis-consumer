package com.example.aws.v1.kinesis;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;

public class Util {
	/**
	 * Initialize network configuration and AWS credentials
	 * 
	 * @return credentialsProvider
	 */
	public static AWSCredentialsProvider initCredentialsProvider() {
		// Ensure the JVM will refresh the cached IP values of AWS resources (e.g. service endpoints).
		java.security.Security.setProperty("networkaddress.cache.ttl", "60");

		// The ProfileCredentialsProvider will return your [default] credential profile.
		AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
		try {
			credentialsProvider.getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct location, "
					+ "and is in valid format.", e);
		}
		return credentialsProvider;
	}
}
