package com.zendesk.maxwell.producer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.amazonaws.auth.AWSCredentials;

public class SQSProducerCredentials implements AWSCredentials {

	private static String aws_access_key_id;

	private static String aws_secret_access_key;

	private static String queueName;

	public static String getQueueName() {
		return queueName;
	}

	static {
		Properties props = new Properties();
		try {
			InputStream in = SQSProducerCredentials.class.getClassLoader()
					.getResourceAsStream("config.properties.example");
			props.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		}
		aws_access_key_id = props.getProperty("aws_access_key_id");
		aws_secret_access_key = props.getProperty("aws_secret_access_key");
		queueName = props.getProperty("queue_name");
	}

	@Override
	public String getAWSAccessKeyId() {
		return aws_access_key_id;
	}

	@Override
	public String getAWSSecretKey() {
		return aws_secret_access_key;
	}

}
