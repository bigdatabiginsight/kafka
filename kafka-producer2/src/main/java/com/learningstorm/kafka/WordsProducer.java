package com.learningstorm.kafka;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class WordsProducer {
	public static void main(String[] args) {
		/*Give the path of the file which you want to read & put into topic.*/
		File f=new File("/home/hduser/Data/cdr.txt");
		// Build the configuration required for connecting to Kafka
		Properties props = new Properties();
		//List of Kafka brokers. Complete list of brokers is not
		//required as the producer will auto discover the rest of
		//the brokers. Change this to suit your deployment.
		props.put("metadata.broker.list", "10.220.3.104:9092");
		// Serializer used for sending data to kafka. Since we are sending string,
		// we are using StringEncoder.
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		//this prop states that producer will consume asynchronously, default is sync
		props.put("producer.type", "async");
		//the queue size
		props.put("queue.buffering.max.messages", "20000");
		//this should be equal to the max.messages, it cant be more than that.
		props.put("batch.num.messages","20000");
		// We don't want acks from Kafka that messages are properly received, we can give the ack by "1".
		props.put("request.required.acks", "0");
		// Create the producer instance
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		BufferedReader reader;
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
		
		String str;
		while ((str = reader.readLine()) != null) {
		//	String newStr = str.replace("|", ",");
			   //Give the Kafka topic name
			KeyedMessage<String,String> data = new KeyedMessage<String, String>("test2", str);
			//Send the message
			producer.send(data);

		}
		} catch (Exception e) {
			throw new RuntimeException("Error", e);
		}
		System.out.println("Produced data");
		//close the producer
		producer.close();
	}
}
