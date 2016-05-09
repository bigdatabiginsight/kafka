package com.learningstorm.kafka;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {

	public static void main(String[] args) throws IOException {
		
		Properties props = new Properties();
		/*List of Kafka brokers. Complete list of brokers is not
		required as the producer will auto discover the rest of
		the brokers. Change this to suit your deployment.*/
		props.put("metadata.broker.list", "10.169.73.76:9092");
		//this prop states that producer will consume asynchronously, default is sync
		props.put("producer.type", "async");
		//the queue size
		props.put("queue.buffering.max.messages", "20000");
		//this should be equal to the max.messages, it cant be more than that.
		props.put("batch.num.messages","20000");
		/*Serializer used for sending data to kafka. Since we are sending string,
		we are using StringEncoder.*/
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// We don't want acks from Kafka that messages are properly received, we can give the ack by "1".
		props.put("request.required.acks", "0");
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		
		/*Give the directory path where all your files are present, this code will sort all the files
		 * in this directory, & it will read the files only once*/
		File folder = new File("/home/ec2-user/Telecom code/India/CDR_files/");
		//read a file & put into an array of files
		File[] listOfFiles = folder.listFiles();
		String fileName = "";
		//sort the array of files
		Arrays.sort(listOfFiles);
		Date d = new Date();
		System.out.println(d);
		System.out.println("----------------------------------------------------------------------------------------" +
		"----------------------------------------------------------------------------------------------------------");
		for (int i = 0; i < listOfFiles.length; i++) {
			//read 1 file at a time & consume all its data & put it into the topic
		   fileName = listOfFiles[i].getName();
		   // Give the directory path
		   File f=new File("/home/ec2-user/Telecom code/India/CDR_files/" + fileName);
		   BufferedReader reader;
		   try {
			   reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
			   String rec;
			   while ((rec = reader.readLine()) != null) {
			//	   String newStr = rec.replace("|", ",");
				   //Give the Kafka topic name & the messages which needs to be consumed
				   KeyedMessage<String,String> data = new KeyedMessage<String, String>("KafkaProd", rec);
				   producer.send(data);
			   }
			   System.out.println("Produced data for File" + fileName);
			   createFile(f);
			   reader.close();
		   } catch (Exception e) {
			   throw new RuntimeException("Error", e);
		   }
		}
		Date dd = new Date();
		System.out.println(dd);
		System.out.println("Produced data");
	//	producer.close();
	}

	private static void createFile(File oldFile) {
		try {
			File file = oldFile;
			String nameF = oldFile.getName();
			if (file.renameTo(new File("/home/ec2-user/Telecom code/India/CDR_files/CDR_backup/" + nameF))) {
				System.out.println("The file was moved successfully to the new folder");
			} else {
				System.out.println("The File was not moved.");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
