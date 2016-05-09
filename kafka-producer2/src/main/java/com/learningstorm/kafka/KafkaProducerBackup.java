package com.learningstorm.kafka;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchKey;
import java.nio.file.WatchEvent;
import java.nio.file.FileSystems;
import java.nio.file.WatchService;
import java.nio.file.StandardWatchEventKinds;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducerBackup 
{
	public static void main(String[] arg) throws IOException,InterruptedException {

		Properties props = new Properties();
		/*List of Kafka brokers. Complete list of brokers is not
		required as the producer will auto discover the rest of
		the brokers. Change this to suit your deployment.*/
		props.put("metadata.broker.list", "10.220.2.90:9092,10.220.2.93:9092,10.220.2.96:9092,10.220.2.97:9092");
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
		// Define the Producer object with key, value, where key is the topic name & value is the message/record
		Producer<String, String> producer = new Producer<String, String>(config);
		
		//Create a watcher object on to the directory from where we want to read the files
		/*Give the directory path where all your files are present, this code will sort all the files
		 * in this directory, & it will read the files only once*/
		Path fileFolder = Paths.get("/home/hduser/data/");
		WatchService watchService = FileSystems.getDefault().newWatchService();
		//register a creation event on the directory so that whenever new files come that will be read by Kafka
		fileFolder.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
		
		boolean valid = true;
		do 
		{
			WatchKey watchKey = watchService.take();

			for (WatchEvent<?> event : watchKey.pollEvents()) 
			{
				if (StandardWatchEventKinds.ENTRY_CREATE.equals(event.kind()))
				{ 
					String fileName = event.context().toString();
					System.out.println("File Created:" + fileName);
					   //get the file name from the watcher service & pass it to write into Kafka topic
					   File f=new File("/home/hduser/data/" + fileName);
					   BufferedReader reader;
					   try {
						   reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
						   String rec;
						   while ((rec = reader.readLine()) != null) {
						//	   String newStr = rec.replace("|", ",");
							   //Give the Kafka topic name & the messages which needs to be consumed
							   KeyedMessage<String,String> data = new KeyedMessage<String, String>("TRTA1", rec);
							   producer.send(data);
						   }
						   System.out.println("Produced data for File" + fileName);
						   createFile(f);
						   reader.close();
					   } catch (Exception e) {
						   throw new RuntimeException("Error", e);
					   }
					}
				}
			valid = watchKey.reset();
		} while (valid);
	}
	
	private static void createFile(File oldFile) {
		try {
			File file = oldFile;
			String nameF = oldFile.getName();
			if (file.renameTo(new File("/home/hduser/" + nameF))) {
				System.out.println("The file was moved successfully to the backup folder.");
			} else {
				System.out.println("The File was not moved.");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
