package com.kafka.example;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import com.mysql.cj.result.Row;

public class Consumer {
    @SuppressWarnings("resource")
    public static int consume(String brokers, String groupId, String topicName) {
        KafkaConsumer<String, String> consumer;
        		
        	   Properties properties = new Properties();
               properties.setProperty("bootstrap.servers", brokers);
               properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
               properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
               properties.setProperty("group.id", groupId);
               properties.setProperty("auto.offset.reset","earliest");


        consumer = new KafkaConsumer<String, String>(properties);

        // Subscribe to the 'actors' topic
         consumer.subscribe(Arrays.asList(topicName));

		
		
		  int count = 0; 
		  while(true) {
			  ConsumerRecords<String, String> records =consumer.poll(200);
		  
			  if (records.count() == 0) {
				  System.out.println("consumer data empty");
			  } else { 
				  for(ConsumerRecord<String, String> record: records) { // Display record and count 
					  count += 1;
					  System.out.println( count + ": " + record.value()); 
				  } 
			  } 
		  }
		/*  SparkSession spark=SparkSession.builder().appName("kafka topic subscribe").config("spark.master", "local").getOrCreate();
		  System.out.println(spark);
	       SparkSession
		 * spark=SparkSession.builder().appName("kafka streaming").getOrCreate();
		 readStream
		  Dataset<org.apache.spark.sql.Row> df = spark.().format("kafka").option("kafka.bootstrap.servers", brokers) .option("subscribe", topicName).load(); 
		  df.show();
		 */
    }
}
