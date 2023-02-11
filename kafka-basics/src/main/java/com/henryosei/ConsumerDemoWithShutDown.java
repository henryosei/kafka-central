package com.henryosei;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoWithShutDown {

    private static final Logger logger= LoggerFactory.getLogger(ConsumerDemoWithShutDown.class.getName());

    public ConsumerDemoWithShutDown() {
    }

    public static void main(String[] args) {
        logger.info("I am a Kafka consumer");
        String bootstrapServer = "127.0.0.1:9092";

        //Create consumer config
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"mysecond-application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //Create config
        KafkaConsumer<String, String> consumer=new KafkaConsumer<String, String>(properties);

        //Get a reference to the current thread
        final Thread mainThread=Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                System.out.println("Detected a shutdown, exit by calling consumer wakeup");
                consumer.wakeup();

                //join the main thread to allow the execution of the code in the main thread
                try{
                    mainThread.join();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        });
        try{
            //Subscribe consumer to top
            consumer.subscribe(Collections.singletonList("kafka_demo"));

            //Poll for new Data

            while(true){
                System.out.println("Polling");
                ConsumerRecords<String, String> records=consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record:records){
                    System.out.println("Key: "+record.key()+", Value: "+ record.value());
                    System.out.println("Partition: "+record.partition()+", Offset"+ record.offset());
                }
            }

        }catch (WakeupException e){
            System.out.println("Wake up exception!");
        }catch (Exception e){
            System.out.println("Unexpected exception");
        }
        finally {
            consumer.close(); //this will also commit the offset if need be
        }

    }
}
