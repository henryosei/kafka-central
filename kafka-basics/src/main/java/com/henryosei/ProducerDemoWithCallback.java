package com.henryosei;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger logger= LoggerFactory.getLogger(ProducerDemoWithCallback.class.getName());

    public static void main(String[] args) {

        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer=new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> producerRecord=new ProducerRecord<>("kafka_demo","Kafka is awesome "+i);
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //excecutes on successful send of message to a topic or exception is thrown
                    if (e==null){
                        System.out.println("Receive new metadata.\n"+
                                "Topic: "+ recordMetadata.topic()+"\n"+
                                "Partition: "+recordMetadata.partition()+"\n"+
                                "Offset: "+recordMetadata.offset()+"\n"+
                                "Timestamp: "+recordMetadata.timestamp());
                    }else{
                        logger.error("Error while producing ",e);
                    }
                }
            });
        }


        producer.flush();
        producer.close();
    }

}
