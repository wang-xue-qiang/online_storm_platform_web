package com.zkh.uv;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class UVProducer {
	private final static String KAFKA_PRODUCER_TOPIC = "uv";
	private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	private final static String hosts[] = {"www.taobao.com"};
	private final static String session_id[] = {"adfggegergngn","ftgthggryjhy","eretrhtgrgn234","bfhgvddgrgr234","dggmkhtnjakhd343"};
	private final static Random random = new Random();
    @SuppressWarnings("resource")
	public static void main(String[] args) throws  Exception {
        Properties properties = new Properties();
        InputStream in = UVProducer.class.getClassLoader().getResourceAsStream("uv.properties");
        properties.load(in);
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        int i =0;
        while(true){
        i++;
        String sid = session_id[random.nextInt(5)]+i;
        String msg1=hosts[0]+"\t"+sid+"\t"+formatter.format(new Date());
        ProducerRecord<String, String> producerRecord1 =
                new ProducerRecord<String, String>(KAFKA_PRODUCER_TOPIC, "uv", msg1);
        producer.send(producerRecord1);

        ProducerRecord<String, String> producerRecord2 =
                new ProducerRecord<String, String>(KAFKA_PRODUCER_TOPIC, "uv", msg1);
        producer.send(producerRecord2);
        Thread.sleep(1000);
        }

    }
}