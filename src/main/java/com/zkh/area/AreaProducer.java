package com.zkh.area;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
/**
 * 通过这个KafkaProducerOps向Kafka topic中生产相关的数据
 * <p>
 * Producer
 */
public class AreaProducer {
	private final static String KAFKA_PRODUCER_TOPIC = "area_order";
	private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private final static String area_id[] = {"1","2","3","4","5"};
	private final static String order_amt[] = {"10.10","20.3","30.6","40.5","55.8"};
	private final static Random random = new Random();
    @SuppressWarnings("resource")
	public static void main(String[] args) throws  Exception {
        Properties properties = new Properties();
        InputStream in = AreaProducer.class.getClassLoader().getResourceAsStream("area_order.properties");
        properties.load(in);
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        String topic = properties.getProperty("producer.topic");
        System.out.println("==="+topic +properties);
        int i =0;
        while(true){
        i++;
        String msg=i+"\t"+order_amt[random.nextInt(5)]+"\t"+formatter.format(new Date())+"\t"+area_id[random.nextInt(5)];
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<String, String>(KAFKA_PRODUCER_TOPIC, "1", msg);
        producer.send(producerRecord);
        Thread.sleep(1000);
        }
        //producer.close();
    }
}