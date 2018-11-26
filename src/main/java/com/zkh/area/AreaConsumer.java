package com.zkh.area;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;



public class AreaConsumer extends Thread{
	private final String topic ;
	private final kafka.javaapi.consumer.ConsumerConnector consumer ;
	Queue<String> queue = new ConcurrentLinkedQueue<String>();
	public AreaConsumer(String topic){
		consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
		this.topic =topic;
	}
	public static ConsumerConfig  createConsumerConfig(){
		Properties props = new Properties();
		 props.put("zookeeper.connect", "bigdata-cdh01.ibeifeng.com:2181,bigdata-cdh02.ibeifeng.com:2181,bigdata-cdh03.ibeifeng.com:2181");
		 props.put("bootstrap.servers", "bigdata-cdh01.ibeifeng.com:9092,bigdata-cdh02.ibeifeng.com:9092,bigdata-cdh03.ibeifeng.com:9092");
	     props.put("group.id", "area_order");
	     props.put("enable.auto.commit", "false");
	     props.put("auto.commit.interval.ms", "1000");
	     props.put("session.timeout.ms", "30000");
	     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return new ConsumerConfig(props);
		
	}
	@Override
	public void run() {
		Map<String,Integer> topicCountMap = new HashMap<String,Integer>();
		topicCountMap.put(topic,new Integer(1));
		Map<String,List<KafkaStream<byte[],byte[]>>>  consumerMap = consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[],byte[]> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[],byte[]> it = stream.iterator();
		while(it.hasNext()){
			String msg = new String(it.next().message());
			System.out.println("================="+msg);
			queue.add(msg);
		}
	}

	public Queue<String> getQueue() {
		return queue;
	}
	
	public static void main(String[] args){
		new AreaConsumer("area_order").start();
    }

}
