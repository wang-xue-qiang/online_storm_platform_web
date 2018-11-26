package com.zkh.uv;

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



public class LogConsumer extends Thread{
	private final String topic ;
	private final kafka.javaapi.consumer.ConsumerConnector consumer ;
	Queue<String> queue = new ConcurrentLinkedQueue<String>();
	public LogConsumer(String topic){
		consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
		this.topic =topic;
	}
	public static ConsumerConfig  createConsumerConfig(){
		Properties props = new Properties();
/*		props.put("zookeeper.connect", "hadoop-senior.ibeifeng.com:2181");
		props.put("group.id", "track");
		props.put("bootstrap.servers", "hadoop-senior.ibeifeng.com:9092");
		props.put("zookeeper.session.timeout.ms", "4000");
		props.put("zookeeper.sync.time.ms", "2000");
		props.put("zookeeper.commit.interval.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");*/
		props.put("zookeeper.connect", "hadoop-senior.ibeifeng.com:2181");
		 props.put("bootstrap.servers", "hadoop-senior.ibeifeng.com:9092");
	     props.put("group.id", "tranck");
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
		new LogConsumer("uv").start();
    }

}
