package com.zkh.area;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class AreaSpout implements IRichSpout{
	private static final long serialVersionUID = 1L;
	String topic = null;
	SpoutOutputCollector collector = null;
	Integer taskId = null;
	Queue<String> queue = new ConcurrentLinkedQueue<String>();
	public AreaSpout(String topic){
		this.topic = topic;
	}
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		taskId = context.getThisTaskId();
		AreaConsumer consumer = new AreaConsumer(topic);
		consumer.start();
		queue =consumer.getQueue();
	}

	@Override
	public void close() {
		
		
	}

	@Override
	public void activate() {
		
		
	}

	@Override
	public void deactivate() {
		
		
	}

	@Override
	public void nextTuple() {
		if(queue.size()>0){
			String str = queue.poll();
			System.err.println("/////////////////////"+str);
			collector.emit(new Values(str));
		}
		
	}

	@Override
	public void ack(Object msgId) {
	
	}

	@Override
	public void fail(Object msgId) {
			
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("logs"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
