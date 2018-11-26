package com.zkh.storm.visits;

import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Values;

public class SourceSpout implements IRichSpout {
	private static final long serialVersionUID = 1L;
	SpoutOutputCollector collector = null;
	String str = null;
	Queue<String> queue = new ConcurrentLinkedQueue<>();
	@SuppressWarnings("rawtypes")

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		try {
			this.collector = collector;
			Random random = new Random();
			String[] hosts ={"www.taobao.com"};
			String[] session_id={"ewfytdsdjwu121323","fgehdwegyuwtr232fe","gdfyutej1223fdfe","behfejhiue3435dfg","sfegf34325fdfgr"};
			String[] time ={"2018-10-10 10:10:10","2018-11-11 11:11:11","2018-01-01 01:01:01","2018-09-09 16:50:10","2018-07-10 12:16:30","2018-60-10 05:20:10"};
			for (int i = 0; i < 100; i++) {
				queue.add(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(6)]+"\n");			
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}


	public void close() {
		// TODO Auto-generated method stub
		
	}


	public void activate() {
		// TODO Auto-generated method stub
		
	}


	public void deactivate() {
		// TODO Auto-generated method stub
		
	}


	public void nextTuple() {
		System.err.println("=====================================//////////");
		if(queue.size() >= 0){
			collector.emit(new Values(queue.poll()));
		}
		try {
			Thread.sleep(5000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}


	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		
	}


	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		
	}


	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}


	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	

}
