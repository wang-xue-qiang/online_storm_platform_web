package com.zkh.storm.demo;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class MyBolt implements IRichBolt{
	
	private static final long serialVersionUID = 1L;
	OutputCollector collector = null;
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector =collector;
	}
	int num =0;
	String str=null;
	public void execute(Tuple input) {
		try {
			str = input.getStringByField("log");
			if(null != str){
				num++;
				System.out.println("线程【"+Thread.currentThread().getName()+"】==========>lines:"+num+",session_id:"+str.split("\t")[1]);
			}
			collector.ack(input);
		} catch (Exception e) {
			e.printStackTrace();
			collector.fail(input);
		}
	}

	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(""));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
