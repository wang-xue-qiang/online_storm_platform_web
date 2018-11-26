package com.zkh.storm.wordcount;

import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.FailedException;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class MySplit implements IBasicBolt{
	private static final long serialVersionUID = 1L;
	String patten = null;
	public MySplit(String patten){
		this.patten = patten;		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("word"));
	}


	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}


	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		
	}


	public void execute(Tuple input, BasicOutputCollector collector) {
		try {
			String sen = input.getString(0);
			if(sen != null){
				for(String str:sen.split(patten)){
					collector.emit(new Values(str));
				}
			}
		} catch (Exception e) {
			throw new FailedException("split fail!");
		}
		
	}


	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

}
