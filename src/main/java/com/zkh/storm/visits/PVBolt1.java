package com.zkh.storm.visits;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class PVBolt1 extends BaseRichBolt{
	private static final long serialVersionUID = 1L;
	OutputCollector collector = null;
	String session_id=null;
	String longStr= null;
	long Pv= 0;
	@SuppressWarnings("rawtypes")

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
	}

	public void execute(Tuple input) {
		String longStr = input.getString(0);
		session_id = longStr.split("\t")[1];
		if(session_id != null){
			Pv++;
		}	
		System.out.println("thread_id:"+Thread.currentThread().getId()+"=== pv:"+Pv);
		collector.emit(new Values(Thread.currentThread().getId(),Pv));
	}


	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new  Fields("threadID","pv_all"));
	}

}
