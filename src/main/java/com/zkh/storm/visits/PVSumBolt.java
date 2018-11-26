package com.zkh.storm.visits;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class PVSumBolt extends BaseRichBolt{
	private static final long serialVersionUID = 1L;
	OutputCollector collector = null;
	String session_id=null;
	String longStr= null;
	long Pv,pv_all= 0;
	Map<Long,Long> counts = new HashMap<Long,Long>();
	@SuppressWarnings("rawtypes")

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;		
	}

	public void execute(Tuple input) {
		if(null !=input){
			long threadID = input.getLong(0);
			long pv = input.getLong(1);	
			counts.put(threadID, pv);
			Iterator<Long> i =counts.values().iterator();
			while(i.hasNext()){
				pv_all += i.next();
			}
			System.out.println("pv_all=====>"+pv_all);
		}
	}


	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
