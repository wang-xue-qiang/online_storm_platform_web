package com.zkh.uv;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.zkh.utils.DateUtils;

public class UVBlot extends BaseBasicBolt{
	private static final long serialVersionUID = 1L;
	Map<String,Long>  map = new HashMap<>();
	Set<String> hasEmittedSet = new HashSet<>();
	String today = DateUtils.getToday(null);
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		try {
			if(input != null){
				String date = input.getString(0);
				System.err.println(date);
				//跨天处理
				if(today.compareTo(date)<0){
					today =date;
					map.clear();
					hasEmittedSet.clear();
				}
				String session_id = input.getString(1);
				String key = date+"_"+session_id;
				if(hasEmittedSet.contains(key)){
					throw new Exception("this toup has emittrd");
				}
				Long pv = map.get(key);
				if(pv == null){
					pv =0l;
				}
				pv++;
				map.put(key, pv);
				if(pv >= 2){
					collector.emit(new Values(key));
					hasEmittedSet.add(key);
				}
				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("date_sessionId"));
	}

	
}
