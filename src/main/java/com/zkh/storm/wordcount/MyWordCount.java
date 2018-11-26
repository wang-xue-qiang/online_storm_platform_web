package com.zkh.storm.wordcount;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class MyWordCount extends BaseBasicBolt{
	private static final long serialVersionUID = 1L;
	Map<String,Integer> counts = new HashMap<String,Integer>();

	public void execute(Tuple input, BasicOutputCollector collector) {
		try {
			String word = input.getString(0);
			Integer count = counts.get(word);
			if(count == null){
				count = 0;
				count++;
				counts.put(word, count);
				System.err.println("==========>word:"+word+",count:"+count);
				collector.emit(new Values(word,count));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word","count"));
	}

	

	

}
