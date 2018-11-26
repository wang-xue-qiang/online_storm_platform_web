package com.zkh.storm.wordcount;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.FailedException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class MySum extends BaseBasicBolt{
	private static final long serialVersionUID = 1L;
	Map<String,Integer> counts = new HashMap<String,Integer>();

	public void execute(Tuple input, BasicOutputCollector collector) {
		try {
			long word_sum =0;
			long word_count =0;
			String  word = input.getString(0);
			Integer count = input.getInteger(1);
			counts.put(word, count);

			Iterator<Integer> i =counts.values().iterator();
			while(i.hasNext()){
				word_sum += i.next();
			}
			Iterator<String> j =counts.keySet().iterator();
			while(j.hasNext()){
				 String oneWordStr = j.next();
				 if(oneWordStr!= null){
					 word_count++;
				 }
			}
			System.out.println("MySum===========>word_sum:"+word_count+", word_sum:"+word_sum);
			collector.emit(new Values(word,count));

		} catch (Exception e) {
			throw new FailedException("SumBolt fail!");
		}
		
	}
	

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word","count"));
	}

	

	

}
