package com.zkh.storm.wordcount;

import java.util.Map;
import java.util.Random;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class MyRandomSentenceSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	SpoutOutputCollector _collector;
	Random _rand;
	@SuppressWarnings("rawtypes") 
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		_rand = new Random();
	}

	String[] sentences = new String[] {"a b a"};

	public void nextTuple() {
		for (String string : sentences) {
			_collector.emit(new Values(string));
		}
		Utils.sleep(100);
	}


	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("word"));
	}

}