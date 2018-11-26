package com.zkh.storm.demo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class MySpout implements IRichSpout {
	private static final long serialVersionUID = 1L;
	FileInputStream fis;
	InputStreamReader isr;
	BufferedReader br;
	SpoutOutputCollector collector;
	public void open(Map conf, TopologyContext context, 
			SpoutOutputCollector collector) {
		try {
			this.collector=collector;
			this.fis = new FileInputStream("track.log");
			this.isr = new InputStreamReader(fis, "UTF-8");
			this.br = new BufferedReader(isr);
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}

	String str = null;
	public void nextTuple() {
		try {
			while((str = this.br.readLine()) !=null){
				collector.emit(new Values(str));
			}
		} catch (Exception e) {
			// TODO: handle exception
			
		}
		
	}

	public void close() {
		try {
			br.close();
			isr.close();
			fis.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	//定义数据格式
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("log"));
	}

	public void activate() {
		// TODO Auto-generated method stub
		
	}
	

	public void deactivate() {
		// TODO Auto-generated method stub
		
	}
	
	
	

	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		
	}
	

	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
