package com.zkh.uv;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class LogFmtBlot extends BaseBasicBolt{
	private static final long serialVersionUID = 1L;
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		try {
			if(input != null){
				String logStr = input.getString(0);
				String arr[] = logStr.split("\\t");
				System.err.println("下一级数据为："+arr[2]+"===="+arr[1]);
				collector.emit(new Values(arr[2],arr[1]));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("date","session_id"));
	}

	
}
