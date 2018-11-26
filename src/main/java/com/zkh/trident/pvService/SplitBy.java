package com.zkh.trident.pvService;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class SplitBy extends BaseFunction {

	private static final long serialVersionUID = 1L;
	String patten = null;
	public SplitBy(String patten){
		this.patten = patten;		
	}

	public void execute(TridentTuple tuple, TridentCollector collector) {
		if(!tuple.isEmpty()){
			String msg = tuple.getString(0);
			System.err.println("===============>SplitBy:msg"+tuple.toString());
			String value[] = msg.split(this.patten);
			collector.emit(new Values(value[0],value[1],value[2]));		
		}
		
	}
}
