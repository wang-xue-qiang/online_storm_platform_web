package com.zkh.trident.pv;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class Split extends BaseFunction{

	private static final long serialVersionUID = 1L;
	String patten = null;
	public Split(String patten){
		this.patten = patten;		
	}

	public void execute(TridentTuple tuple, TridentCollector collector) {
		if(!tuple.isEmpty()){
			String msg = tuple.getString(0);
			String value[] = msg.split(this.patten);
			for (String v : value) {
				collector.emit(new Values(v));
			}		
		}
		
	}

}
