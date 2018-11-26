package com.zkh.trident.pv;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class MySplit extends BaseFunction{
	private static final long serialVersionUID = 1L;
	String patten = null;
	public MySplit(String patten){
		this.patten = patten;		
	}
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		if(!tuple.isEmpty()){
			String log = tuple.getString(0);
			String logArr[] = log.split(this.patten);
			if(logArr.length==3){
				collector.emit(new Values(logArr[2],"cf","pv_count",logArr[1]));
			}		
		}
		
	}


}
