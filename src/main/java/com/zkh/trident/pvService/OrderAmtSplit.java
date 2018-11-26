package com.zkh.trident.pvService;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
public class OrderAmtSplit extends BaseFunction{
	private static final long serialVersionUID = 1L;
	String patten = null;
	public OrderAmtSplit(String patten){
		this.patten = patten;		
	}

	public void execute(TridentTuple tuple, TridentCollector collector) {
		// "order_id","order_amt","create_date","province_id","cf"
		if(!tuple.isEmpty()){
			String msg = tuple.getString(0);
			String value[] = msg.split(this.patten);
			collector.emit(new Values(value[0],Double.parseDouble(value[1]),value[2],"amt_"+value[3],"cf"));			
		}
		
	}

	

}
