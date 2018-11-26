package com.zkh.trident.transaction;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseTransactionalBolt;
import org.apache.storm.transactional.ICommitter;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Tuple;

public class MyCommitter extends BaseTransactionalBolt implements ICommitter{
	private static final long serialVersionUID = 1L;
	private static Map<String,DBValue> dbMap = new HashMap<String,DBValue>();
	public static final String GLOBAL_KEY ="global_key";
	long sum = 0;TransactionAttempt id;
	@Override
	public void execute(Tuple tuple) {
		sum += tuple.getLong(1);
	}

	@Override
	public void finishBatch() {
		DBValue value = dbMap.get(GLOBAL_KEY);
		DBValue newValue;
		if(value == null || !value.txid.equals(id.getTransactionId()) ){
			//更新数据库
			newValue =new DBValue();
			newValue.txid = id.getTransactionId();
			if(value == null){
				newValue.count = sum;				
			}else{
				newValue.count =value.count+sum;
			}
			dbMap.put(GLOBAL_KEY, newValue);
		}else{
			newValue =value;
		}
		System.err.println("============>total:"+dbMap.get(GLOBAL_KEY).count);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, 
			BatchOutputCollector collector, TransactionAttempt id) {
		this.id =id;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}
	
	public static class DBValue{
		BigInteger txid;
		long count =0;
		@Override
		public String toString() {
			return "count:"+count;
		}
	}
}
