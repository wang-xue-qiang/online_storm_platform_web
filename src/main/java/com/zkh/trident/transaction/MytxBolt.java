package com.zkh.trident.transaction;

import java.util.Map;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseTransactionalBolt;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class MytxBolt extends BaseTransactionalBolt{

	private static final long serialVersionUID = 1L;
	long count =0;
	BatchOutputCollector collector = null;
	TransactionAttempt tx;
	@Override
	public void execute(Tuple input) {
		tx = (TransactionAttempt) input.getValue(0);
		System.err.println("MytxBolt TransactionAttempt:"+tx.getTransactionId()+" ,attemptId："+tx.getAttemptId());
		String log = input.getString(1);
		if(log != null && log.length()>0){
			count++;
		}
	}

	@Override
	public void finishBatch() {
		collector.emit(new Values(tx,count));
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context,
			BatchOutputCollector collector, TransactionAttempt id) {
		System.err.println("MytxBolt prepare:"+id);
		this.collector =collector;
		this.tx =id;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tx","count"));
	}

}
