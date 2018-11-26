package com.zkh.trident.transaction;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.transactional.ITransactionalSpout;
import org.apache.storm.tuple.Fields;
/**
 * 普通事务
 * @author admin
 *
 */
public class MyTxSpout implements ITransactionalSpout<MyMata>{
	private static final long serialVersionUID = 1L;
	Map<Long,String> dbMap = null;
	public MyTxSpout(){
		dbMap = new HashMap<>();
		Random random = new Random();
		String[] hosts ={"www.taobao.com"};
		String[] session_id={"ewfytdsdjwu121323","fgehdwegyuwtr232fe","gdfyutej1223fdfe","behfejhiue3435dfg","sfegf34325fdfgr"};
		String[] time ={"2018-10-10 10:10:10","2018-11-11 11:11:11","2018-01-01 01:01:01","2018-09-09 16:50:10","2018-07-10 12:16:30","2018-60-10 05:20:10"};
		for (long i = 0; i < 100; i++) {
			dbMap.put(i,hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(6)]+"\n");			
		}
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tx","log"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public org.apache.storm.transactional.ITransactionalSpout.Coordinator<MyMata> getCoordinator(Map conf,
			TopologyContext context) {
		return new MyCoordinator();
	}

	@Override
	public org.apache.storm.transactional.ITransactionalSpout.Emitter<MyMata> getEmitter(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context) {
		return new MyEmitter(dbMap);
	}

}
