package com.zkh.hbasestate;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.storm.trident.state.Serializer;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.trident.state.map.IBackingMap;

public class HBaseAggregateState<T> implements IBackingMap<T>{
	
	private HTableConnector connector;
	private Serializer<T> serializer;
	
	public HBaseAggregateState(TridentConfig conf){
		this.serializer = conf.getStateSerializer();
		try {
			this.connector = new  HTableConnector(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	//不透明
	public static StateFactory opaque(TridentConfig conf){
		return new HBaseAggregateFactory(conf,StateType.OPAQUE);
	}
	//事务
	public static StateFactory transactional(TridentConfig conf){
		return new HBaseAggregateFactory(conf,StateType.TRANSACTIONAL);
	}
	//非事务
	public static StateFactory nonTransactional(TridentConfig conf){
		return new HBaseAggregateFactory(conf,StateType.NON_TRANSACTIONAL);
	}
	//批量查询
	@Override
	public List<T> multiGet(List<List<Object>> keys) {
		List<Get> gets = new ArrayList<>(keys.size());
		byte[] rk,cf,cq;
		for(List<Object> k:keys){
			//rk = ((String)k.get(0)).getBytes();
			rk = org.apache.hadoop.hbase.util.Bytes.toBytes((String)k.get(0));
			cf = org.apache.hadoop.hbase.util.Bytes.toBytes((String)k.get(1));
			cq = org.apache.hadoop.hbase.util.Bytes.toBytes((String)k.get(2));
			Get get = new Get(rk);
			gets.add(get.addColumn(cf, cq));
		}
		Result[] results = null;
		try {
			results = connector.getTable().get(gets);
		} catch (IOException e) {
			e.printStackTrace();
		}
		List<T> rtn = new ArrayList<T>(keys.size());
		for (int i = 0; i < keys.size(); i++) {
			cf = org.apache.hadoop.hbase.util.Bytes.toBytes((String)keys.get(i).get(1));
			cq = org.apache.hadoop.hbase.util.Bytes.toBytes((String)keys.get(i).get(2));
			Result result = new Result();
			if(result.isEmpty()){
				rtn.add(null);
			}else{
				rtn.add((T)serializer.deserialize(result.getValue(cf, cq)));
			}
		}
		return rtn;
	}
	//批量插入
	@Override
	public void multiPut(List<List<Object>> k, List<T> vals) {
		List<Put> puts = new ArrayList<Put>();
		for (int i = 0; i < k.size(); i++) {
			byte[] rk = org.apache.hadoop.hbase.util.Bytes.toBytes((String)k.get(i).get(0));
			byte[] cf = org.apache.hadoop.hbase.util.Bytes.toBytes((String)k.get(i).get(1));
			byte[] cq = org.apache.hadoop.hbase.util.Bytes.toBytes((String)k.get(i).get(2));
			byte[] cv  = serializer.serialize(vals.get(i));
			Put p  = new Put(rk);
			puts.add(p.add(cf,cq,cv));
		}
		try {
			connector.getTable().put(puts);
			connector.getTable().flushCommits();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
