package com.zkh.area;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.zkh.dao.HbaseDao;
import com.zkh.dao.HbaseDaoImpl;



public class AreaAmtBolt implements IBasicBolt {
	private static final long serialVersionUID = 3L;
	Map<String ,Double> countsMap = null;
	private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	HbaseDao hbaseDao = null;
	String today =null;
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		hbaseDao = new HbaseDaoImpl();
		//countsMap = new HashMap<String ,Double>();
		today = formatter.format(new Date());
		countsMap = this.initMap(today, hbaseDao);
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		//area_id,order_amt,order_date
		if(null != input){
			String area_id = (String)input.getValueByField("area_id");
			String amtStr = (String)input.getValueByField("order_amt");
			double order_amt = Double.parseDouble(amtStr);
			String order_date = (String)input.getValueByField("order_date");
			//跨天清空
			if(! order_date.equals(today)){
				countsMap.clear();
			}
			Double count = countsMap.get(order_date+"_"+area_id);
			if(null == count){count = 0.0;}
			count += order_amt;
			countsMap.put(order_date+"_"+area_id, count);
			System.out.println("有值============》area_id:"+area_id+",order_amt:"+order_amt+",order_date:"+order_date+",area_id:"+order_date+"_"+area_id+",count:"+count);
			collector.emit(new Values(order_date+"_"+area_id,count));
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("date_area","amt"));
	}

	
	public void cleanup() {
		// TODO Auto-generated method stub
		countsMap.clear();
	}
	
	@SuppressWarnings("deprecation")
	public HashMap<String ,Double> initMap(String rowKeyDate,HbaseDao hbaseDao) {
		HashMap<String,Double> rsltMap = new HashMap<String ,Double>();
		List<Result> list = hbaseDao.getRows("area_order", rowKeyDate, new String[]{"order_amt"});
		for (Result rs : list) {
			for(KeyValue kv:rs.raw()){
				if("order_amt".equals(kv.getQualifier())){
					String key = new String(kv.getRow());
					Double value =Double.parseDouble( new String(kv.getValue()));
					System.out.println("rowkey:"+new String(kv.getRow()));
					System.out.println("属性："+new String(kv.getQualifier()));
					System.out.println("值"+new String(kv.getValue()));
					rsltMap.put(key, value);			
				}
			}
		}
		return rsltMap;
	}
}
