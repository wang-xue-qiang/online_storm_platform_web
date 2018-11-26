package com.zkh.storm.visits;

import java.net.InetAddress;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
public class PVBolt extends BaseRichBolt{

	private static final long serialVersionUID = 1L;
	String session_id=null;
	String longStr= null;
	long Pv= 0;
	public static String zk_path ="/lock/storm/pv";
	ZooKeeper zooKeeper = null;
	String localData = null;
	@SuppressWarnings("rawtypes")

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		 try {
			 zooKeeper = new ZooKeeper("192.168.174.128:2181", 3000, new Watcher(){
			
				public void process(WatchedEvent event) {
					System.err.println("event:"+event.getType());
				}			 
			 });
			while(zooKeeper.getState() != ZooKeeper.States.CONNECTED){
				Thread.sleep(1000);
			}
			InetAddress address = InetAddress.getLocalHost();
			localData = address.getHostAddress()+":"+context.getThisTaskId();
			if(zooKeeper.exists(zk_path, false) == null){
				zooKeeper.create(zk_path, localData.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			}		
		} catch (Exception e) {
			try {
				zooKeeper.close();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
		}
	}
    
	long beginTime = System.currentTimeMillis();
	long endTime  = 0;

	public void execute(Tuple input) {
		try {
			String longStr = input.getString(0);
			System.err.println("///////////////////////////"+longStr);
			if(longStr != null){
				endTime =System.currentTimeMillis();
				session_id = longStr.split("\t")[1];
				if(session_id != null){Pv ++ ;}		
			}
			System.err.println("============PV:"+ Pv * 4);	
			if(endTime - beginTime >= 5 * 1000){
				if(localData.equals(zooKeeper.getData(zk_path, false, null))){
					System.err.println("============PV:"+ Pv * 4);				
				}
				beginTime = System.currentTimeMillis();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	
	}

}
