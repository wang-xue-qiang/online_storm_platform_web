package com.zkh.trident.transaction;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.transactional.TransactionalTopologyBuilder;

@SuppressWarnings("deprecation")
public class MyTopo {
	public static void main(String[] args) {
		TransactionalTopologyBuilder builder = 
				new  TransactionalTopologyBuilder("ttbid","spoutid",new MyTxSpout(),1);
		builder.setBolt("bolt1", new MytxBolt(),3).shuffleGrouping("spoutid");
		builder.setBolt("bolt2", new MyCommitter()).shuffleGrouping("bolt1");
		Config conf = new Config();
		conf.setDebug(true);
		if (args.length > 0) {
          try {
              StormSubmitter.submitTopology(args[0], conf, builder.buildTopology());
          } catch (Exception e) {
          }
      }else{
          LocalCluster localCluster = new LocalCluster();
          localCluster.submitTopology("mytopology", conf, builder.buildTopology());
      }
	}
}
