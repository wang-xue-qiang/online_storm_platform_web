package com.zkh.storm.demo;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
//https://github.com/nathanmarz/storm-starter 官方例子
public class MyPolo {
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new MySpout(),1);
		builder.setBolt("blot", new MyBolt(),2).shuffleGrouping("spout");
		Map conf = new HashMap();
		conf.put(Config.TOPOLOGY_WORKERS, 4);
		if (args.length > 0) {
            try {
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            } catch (Exception e) {
            }
        }else{
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("mytopology", conf, builder.createTopology());
        }
	}
}
