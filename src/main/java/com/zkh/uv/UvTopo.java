package com.zkh.uv;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class UvTopo {
	public static void main(String[] args) throws Exception {
		TopologyBuilder  builder = new TopologyBuilder();
        builder.setSpout("spout",new LogSpout("uv"),3);
        builder.setBolt("fmBlot", new LogFmtBlot(),5).shuffleGrouping("spout");
        builder.setBolt("uvBolt", new UVBlot(),2).fieldsGrouping("fmBlot",new Fields("date","session_id"));
        builder.setBolt("rsltbolt",new UVRstlBlot(),1).shuffleGrouping("uvBolt");
        //2、任务提交
        Config config = new Config();
        config.setDebug(true);
        StormTopology stormTopology = builder.createTopology();
        if(args != null && args.length > 0){
        	StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        }else{
            //本地模式
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("uvTop",config,stormTopology);
        }
	}
}
