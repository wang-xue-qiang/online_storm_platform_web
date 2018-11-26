package com.zkh.storm.wordcount;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
public class WordCountTopology {
  public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new MyRandomSentenceSpout(),1);
		builder.setBolt("split", new MySplit(" "),8).shuffleGrouping("spout");
		builder.setBolt("count", new MyWordCount(),3).fieldsGrouping("split", new Fields("word"));
		builder.setBolt("sum", new MySum(),1).shuffleGrouping("count");
		Config conf = new Config();
		conf.setDebug(true);
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
