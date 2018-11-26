package com.zkh.storm.visits;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.zkh.storm.demo.MySpout;
//https://github.com/nathanmarz/storm-starter 官方例子
/**
 * FieldsGrouping作用：过滤去重
 * 单线程： 加减乘除
 * 多线程：局部加减乘除 持久化
 * @author lenovo
 *
 */
public class PVPolo {
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new MySpout(),1);
		builder.setBolt("blot", new PVBolt1(),4).shuffleGrouping("spout");
		builder.setBolt("sumBolt", new PVSumBolt(),1).fieldsGrouping("blot", new Fields("threadID"));
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
