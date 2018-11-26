package com.zkh.storm.mysql;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;

import com.zkh.storm.mysql.blolt.MySplitBolt;
import com.zkh.storm.mysql.blolt.MyWordCountAndPrintBolt;

 
/**
 * 这个Driver使Kafka、strom、mysql进行串联起来。
 *
 * 这个代码执行前需要创建kafka的topic,创建代码如下：
 * [root@hadoop-senior kafka]# bin/kafka-topics.sh --create --zookeeper hadoop-senior.ibeifeng.com:2181 --replication-factor 1 -partitions 3 --topic wordCount
 *
 * 接着还要向kafka中传递数据，打开一个shell的producer来模拟生产数据
 * [root@hadoop-senior kafka]# bin/kafka-console-producer.sh --broker-list hadoop-senior.ibeifeng.com:9092 --topic wordCount
 * 接着输入数据
 */
public class StormTopologyDriver {
 
    public static void main(String[] args) throws Exception {
        //1、准备任务信息
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        //SpoutConfig spoutConfig = new SpoutConfig(new ZkHosts("hadoop-senior.ibeifeng.com:2181"),"wordCount","/wordCount","wordCount");
        SpoutConfig spoutConfig = new SpoutConfig(new ZkHosts("hadoop-senior.ibeifeng.com:2181"),"areaOrder","/areaOrder","areaOrder");
        spoutConfig.ignoreZkOffsets =false;
        topologyBuilder.setSpout("KafkaSpout",new KafkaSpout(spoutConfig),2);
        topologyBuilder.setBolt("bolt1",new MySplitBolt(),4).shuffleGrouping("KafkaSpout");
        topologyBuilder.setBolt("bolt2",new MyWordCountAndPrintBolt(),2).shuffleGrouping("bolt1");
 
        //2、任务提交
        Config config = new Config();
        config.setNumWorkers(2);
        StormTopology stormTopology = topologyBuilder.createTopology();
        
        if(args != null && args.length > 0){
        	StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
        }else{
            //本地模式
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("wordcount",config,stormTopology);
        }
        
    }
}
