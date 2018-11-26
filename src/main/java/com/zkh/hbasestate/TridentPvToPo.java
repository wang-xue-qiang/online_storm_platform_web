package com.zkh.hbasestate;
import java.util.Random;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import com.zkh.trident.pv.MySplit;

public class TridentPvToPo {

  @SuppressWarnings({ "unchecked", "rawtypes" })
public static StormTopology buildTopology() {
	Random random = new Random();
	String[] hosts  ={"www.taobao.com"};
	String[] session_id={"ewfytdsdjwu121323","fgehdwegyuwtr232fe","gdfyutej1223fdfe","behfejhiue3435dfg","sfegf34325fdfgr"};
	String[] time ={"2018-10-10","2018-11-11","2018-01-01","2018-09-09","2018-07-10","2018-06-10"};
    FixedBatchSpout spout = new FixedBatchSpout(new Fields("eachLog"), 3, 
    		new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(6)]),
    		new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(6)]),
    		new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(6)]),
    		new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(6)]),
    		new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(6)]),
    		new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(6)]),
    		new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(6)]),
    		new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(6)])    		
    		);
    spout.setCycle(true);
    
    
    TridentConfig config = new TridentConfig("state");
    StateFactory state = HBaseAggregateState.transactional(config);
    TridentTopology topology = new TridentTopology();
    topology.newStream("spout", spout)
    		.each(
    				new Fields("eachLog"),
    				new MySplit("\t"), 
    				new Fields("date","cf","pv_count","session_id"))
    		.project(new Fields("date","cf","pv_count"))
    		.groupBy(new Fields("date","cf","pv_count"))
    		.persistentAggregate(
    				state,
    				new Count(), 
    				new Fields("PV")
    				);


    	return topology.build();
  }

  public static void main(String[] args) throws Exception {
    Config conf = new Config();
    conf.setMaxSpoutPending(20);
    if (args.length == 0) {
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("state", conf, buildTopology());
      Thread.sleep(1000);  
    }
    else {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0], conf, buildTopology());
    }
  }
}
