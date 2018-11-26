package com.zkh.trident.pv;
import java.util.Random;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.FirstN;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class TridentPvToPo {

/**
 * 
 * @Description: 内存行
 * @param drpc
 * @return
 * @author wangxueqiang
 * @date 2018年10月31日 上午12:27:31
 *
 */
@SuppressWarnings("unchecked")
public static StormTopology buildTopology(LocalDRPC drpc) {
	Random random = new Random();
	String[] hosts  ={"www.taobao.com"};
	String[] session_id={"ewfytdsdjwu121323","fgehdwegyuwtr232fe","gdfyutej1223fdfe","behfejhiue3435dfg","sfegf34325fdfgr"};
	String[] time ={"2018-10-25","2018-10-26","2018-01-01","2018-10-27","2018-10-28","2018-10-29","2018-10-30"};
    FixedBatchSpout spout = new FixedBatchSpout(new Fields("eachLog"), 3, 
    		new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(7)]),
    		new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(7)]),
    		new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(7)]),
    		new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(7)]),
    		new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(7)]),
    		new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(7)]),
    		new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(7)]),
    		new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(7)])    		
    		);
    spout.setCycle(false);
    TridentTopology topology = new TridentTopology();
    TridentState logs = topology.newStream("spout1", spout)
    		//.parallelismHint(16)//并发度
    		.each(new Fields("eachLog"),new MySplit("\t"), new Fields("date","session_id"))
    		.groupBy(new Fields("date")).persistentAggregate(new MemoryMapState.Factory(),
    				new Fields("session_id"), new Count(), new Fields("PV"));
    		//.parallelismHint(16);

    topology.newDRPCStream("getPV", drpc)
    .each(new Fields("args"), new Split(" "), new Fields("date"))
    .groupBy(new Fields("date"))
    .stateQuery(logs, new Fields("date"), new MapGet(), new Fields("PV"))
    .each(new Fields("PV"), new FilterNull())
    .applyAssembly(new FirstN(2,"PV",true));
   // .aggregate(new Fields("count"), new Sum(), new Fields("sum"));
    return topology.build();
  }

  public static void main(String[] args) throws Exception {
    Config conf = new Config();
    conf.setMaxSpoutPending(20);
    if (args.length == 0) {
      LocalDRPC drpc = new LocalDRPC();
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
      for (int i = 0; i < 100; i++) {
        System.err.println("DRPC RESULT: " + drpc.execute("getPV", "2018-10-10 2018-11-11 2017-11-11"));
        Thread.sleep(1000);
      }
    }
    else {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
    }
  }
}
