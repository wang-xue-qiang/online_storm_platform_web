package com.zkh.trident.pvService;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FirstN;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.tuple.Fields;
import com.zkh.hbasestate.HBaseAggregateState;
import com.zkh.hbasestate.TridentConfig;
/**
 * 
 * @Description: hbase持久化
 * @author wangxueqiang
 * @date 2018年10月31日 上午12:28:06
 *
 */
public class TridentTopo {
	@SuppressWarnings("rawtypes")
	public static StormTopology builder(LocalDRPC drpc){
		TridentConfig tridentConfig = new TridentConfig("state");
	    StateFactory state = HBaseAggregateState.transactional(tridentConfig);
	    String topic = "pv";
	    BrokerHosts brokerHosts = new ZkHosts("hadoop-senior.ibeifeng.com:2181");
        TridentKafkaConfig config = new TridentKafkaConfig(brokerHosts, topic);
        config.scheme = new SchemeAsMultiScheme(new StringScheme());
        config.fetchSizeBytes  =100;  
        TransactionalTridentKafkaSpout spout = new TransactionalTridentKafkaSpout(config);
        TridentTopology topology = new TridentTopology() ;
        //销售额
        TridentState amtState = topology.newStream("spout", spout)
        		.parallelismHint(1)
        		.each(
        				new Fields(StringScheme.STRING_SCHEME_KEY),
        				new OrderAmtSplit("\\t"), 
        				new Fields("order_id","order_amt","create_date","province_id","cf")
        			 )
        		.shuffle()
        		.groupBy(new Fields("create_date","cf","province_id"))
        		.persistentAggregate(
        				state,//new MemoryMapState.Factory(), 
        				new Fields("order_amt"),
        				new Sum(),
        				new Fields("sum_amt")
        				);
        
        topology.newDRPCStream("getOrderAmt", drpc)
        		.each(
        				new Fields("args"),
        				new Split(" "),
        				new Fields("arg")
        			 )
        		.each(
        				new Fields("arg"),
        				new SplitBy("\\:"),
        				new Fields("create_date","cf","province_id")
        			 )
        		.groupBy(new Fields("create_date","cf","province_id"))
        		.stateQuery(
        				amtState,
        				new Fields("create_date","cf","province_id"),
        				new MapGet(), 
        				new Fields("sum_amt")
        				)
        		.applyAssembly(new FirstN(5,"sum_amt",true));
        
        //订单数
        TridentState orderState = topology.newStream("orderSpout", spout)
        		.parallelismHint(1)
        		.each(
        				new Fields(StringScheme.STRING_SCHEME_KEY),
        				new OrderNumSplit("\\t"), 
        				new Fields("order_id","order_amt","create_date","province_id","cf")
        			 )
        		.shuffle()
        		.groupBy(new Fields("create_date","cf","province_id"))
        		.persistentAggregate(
        				state,//new MemoryMapState.Factory(), 
        				new Fields("order_id"),
        				new Count(),
        				new Fields("order_number")
        				);
        topology.newDRPCStream("getOrderNumber", drpc)
		.each(
				new Fields("args"),
				new Split(" "),
				new Fields("arg")
			 )
		.each(
				new Fields("arg"),
				new SplitBy("\\:"),
				new Fields("create_date","cf","province_id")
			 )
		.groupBy(new Fields("create_date","cf","province_id"))
		.stateQuery(
				orderState,
				new Fields("create_date","cf","province_id"),
				new MapGet(), 
				new Fields("order_number")
				);
        //.applyAssembly(new FirstN(5,"order_mumber",true));
        return topology.build();
	}
    public static void main(String[] args) throws AuthorizationException {
    	LocalDRPC drpc = new LocalDRPC ();
		Config conf = new Config() ;
        conf.setNumWorkers(1);
        conf.setDebug(false);
        if (args.length > 0) {
            try {
                StormSubmitter.submitTopology(args[0], conf,builder(null));
            } catch (Exception e) {
            	e.printStackTrace();
            }
        }else{
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("mytopology", conf, builder(drpc));
        }
        while(true){
        	System.err.println("销售额结果=============>"
        			+drpc.execute("getOrderAmt", "2018-10-31:cf:amt_1 2018-10-31:cf:amt_2 2018-10-31:cf:amt_3"));
        	System.err.println("订单数结果=============>"
        			+drpc.execute("getOrderNumber", "2018-10-31:cf:orderNum_1 2018-10-31:cf:orderNum_2 2018-10-31:cf:orderNum_3"));
        }
        
    }

}
