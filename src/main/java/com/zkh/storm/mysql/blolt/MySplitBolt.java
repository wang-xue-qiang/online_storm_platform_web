package com.zkh.storm.mysql.blolt;
 
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
 
/**
 * 这个Bolt模拟从kafkaSpout接收数据，并把数据信息发送给MyWordCountAndPrintBolt的过程。
 */
public class MySplitBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 4482101012916443908L;
    public void execute(Tuple input, BasicOutputCollector collector) {
        //1、数据如何获取
        //如果StormTopologyDriver中的spout配置的是MyLocalFileSpout，则用的是declareOutputFields中的juzi这个key
        //byte[] juzi = (byte[]) input.getValueByField("juzi");
        //2、这里用这个是因为StormTopologyDriver这个里面的spout用的是KafkaSpout，而KafkaSpout中的declareOutputFields返回的是bytes，所以下面用bytes，这个地方主要模拟的是从kafka中获取数据
        byte[] juzi = (byte[]) input.getValueByField("bytes");
/*        //2、进行切割
        String[] strings = new String(juzi).split(" ");
        
        //3、发送数据
        for (String word : strings) {
            //Values对象帮我们生成一个list
            collector.emit(new Values(word,1));
        }*/
        System.err.println("====="+new String(juzi));
        Map<String,String> map = transferContent2Map(new String(juzi));
        collector.emit(new Values(map.get("createTime"),map.get("fan"),map.get("windField"),map.get("windSpeed"),map.get("eletc")));
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
       // declarer.declare(new Fields("word","num"));
    	declarer.declare(new Fields("createTime","fan","windField","windSpeed","eletc"));
    }
    
	//转换字符转为map
	public static Map<String,String>  transferContent2Map(String content){
		Map<String,String> map = new HashMap<String,String>();
		int i = 0;
		String key = "";
		StringTokenizer tokenizer = new StringTokenizer(content, "({|}|\"|:|,)");
		while (tokenizer.hasMoreTokens()) {
			if (++i % 2 == 0) {
				// 当前的值是value
				map.put(key, tokenizer.nextToken());
			} else {
				// 当前的值是key
				key = tokenizer.nextToken();
			}
		}
		return map;
	}
	
	public static void main(String[] args) {
		String str = "{\"windField\":\"内蒙古赤峰赛罕坝风电场\",\"fan\":\"外置式轴流风机\",\"windSpeed\":19.647571840056898,\"eletc\":3.7330386496108106,\"createTime\":\"20180929151649\"}";
		System.out.println(str);
		System.out.println(transferContent2Map(str).get("eletc"));		
	}
}
