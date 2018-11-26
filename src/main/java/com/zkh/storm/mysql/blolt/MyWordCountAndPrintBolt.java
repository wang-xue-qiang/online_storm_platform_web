package com.zkh.storm.mysql.blolt;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import com.zkh.storm.mysql.db.DBProvider;


 


/**
 * 用于统计分析，并且把统计分析的结果存储到mysql中。
 * @author liuyazhuang
 *
 */
public class MyWordCountAndPrintBolt extends BaseBasicBolt {
 
	private static final long serialVersionUID = 5564341843792874197L;
	private DBProvider provider;

    public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) {
        //连接redis---代表可以连接任何事物
    	provider = new DBProvider();
        super.prepare(stormConf,context);
    }
 
    public void execute(Tuple input, BasicOutputCollector collector) {
      //  String word = (String) input.getValueByField("word");
      // Integer num = (Integer) input.getValueByField("num");
    	String createTime = (String) input.getValueByField("createTime");
    	String fan = (String) input.getValueByField("fan");
    	String windField = (String) input.getValueByField("windField");
    	String windSpeed = (String) input.getValueByField("windSpeed");
    	String eletc = (String) input.getValueByField("eletc");
        Connection conn = null;
        Statement stmt = null;
        try {
        	conn = provider.getConnection();
        	stmt = conn.createStatement() ;
        	String sql = "INSERT INTO sys_wind_data (createTime,fan,windField,windSpeed,eletc) VALUES ('" + createTime + "', '" + fan + "','" + windField + "','" + windSpeed + "','" + eletc + "')";
        	System.err.println("==sql:"+sql);
        	//stmt.executeUpdate("INSERT INTO word_count (word, count) VALUES ('" + word + "', " + num + ")  ON DUPLICATE KEY UPDATE count = count + " + num) ; 
        	stmt.executeUpdate(sql) ; 
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(stmt != null){
				try {
					stmt.close();
					stmt = null;
				} catch (Exception e2) {
					e2.printStackTrace();
				}
			}
			if(conn != null){
				try {
					conn.close();
					conn = null;
				} catch (Exception e2) {
					e2.printStackTrace();
				}
			}
		}
    }
 
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //todo 不需要定义输出的字段
    }
}
