package com.zkh.hbasestate;

import java.io.IOException;
import java.io.Serializable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;

public class HTableConnector implements Serializable{
	private static final long serialVersionUID = 1L;
	private Configuration configuration;
	private HTableInterface table;
	private String tableName;
	HConnection hTablePool = null;
	public HTableConnector(TupleTableConfig conf ) throws Exception{
/*		this.tableName = conf.getTableName();
		this.configuration = HBaseConfiguration.create();
		String fielPath = "hbase-site.xml";
		Path path =  new Path(fielPath); 
		this.configuration.addResource(path);
		this.table = new HTable(this.configuration,this.tableName);*/
		configuration = new Configuration();
		configuration.set("hbase.zookeeper.quorum","hadoop-senior.ibeifeng.com");
		try {
			hTablePool = HConnectionManager.createConnection(configuration);
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.tableName = conf.getTableName();
		this.table = hTablePool.getTable(this.tableName);
	}
	public Configuration getConfiguration() {
		return configuration;
	}
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}
	public HTableInterface getTable() {
		return table;
	}
	public void setTable(HTable table) {
		this.table = table;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public void close(){
		try {
			this.table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
