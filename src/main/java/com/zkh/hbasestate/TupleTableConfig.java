package com.zkh.hbasestate;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TupleTableConfig implements Serializable{
	private static final long serialVersionUID = 1L;
	private String tableName;
	protected String tupleRowKeyField;
	protected String tupleTimestampField;
	protected Map<String,Set<String>> columnFamily;
	
	
	public TupleTableConfig(final String tableName) {	
		this.tableName = tableName;
		this.tupleTimestampField ="";
		this.columnFamily = new HashMap<String,Set<String>>();
	}
	public TupleTableConfig(final String tableName, final String rowKey, final String timestamp) {	
		this.tableName = tableName;
		this.tupleRowKeyField = rowKey;
		this.tupleTimestampField =timestamp;
		this.columnFamily = new HashMap<String,Set<String>>();
	}
	public TupleTableConfig(){}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getTupleRowKeyField() {
		return tupleRowKeyField;
	}
	public void setTupleRowKeyField(String tupleRowKeyField) {
		this.tupleRowKeyField = tupleRowKeyField;
	}
	public String getTupleTimestampField() {
		return tupleTimestampField;
	}
	public void setTupleTimestampField(String tupleTimestampField) {
		this.tupleTimestampField = tupleTimestampField;
	}
	public Map<String, Set<String>> getColumnFamily() {
		return columnFamily;
	}
	public void setColumnFamily(Map<String, Set<String>> columnFamily) {
		this.columnFamily = columnFamily;
	}
	
}
