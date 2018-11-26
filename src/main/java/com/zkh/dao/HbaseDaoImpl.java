package com.zkh.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import com.zkh.dao.HbaseDao;

public class HbaseDaoImpl implements HbaseDao{
	HConnection hTablePool = null;
	public HbaseDaoImpl() {
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "hadoop-senior.ibeifeng.com:2181/");
		try {
			hTablePool = HConnectionManager.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void save(Put put, String tableName) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName);
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void insert(String tableName, String rowKey, String family,
			String quailifer, String value) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName);
			Put put = new Put(rowKey.getBytes());
			put.add(family.getBytes(), quailifer.getBytes(), value.getBytes());
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	@Override
	public void save(List<Put> puts, String tableName) {
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName);
			table.put(puts);
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	@Override
	public Result getOneRow(String tableName, String rowKey) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		Result result = null;
		try {
			table = hTablePool.getTable(tableName);
			Get get = new Get(rowKey.getBytes());
			result= table.get(get);
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return result;
	}

	@Override
	public List<Result> getRows(String tableName, String rowKey_like) {
		HTableInterface table = null;
		List<Result> result = null;
		try {
			table = hTablePool.getTable(tableName);
			PrefixFilter filter = new PrefixFilter(rowKey_like.getBytes());
			Scan scan = new Scan();
			scan.setFilter(filter);
			ResultScanner scanner =table.getScanner(scan);
			result = new ArrayList<Result>();
			for (Result rs : scanner) {
				result.add(rs);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return result;
	}

	@Override
	public List<Result> getRows(String tableName, String rowKey_like,String cols[]) {
		HTableInterface table = null;
		List<Result> result = null;
		try {
			table = hTablePool.getTable(tableName);
			PrefixFilter filter = new PrefixFilter(rowKey_like.getBytes());
			Scan scan = new Scan();
			scan.setFilter(filter);
			for (int i=0;i<cols.length;i++) {
				scan.addColumn("cf".getBytes(), cols[i].getBytes());
			}
			ResultScanner scanner =table.getScanner(scan);
			result = new ArrayList<Result>();
			for (Result rs : scanner) {
				result.add(rs);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return result;
	}
	
	@Override
	public List<Result> getRows(String tableName, String startRow,String stopRow) {
		HTableInterface table = null;
		List<Result> result = null;
		try {
			table = hTablePool.getTable(tableName);
			Scan scan = new Scan();
			scan.setStartRow(startRow.getBytes());
			scan.setStopRow(stopRow.getBytes());
			ResultScanner scanner =table.getScanner(scan);
			result = new ArrayList<Result>();
			for (Result rs : scanner) {
				result.add(rs);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return result;
	}

	@Override
	public void insert(String tableName, String rowKey, String family, String[] quailifer, String[] value) {
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName);
			Put put = new Put(rowKey.getBytes());
			for (int i = 0; i < quailifer.length; i++) {
				String col = quailifer[i];
				String val = value[i];
				put.add(family.getBytes(), col.getBytes(), val.getBytes());			
			}
			table.put(put);
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	@SuppressWarnings("deprecation")
	public static void main(String[] args){
		HbaseDao dao = new HbaseDaoImpl();
/*		List<Put> puts = new ArrayList<Put>();
		Put put = new Put("cloudy".getBytes());
		put.add("cf".getBytes(), "name".getBytes(), "lijuanjuan".getBytes());
		puts.add(put);
		dao.save(puts, "test");*/
		//dao.insert("test", "testrow", "cf","tel", "123456");
/*		Result rs = dao.getOneRow("test","testrow");
		for(KeyValue kv:rs.raw()){
			System.out.println("rowkey:"+new String(kv.getRow()));
			System.out.println("属性："+new String(kv.getQualifier()));
			System.out.println("值"+new String(kv.getValue()));
		}*/
		//List<Result> list =dao.getRows("uv", "2018-10-16_lastest",new String[]{"time_title","xValue","uv"});
		//List<Result> list =dao.getRows("test", "cloudy","test");
		List<Result> list =dao.getRows("uv", "2018-10-18_hour");
		String[] rstlArr = {"0","0","0","0","0","0","0", "0","0","0","0","0","0","0", "0","0","0","0","0","0","0", "0","0","0","0","0","0","0"};
		String rstl ="[";
		for (Result rs : list) {
			for(KeyValue kv:rs.raw()){
				System.out.println("rowkey:"+new String(kv.getRow()));
				System.out.println("属性："+new String(kv.getQualifier()));
				System.out.println("值"+new String(kv.getValue()));
				String index = new String(kv.getRow()).split("_hour_")[1];
				int _index = Integer.parseInt(index);
				rstlArr[_index] = new String(kv.getValue());
			}
		}
		for (String str : rstlArr) {
			rstl += str+",";
			
		}
		rstl =rstl.substring(0, rstl.length()-1)+"]";
		System.out.println(rstl);
	}
}
