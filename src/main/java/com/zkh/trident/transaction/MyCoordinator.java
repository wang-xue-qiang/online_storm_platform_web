package com.zkh.trident.transaction;

import java.math.BigInteger;

import org.apache.storm.transactional.ITransactionalSpout.Coordinator;
import org.apache.storm.utils.Utils;

public class MyCoordinator implements Coordinator<MyMata>{
	public static int BATCH_NUM =10;
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public MyMata initializeTransaction(BigInteger txid, MyMata preMetedata) {
		long beginpoint =0;
		if(preMetedata == null){
			beginpoint = 0;
		}else{
			beginpoint = preMetedata.getBeginpoint()+preMetedata.getNum();
		}
		MyMata mata = new MyMata();
		mata.setBeginpoint(beginpoint);
		mata.setNum(BATCH_NUM);
		System.err.println("启动一个事物："+mata);
		return mata;
	}

	@Override
	public boolean isReady() {
		Utils.sleep(2000);
		return true;
	}

}
