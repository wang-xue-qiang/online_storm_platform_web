package com.zkh.trident.pvService;

import java.util.Map;

import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;

public class TridentDRPCclient {
	public static void main(String[] args) {
		DRPCClient client   = null;
		try {
	        @SuppressWarnings("rawtypes")
			Map config = Utils.readDefaultConfig();
	         client = new DRPCClient(config, "hadoop-senior.ibeifeng.com", 3772);// drpc
			while (true) {				
/*				System.err.println("销售额结果=============>" + client.execute("getOrderAmt",
						"2018-10-15:1 2018-10-15:2 2018-10-15:3 2018-10-15:4 2018-10-15:5 2018-10-15:6 2018-10-15:7"));
				System.err.println("订单数结果=============>" + client.execute("getOrderNumber",
						"2018-10-15:1 2018-10-15:2 2018-10-15:3 2018-10-15:4 2018-10-15:5 2018-10-15:6 2018-10-15:7 2018-10-15:8"));*/
				
				System.err.println("销售额结果=============>"
	        			+client.execute("getOrderAmt", "2018-10-15:cf:amt_1 2018-10-15:cf:amt_2 2018-10-15:cf:amt_3"));
	        	System.err.println("订单数结果=============>"
	        			+client.execute("getOrderNumber", "2018-10-15:cf:orderNum_1 2018-10-15:cf:orderNum_2 2018-10-15:cf:orderNum_3"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
