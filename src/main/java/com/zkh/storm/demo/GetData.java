package com.zkh.storm.demo;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

public class GetData {
	public static void main(String[] args) throws IOException {
		File logFile = new File("track.log");
		Random random = new Random();
		String[] hosts ={"www.taobao.com"};
		String[] session_id={"ewfytdsdjwu121323","fgehdwegyuwtr232fe","gdfyutej1223fdfe","behfejhiue3435dfg","sfegf34325fdfgr"};
		String[] time ={"2018-10-10 10:10:10","2018-11-11 11:11:11","2018-01-01 01:01:01","2018-09-09 16:50:10","2018-07-10 12:16:30","2018-60-10 05:20:10"};
		StringBuilder sbBuilder = new StringBuilder();
		for (int i = 0; i < 50; i++) {
			sbBuilder.append(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(6)]+"\n");			
		}
		if(!logFile.exists()){
			logFile.createNewFile();
		}
		byte[] b =(sbBuilder.toString()).getBytes();
		FileOutputStream fs;
		try {
			fs = new FileOutputStream(logFile);
			fs.write(b);
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
