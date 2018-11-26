package com.zkh.trident.transaction;

import java.io.Serializable;

public class MyMata implements Serializable{
	private static final long serialVersionUID = 1L;
	private long beginpoint;//事务开始位置
	private int num;//batch的tuple个数
	public long getBeginpoint() {
		return beginpoint;
	}
	public void setBeginpoint(long beginpoint) {
		this.beginpoint = beginpoint;
	}
	public int getNum() {
		return num;
	}
	public void setNum(int num) {
		this.num = num;
	}
	@Override
	public String toString() {
		return "MyMata [beginpoint=" + beginpoint + ", num=" + num + "]";
	}
	

}
