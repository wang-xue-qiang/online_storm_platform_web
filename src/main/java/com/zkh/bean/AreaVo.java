package com.zkh.bean;

public class AreaVo {
	private String beijing;
	private String shanghai;
	private String guangzhou;
	private String shenzhen;
	private String zhengzhou;
	public String getBeijing() {
		return beijing;
	}
	public void setBeijing(String beijing) {
		this.beijing = beijing;
	}
	public String getShanghai() {
		return shanghai;
	}
	public void setShanghai(String shanghai) {
		this.shanghai = shanghai;
	}
	public String getGuangzhou() {
		return guangzhou;
	}
	public void setGuangzhou(String guangzhou) {
		this.guangzhou = guangzhou;
	}
	public String getShenzhen() {
		return shenzhen;
	}
	public void setShenzhen(String shenzhen) {
		this.shenzhen = shenzhen;
	}
	public String getZhengzhou() {
		return zhengzhou;
	}
	public void setZhengzhou(String zhengzhou) {
		this.zhengzhou = zhengzhou;
	}
	public void setData(String areaId,String amt){
		if("1".equals(areaId)){this.setBeijing(amt);}
		else if("2".equals(areaId)){this.setShanghai(amt);}
		else if("3".equals(areaId)){this.setGuangzhou(amt);}
		else if("4".equals(areaId)){this.setShenzhen(amt);}
		else if("5".equals(areaId)){this.setZhengzhou(amt);}
	}
}
