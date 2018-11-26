package com.zkh.servlet;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

import com.zkh.bean.AreaVo;
import com.zkh.dao.HbaseDao;
import com.zkh.dao.HbaseDaoImpl;
import com.zkh.utils.DateUtils;



public class AreaServlet extends HttpServlet{
	private static final long serialVersionUID = 1L;
	HbaseDao hbaseDao = null;
	String today,hisDay,hisData=null;
	SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	@Override
	public void init() throws ServletException {
		hbaseDao = new HbaseDaoImpl();
		today = formatter.format(new Date());//今天实时数据
	}
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		doPost(req, resp);
	}
	
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		hisDay = DateUtils.getDate(null, "yyyy-MM-dd", -1);
		hisData = this.getData(hisDay, hbaseDao);//昨天数据
		while(true){
			String data = this.getData(today, hbaseDao);
			String jsDataStr = "{\'todayData\':"+data+",\'hisData\':"+hisData+"}";
			boolean flag = this.sendData("jsFun", resp, jsDataStr);
			if(!flag){	
				break;
			}
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}
	
	@SuppressWarnings("deprecation")
	public String getData(String date,HbaseDao hbaseDao){
		AreaVo vo = new AreaVo();
		List<Result> list = hbaseDao.getRows("area_order", date);
		for (Result rs : list) {
			for(KeyValue kv:rs.raw()){
				if("order_amt".equals(new String(kv.getQualifier()))){
					String rowKey = new String(kv.getRow());
					String amt = new String(kv.getValue());
					if(rowKey.split("_").length==2){
						String areaId = rowKey.split("_")[1];
						vo.setData(areaId, amt);
						break;
					}
				}
			}
		}
		String rstl ="["+getFmtpoint(vo.getBeijing())+","+getFmtpoint(vo.getShanghai())+","
						+getFmtpoint(vo.getGuangzhou())+","+getFmtpoint(vo.getShenzhen())+","
						+getFmtpoint(vo.getZhengzhou())+"]"; 
		return rstl;
	}
	public static String getFmtpoint(String str){
		DecimalFormat format = new DecimalFormat("#.###");
		if(null !=str){
			return format.format(Double.parseDouble(str));
		}
		return null;
	}
	public boolean sendData(String jsFun, HttpServletResponse resp,String data){
		try {
			resp.setContentType("text/html;charset=utf-8");
			resp.getWriter().write("<script type=\"text/javascript\">parent."+jsFun+"(\""+data+"\")</script>");
			resp.flushBuffer();
			return true;
		} catch (Exception e) {
			System.out.println("长连中断!!!");
			return false;
		}
	}
}
