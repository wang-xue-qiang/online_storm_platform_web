package com.zkh.servlet;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import com.zkh.dao.HbaseDao;
import com.zkh.dao.HbaseDaoImpl;
import com.zkh.utils.DateUtils;
@SuppressWarnings("unused")
public class UvServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
    public UvServlet() {
        super();
        
    }
    private HbaseDao hbaseDao = null; 
    private static String initData = "[]";
    private static String initHisData = "[]";
    private static String initHourData = "[]";
    private String jsonStr = "";
    private String todayStr = null;
	private String xValue = "0";
    private String xTitle = "0";
    private String uv ="0";
    private String rowkey_uv_last ="";
    String[] colsArr = new String[]{"time_title","xValue","uv"};
	SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	@Override
	public void init() throws ServletException {
		hbaseDao = new HbaseDaoImpl();
		todayStr = formatter.format(new Date());
		//rowkey_uv_last ="last_tracker_uv_"+todayStr;
		initStrData();
	}
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doPost(request, response);
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		try{
			//初始化数据
			initHourData = getData(todayStr+"_hour", hbaseDao);
			response.setCharacterEncoding("utf-8");
			response.setContentType("text/html;charset=utf-8");
			initStrData();
			jsonStr ="{\'initData\':"+initData+ ",\'initHisData\':"+initHisData+",\'initHourData\':'"+initHourData+"'}";
			response.getWriter().write("<script type=\"text/javascript\">parent.msg"+"(\""+jsonStr+"\")</script>");
			response.flushBuffer();
			while(true){
				if(!DateUtils.getToday(null).equals(todayStr)){
					init();
					jsonStr ="{\'initData\':"+initData+",\'initHisData\':"+initHisData+",\'isNewday\':"+1+",\'initHourData\':'"+initHourData+"'}";
					response.setCharacterEncoding("utf-8");
					response.setContentType("text/html;charset=utf-8");
					response.getWriter().write("<script type=\"text/javascript\">parent.msg"+"(\""+jsonStr+"\")</script>");
					response.flushBuffer();
				}
				String[] valueArr = null;
				valueArr = findLineData(hbaseDao,"uv",todayStr+"_lastest",colsArr);
				if(valueArr != null){
					xTitle =valueArr[0];
					xValue =valueArr[1]; 
					uv = valueArr[2];
				}
				initData = appendStr(initData,colsArr);
				jsonStr ="{\'xname\':\'"+xTitle+"\',\'x\':"+xValue+",\'y\':"+uv+",\'initHourData\':'"+initHourData+"'}";
					
				if(uv != null){
					boolean b = sendMsg(jsonStr,response,"msg");
					if(!b){break;}
				}
				Thread.sleep(5000);
				initHourData = getData(todayStr+"_hour", hbaseDao);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	@SuppressWarnings("deprecation")
	private String[] findLineData(HbaseDao hbaseDao, String table, String rowkey, String[] colsArr) {
		List<Result> list =hbaseDao.getRows(table, rowkey,colsArr);
		String[] valueArr =new String[3];
		for (Result r : list) {
			for(KeyValue keyValue:r.raw()){
				if(new String(keyValue.getQualifier()).equals(colsArr[0])){
					valueArr[0]= new String(keyValue.getValue());
				}
				if(new String(keyValue.getQualifier()).equals(colsArr[1])){
					valueArr[1]= new String(keyValue.getValue());
				}
				if(new String(keyValue.getQualifier()).equals(colsArr[2])){
					valueArr[2]= new String(keyValue.getValue());
				}	
			}
		}
		return valueArr;
	}
	public boolean sendMsg(String data, HttpServletResponse resp,String jsFun){
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
	private void initStrData(){
		try{
			String rowkey_id = todayStr.replace("-", "");
			String rowkey_yc =DateUtils.getDate(null, "yyyy-MM-dd", -1).replace("-", "");
			/**读取图历史数据**/
			List<Result> listToday = hbaseDao.getRows("uv",rowkey_id,colsArr);
			initData= transformHistoryData(listToday,colsArr);
			/**读取预测数据**/
			List<Result> listYc = hbaseDao.getRows("uv",rowkey_yc,colsArr);
			initHisData= transformHistoryData(listYc,colsArr);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	
	@SuppressWarnings({ "deprecation" })
	public  String transformHistoryData(List<Result> listToday ,String[] colsArr){
		int listTodaySize = listToday.size();
		StringBuffer data = new StringBuffer();
		data.append("\'[");
		int oNum =0;
		for(Result r :listToday){
			oNum++;
			String p_title=null;
			String xValue=null;
			String yValue=null;
			for(KeyValue keyValue:r.raw()){
				if(new String(keyValue.getQualifier()).equals(colsArr[0])){
					p_title= new String(keyValue.getValue());
				}
				if(new String(keyValue.getQualifier()).equals(colsArr[1])){
					xValue= new String(keyValue.getValue());
				}
				if(new String(keyValue.getQualifier()).equals(colsArr[2])){
					yValue= new String(keyValue.getValue());
				}		
			}
			if(oNum !=1){
				data.append(",");
			}
			data.append(getOnePointJson(p_title,xValue,yValue));
		}	
		data.append("]\'");
		return data.toString();	
	}
	
	private  String getOnePointJson(String pt, String xValue, String yValue) {
		StringBuffer data = new StringBuffer();
		data.append("\\{");
		data.append("name:\\\\'" + pt  + "\\\\'" );
		data.append(",x:" + xValue );
		data.append(",y:" + yValue );
		data.append("\\}" );
		return data.toString();
	}
	
	private String appendStr(String sBuffer,String[] arr){
		if(sBuffer.toString().equals("'[]'")){
			sBuffer = sBuffer.substring(0, sBuffer.length()-2);
			return sBuffer +getOnePointJson(arr[0],arr[1],arr[2])+"]\'";
		}else{
			sBuffer = sBuffer.substring(0, sBuffer.length()-2);
			return sBuffer+"," +getOnePointJson(arr[0],arr[1],arr[2])+"]\'";
		}
	}
	
	@SuppressWarnings("deprecation")
	public String getData(String date,HbaseDao hbaseDao){
		String[] rstlArr = new String[25];
		List<Result> list = hbaseDao.getRows("uv", date);
		String rstl ="[";
		for (Result rs : list) {
			for(KeyValue kv:rs.raw()){
				String index = new String(kv.getRow()).split("_hour_")[1];
				int _index = Integer.parseInt(index);
				rstlArr[_index] = new String(kv.getValue());
			}
		}
		for (String str : rstlArr) {
			if(null ==str){str = "0";}
			rstl += (str+",");
			
		}
		rstl =rstl.substring(0, rstl.length()-1)+"]";
		return rstl;
	}
}
