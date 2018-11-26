package com.zkh.uv;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import com.zkh.dao.HbaseDao;
import com.zkh.dao.HbaseDaoImpl;
import com.zkh.utils.DateUtils;

public class UVRstlBlot extends BaseBasicBolt{
	private static final long serialVersionUID = 1L;
	Map<String,Long>  uvMap = new HashMap<>();
	String todayStr = null;
	long beginTime = System.currentTimeMillis();
	long endTime ;
	HbaseDao dao;
	int  hour =0;
	long hour_uv = 0;
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		super.prepare(stormConf, context);
		todayStr = DateUtils.getToday(null);
		dao = new HbaseDaoImpl();
		Calendar c = Calendar.getInstance();
		hour = c.get(Calendar.HOUR_OF_DAY);
	}
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		System.err.println(".............................................................."+uvMap.get(todayStr));
		try {
			if(input != null){
				String key = input.getString(0);
				String dateStr = key.split("_")[0];
				//跨天处理
				if(todayStr != dateStr &&  todayStr.compareTo(dateStr)<0){
					todayStr= dateStr ;
					uvMap.clear();
				}
				//判断是否跨小时
				if(Calendar.getInstance().get(Calendar.HOUR_OF_DAY) != hour){
					hour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
					hour_uv = 0;
				}
				Long uvCnt = uvMap.get(dateStr);
				if(uvCnt == null){
					uvCnt =0l;
				}
				uvCnt++;
				hour_uv ++;
				uvMap.put(dateStr, uvCnt);
				endTime = System.currentTimeMillis();
				if(endTime-beginTime >= 5000){
					//定时写库
					String[] arr = this.getXValueStr();
					//保存历史点，为了取月环比 每分钟写一次
					dao.insert("uv", DateUtils.getToday("yyyyMMddHHmm"), "cf", new String[]{"time_title","xValue","uv"}, new String[]{arr[0],arr[1],""+uvMap.get(todayStr)});
					//用于实时刷新
					dao.insert("uv", todayStr+"_lastest", "cf", new String[]{"time_title","xValue","uv"}, new String[]{arr[0],arr[1],""+uvMap.get(todayStr)});
					//小时柱子数据
					dao.insert("uv", todayStr+"_hour_"+hour, "cf", new String[]{"uv"}, new String[]{""+hour_uv});
					beginTime = System.currentTimeMillis();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("date_sessionId"));
	}

	//获取x轴
	public  String[] getXValueStr(){
		Calendar c = Calendar.getInstance();
		int hour = c.get(Calendar.HOUR_OF_DAY);
		int minute = c.get(Calendar.MINUTE);
		int sec = c.get(Calendar.SECOND);
		int curSecNum = hour*3600+ minute*60+sec;
		Double xValue = (double)curSecNum/3600;
		String[] end = {hour+":"+minute,xValue.toString()};
		return end;
	}

}
