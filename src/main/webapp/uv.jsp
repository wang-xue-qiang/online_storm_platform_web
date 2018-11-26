<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<% 
String path = request.getContextPath(); 
// 获得本项目的地址(例如: http://localhost:8080/MyApp/)赋值给basePath
String basePath = request.getScheme()+"://"+request.getServerName()+":"+request.getServerPort()+path+"/"; 
// 将 "项目路径basePath" 放入pageContext中，待以后用EL表达式读出。 
pageContext.setAttribute("basePath",basePath); 
%>  
<!DOCTYPE HTML>
<html>
<head>
<meta charset="utf-8">
<link rel="icon" href="https://static.jianshukeji.com/highcharts/images/favicon.ico">
<meta name="viewport" content="width=device-width, initial-scale=1">	<meta name="description" content="">
<title></title>
<script src="${pageScope.basePath}js/jquery/jquery-2.1.1.min.js"></script>
<script src="https://img.hcharts.cn/highcharts/highcharts.js"></script>
<script src="https://img.hcharts.cn/highcharts/modules/exporting.js"></script>
<script src="https://img.hcharts.cn/highcharts-plugins/highcharts-zh_CN.js"></script>
<script src="${pageScope.basePath}js/highcharts/skies.js"></script>
</head>
<body>
<form action="" method="post" id="myFrom" target="myiframe"></form>
<iframe id="myiframe" name="myiframe" style="display:none"></iframe>
<div id="container" style="height: 600px;width: 100%"></div>
<script type="text/javascript">
var chart;
var series;
var series1;
var series2;
var initFlag = true;
var lst_x =0;
var xTitleShow ="";
var isShowTip=false;//用于控制只在addpoint的时候才对tip框进行操作
function msg(m){
	showFrameData(m); 
}
function initShow(){
	$("#myFrom").attr("action","${pageScope.basePath}uvServlet");
	$("#myFrom").submit();
}
//展示数据
function showFrameData(m){
	var jsonData  = eval("("+m+")");//转换json对象
	//alert(JSON.stringify(jsonData));
	//console.log(m);
	//跨天处理
	var isNewDay = jsonData.isNewDay;
	if(isNewDay == 1){
		debugger;
		alert("跨天了");
		initFlag = true;
		//清空
		series.remove(false);
		series1.remove(false);
		chart.redraw();
		//重绘
		chart.addSeries({
			id:1,
			name:"当前UV",
			color:'#FF000',
			type: 'spline',
			data: [[0,0]]
		},false);
		chart.addSeries({
			id:2,
			name:"上月同期UV",
			color:'#00B050',
			type: 'spline',
			data: [[0,0]]
		},false);
		var seriestt = chart.series;
		series = seriestt[0];
		series1 = seriestt[1];
		series2 = seriestt[2];
		//跨天之后每3秒刷新一次页面，为的是使dataLables生效
		setInterval(function(){window.location.reload();},3000);
	}
	if(initFlag){
		series.addPoint([0,0],true, false);
		series1.addPoint([0,0],true, false);
		series.setData(eval(jsonData.initData));
		series1.setData(eval(jsonData.initHisData));
		series2.setData(eval(jsonData.initHourData));
		initFlag = false;
	}else{
		var option_ = {'name':jsonData.xname,'x':jsonData.x,'y':jsonData.y};
		lst_x = jsonData.x;
		xTitleShow = jsonData.xname;
		series.addPoint(option_ ,true, false);
		series2.setData(eval(jsonData.initHourData));
		var isShowTip= true;
	}
}
$(function(){
	chart = new Highcharts.Chart({
		chart: {
			renderTo: 'container',
			events:{
				load : function(){
					series = this.series[0];
					series1 = this.series[1];
					series2 = this.series[2];
					initShow();
				}	
			}
		},
		title: {
				text: '实时UV统计'
		},
		xAxis: {
				max: 24,
				categories:['00','01','02','03',"04",'05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24'],
		}, 
		yAxis: [
		        {
				min:0,
				labels:{enabled:true,}
				}
				
		],
		tooltip: {
				valueDecimals:0,
		},
 		plotOptions: {
			series:{
				turboThreshold:0,
			},
			spline:{
				lineWidth: 1,
				marker:{
					enabled :true
				},
				/* dataLabels:{
					enabled:true
				} */
			},
			column:{
				lineWidth: 100,
				marker:{
					enabled :true
				},
				dataLabels:{
					enabled:true
				}
			}
		}, 
		series: [{
				name: '当前UV',
				color:"FF0000",
				legendIndex:2,
				type:'spline',
				cursor:'pointer',
				dataLabels:{
					enabled:true,//开启数据标签
					align:'left',
					borderRadius:1,
					borderWidth:1,
					borderColor:'#AAA',
					shadow:true,
					style:{fontWeight:'bold'},
					useHTML:true,
					formatter:function(){
						if(lst_x == this.point.x&&isShowTip){
							var showTipValue = (this.point.y).toFixed(0);//去小数点
							showTipValue = commafy(showTipValue);//转千分位如 123,4560
							return "<div id='tmplst'><b>时间:"+xTitleShow+"</b><br>UV:"+showTipValue+"</div>";
						}
					}
				},
				data: [],
				tooltip:{}
		},{
			name: '上月同期UV',
			color:"#00B050",
			legendIndex:1,
			type:'spline',
			data: [],
			tooltip:{}
		},{
			name: '小时分布',
			color:"blue",
			legendIndex:3,
			type:'column',
			data: [],
			tooltip:{}
		}
		
		]
	});
});

function commafy(num){
	//先去除空格，判断空值和非数
	num = num +"";
	num = num.replace(/[ ]/g,"");//去除空格
	if(isNaN(num)){return;}
	//是否含有小数点
	var index = num.indexOf(".");
	if(index == -1){
		var reg =/(-?\d+)(\d{3})/;
		while(reg.test(num)){
			num = num.replace(reg,"$1,$2");
		}
	}else{
		var initPart = num.substring(0,index);
		var pointPart = num.substring(idex+1,num.length);
		var reg =/(-?\d+)(\d{3})/;
		while(reg.test(initPart)){
			initPart = initPart.replace(reg,"$1,$2");
		}
		num =initPart+"."+pointPart;
	}
	return num;
}
</script>











</body>
</html>