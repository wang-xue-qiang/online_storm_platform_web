<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<% 
String path = request.getContextPath(); 
// 获得本项目的地址(例如: http://localhost:8080/MyApp/)赋值给basePath
String basePath = request.getScheme()+"://"+request.getServerName()+":"+request.getServerPort()+path+"/"; 
// 将 "项目路径basePath" 放入pageContext中，待以后用EL表达式读出。 
pageContext.setAttribute("basePath",basePath); 
%>  
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>区域订单金额实时汇总</title>
<script src="https://img.hcharts.cn/jquery/jquery-1.8.3.min.js"></script>
<script src="https://img.hcharts.cn/highcharts/highcharts.js"></script>
<script src="https://img.hcharts.cn/highcharts/highcharts-3d.js"></script>
<script src="https://img.hcharts.cn/highcharts/modules/exporting.js"></script>
<script src="https://img.hcharts.cn/highcharts-plugins/highcharts-zh_CN.js"></script>
<script type="text/javascript">
var series1;
var series2;
function jsFun(m){
	var jsdata = eval("("+m+")");
	//alert(JSON.stringify(jsdata));
	series1.setData(eval(jsdata.todayData));
	series2.setData(eval(jsdata.hisData));
}
function init(){
	$("#myFrom").attr("action","${pageScope.basePath}AreaServlet");
	$("#myFrom").submit();
}
</script>
</head>
<body>
<form action="" method="post" id="myFrom" target="myiframe"></form>
<iframe id="myiframe" name="myiframe" style="display:none"></iframe>
<div id="container" style="height: 600px;width: 100%"></div>
<div id="sliders">
	<table>
		<tr>
			<td>α 角（内旋转角）</td>
			<td>
				<input id="alpha" type="range" min="0" max="45" value="15"/> 
				<span id="alpha-value" class="value"></span>
			</td>
		</tr>
		<tr>
			<td>β 角（外旋转角）</td>
			<td>
				<input id="beta" type="range" min="-45" max="45" value="15"/> 
				<span id="beta-value" class="value"></span>
			</td>
		</tr>
		<tr>
			<td>深度</td>
			<td>
				<input id="depth" type="range" min="20" max="100" value="50"/> 
				<span id="depth-value" class="value"></span>
			</td>
		</tr>
	</table>
</div>
<script type="text/javascript">
var chart = new Highcharts.Chart({
	chart: {
		renderTo: 'container',
		type: 'column',//column
		events:{
			load : function(){
				series1 = this.series[0];
				series2 = this.series[1];
				init();
			}	
		},
		options3d: {
			enabled: true,
			alpha: 15,
			beta: 15,
			depth: 50,
			viewDistance: 25
		}
	},
	title: {
		text: '区域订单金额'
	},
	subtitle: {
		text: '按天统计'
	},
	plotOptions: {
		column: {
			dataLabels:{
				enabled:true
			}
		}
	},
	xAxis:{
		categories:['北京','上海','广州','深圳','郑州']
	},
	yAxis:{
		min: 0,
		labels:{overflow:'justify'},
	},
	series: [{
		name:'当前',
		color:'#41ABBE',
		//lengendIndex:2,
		//index:2,
		data: []
	},
	{
		name:'昨日',
		color:'#808080',
		//lengendIndex:1,
		//index:1,
		data: []
	}
	]
});
// 将当前角度信息同步到 DOM 中
var alphaValue = document.getElementById('alpha-value'),
	betaValue = document.getElementById('beta-value'),
	depthValue = document.getElementById('depth-value');
function showValues() {
	alphaValue.innerHTML = chart.options.chart.options3d.alpha;
	betaValue.innerHTML = chart.options.chart.options3d.beta;
	depthValue.innerHTML = chart.options.chart.options3d.depth;
}
// 监听 sliders 的变化并更新图表
$('#sliders input').on('input change', function () {
	chart.options.chart.options3d[this.id] = this.value;
	showValues();
	chart.redraw(false);
});
showValues();
</script>
</body>
</html>