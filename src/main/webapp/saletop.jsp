<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title></title>
<script src="https://img.hcharts.cn/highcharts/highcharts.js"></script>
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
	$("#myFrom").attr("action","http://localhost:8083/storm/saleServlet");
	$("#myFrom").submit();
}
</script>
</head>
<body>
<form action="" method="post" id="myFrom" target="myiframe"></form>
<iframe id="myiframe" name="myiframe" style="display:none"></iframe>
<div id="container" style="height: 600px;width: 100%"></div>
<script type="text/javascript">
var chart = Highcharts.chart('container', {
    chart: {
        zoomType: 'xy',
        events:{
			load : function(){
				series1 = this.series[0];
				series2 = this.series[1];
				init();
			}	
		},
    },
    title: {
        text: '区域订单金额和订单数量统计'
    },
    subtitle: {
        text: ''
    },
	plotOptions: {
		column: {
			dataLabels:{
				enabled:true
			}
		}
	},
    xAxis: [{
    	categories:['北京','上海','广州','深圳','郑州'],
        crosshair: true
    }],
    yAxis: [{ // Primary yAxis
        labels: {
            format: '{value}',
            style: {
                color: Highcharts.getOptions().colors[1]
            }
        },
        title: {
            text: '订单数',
            style: {
                color: Highcharts.getOptions().colors[1]
            }
        }
    }, { // Secondary yAxis
        title: {
            text: '销售额',
            style: {
                color: Highcharts.getOptions().colors[0]
            }
        },
        labels: {
            format: '{value}',
            style: {
                color: Highcharts.getOptions().colors[0]
            }
        },
        opposite: true
    }],
    tooltip: {
        shared: true
    },
    legend: {
        layout: 'vertical',
        align: 'left',
        x: 120,
        verticalAlign: 'top',
        y: 100,
        floating: true,
        backgroundColor: (Highcharts.theme && Highcharts.theme.legendBackgroundColor) || '#FFFFFF'
    },
    series: [{
        name: '销售额',
        type: 'column',
        yAxis: 1,
        data: [],
        tooltip: {
            valueSuffix: ' 元'
        }
    }, {
        name: '订单数',
        type: 'spline',
        data: [],
        tooltip: {
            valueSuffix: '个'
        }
    }]
});
</script>
</body>
</html>