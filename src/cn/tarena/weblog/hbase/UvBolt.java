package cn.tarena.weblog.hbase;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import cn.tarena.dao.HBaseDao;
import cn.tarena.pojo.FluxInfo;

public class UvBolt extends BaseRichBolt {

	private OutputCollector collector;
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		//实现思路：当用户访问时，我们接受当前用户的uvid
		//--拿着当前的uvid区HBase 查询今天的所有数据
		//--2 判断uvid是否出现过，如果出现过，说明不是新用户
		//--如果没有出现过记 uv= 1
		//--3 灾区HBase 查询，或用到Hbase 行键过滤器
		//--行键：sstime_uvid_ssid 两位随机数
		
		//--把当前用户的访问的时间戳当作HBase表的终止范围
		try {
			String endtime = input.getStringByField("sstime");
			Calendar calendar = Calendar.getInstance();
			//--以用户的访问时间戳为基准，下一步找当天的0：00
			calendar.setTimeInMillis(Long.parseLong(endtime));
			calendar.set(Calendar.HOUR, 0);
			calendar.set(Calendar.MINUTE, 0);
			calendar.set(Calendar.SECOND, 0);
			calendar.set(Calendar.MILLISECOND, 0);
			
			//--获取当天0：00的时间戳
			String startTime = String.valueOf(calendar.getTimeInMillis());
			String uvid = input.getStringByField("uvid");
			//--sstime_uvid_ssid_两位随机数
			String regex = "^\\d{13}_"+uvid+"\\d{10}_"+"\\d{2}$";
			
			//去HBase表查数据
			List<FluxInfo> result = HBaseDao.queryBYRange(startTime,endtime,regex);
			int uv =result.size()==0?1:0;
			
			List<Object> values = input.getValues();
			values.add(uv);
			this.collector.emit(input,values);
			this.collector.ack(input);
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			this.collector.fail(input);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("url","urlname","uvid","ssid","sscount","sstime","cip","pv","uv"));

	}

}
