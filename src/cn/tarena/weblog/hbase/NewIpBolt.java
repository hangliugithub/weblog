package cn.tarena.weblog.hbase;

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

public class NewIpBolt extends BaseRichBolt{
	private OutputCollector collector;
	@Override
	public void execute(Tuple input) {
		
		try {
			String cip = input.getStringByField("cip");
			//--接下来，拿着用户的cip去HBase 整表查询
			//--查询历史数据，是否出现过当前的cip.需要用到列值过滤器
			
			String columnFamily = "cf1";
			String columnName = "cip";
			String columnValue = cip;
			List<FluxInfo> result = HBaseDao.queryByColum(columnFamily,columnName,cip);
			int newIp = result.size()==0?1:0;
			List<Object> values = input.getValues();
			values.add(newIp);
			this.collector.emit(input,values);
			this.collector.ack(input);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			this.collector.ack(input);
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("url","urlname","uvid","ssid","sscount","sstime","cip","pv","uv","vv","newip"));
	}

}
