package cn.tarena.weblog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class UvBolt extends BaseRichBolt {
	private OutputCollector collector;
	private Map<String,String> uvMap = new HashMap();
	@Override
	public void execute(Tuple input) {
		try {
		String uvid = input.getStringByField("uvid");
		//--设计对uvid 的去重
		if(uvMap.containsKey(uvid)){
			//重复
		}else{
			uvMap.put(uvid, uvid);
		}
		int uv = uvMap.size();
		List<Object> values = input.getValues();
		values.add(uv);
		collector.emit(input,values);
		collector.ack(input);
		} catch (Exception e) {
			e.printStackTrace();
			collector.fail(input);
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	
		declarer.declare(new Fields("url","urlname","uvid","ssid","sscount","sstime","cip","pv","uv"));

	}

}
