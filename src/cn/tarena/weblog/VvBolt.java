package cn.tarena.weblog;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class VvBolt extends BaseRichBolt {
	private OutputCollector collector;
	private Set<String> vvSet = new HashSet();
 	@Override
	public void execute(Tuple input) {
 		try {
			String ssid= input.getStringByField("ssid");
			vvSet.add(ssid);
			int vv = vvSet.size();
			List<Object> values = input.getValues();
			values.add(vv);
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
		this.collector= collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url","urlname","uvid","ssid","sscount","sstime","cip","pv","uv","vv"));
	}

}
