package cn.tarena.weblog;

import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class PvBolt extends BaseRichBolt {
	
	private OutputCollector collector;
	private int pv;
	@Override
	public void execute(Tuple input) {
		try {
			pv++;
			List<Object> values = input.getValues();
			values.add(pv);
			collector.emit(input,values);
			collector.ack(input);
		} catch (Exception e) {
			e.printStackTrace();
			this.collector.fail(input);
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url","urlname","uvid","ssid","sscount","sstime","cip","pv"));

	}

}
