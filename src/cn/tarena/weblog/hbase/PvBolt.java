package cn.tarena.weblog.hbase;

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
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		try {
			int pv = 1;
			List<Object> values = input.getValues(); 
			values.add(pv);
			this.collector.emit(input,values);
			this.collector.ack(input);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			this.collector.fail(input);
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
		declarer.declare(new Fields("url","urlname","uvid","ssid","sscount","sstime","cip","pv"));
	}

}
