package cn.tarena.tick;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 用于控制伪实时查询
 * @author Administrator
 *
 */
public class TickTimeBolt extends BaseRichBolt{

	private OutputCollector collector;
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.put(conf.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
		return conf;
	}
	@Override
	public void execute(Tuple arg0) {

		long endtime = System.currentTimeMillis();
		collector.emit(new Values(endtime));
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {

		this.collector = collector;
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("endtime"));
	}
	
}
