package cn.tarena.tick;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
/**
 * 达到每个十秒处罚一次execute
 * @author Administrator
 *
 */
public class TickBolt extends BaseRichBolt {

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();	
		conf.put(conf.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
		return conf;
	}
	
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		System.out.println("bolt 1805");
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
