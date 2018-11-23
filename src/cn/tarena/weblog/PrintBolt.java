package cn.tarena.weblog;

import java.util.Iterator;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class PrintBolt extends BaseRichBolt {
	private OutputCollector collector;
	@Override
	public void execute(Tuple input) {
		try {
			//获取tuple中的所有key字段
			Fields keys = input.getFields();
			//--获取key字段的迭代器
			Iterator<String> it = keys.iterator();
			while(it.hasNext()){
				String key = it.next();
				Object value = input.getValueByField(key);
				System.out.println(key+" : "+value);
			}
			collector.ack(input);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			collector.fail(input);
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub

	}

}
