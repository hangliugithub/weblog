package cn.tarena.weblog.hbase;

import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class VvBolt extends BaseRichBolt {

	private OutputCollector collector;
	@Override
	public void execute(Tuple input) {

		try {
			//--实现思路 根据的ssid去HBase 查询
			//--如果list.size =0 则是vv=1 繁殖vv=0
			//--实现思路 sscount=0
			//--实现思路①：根据用户的ssid去HBase查询，返回List<FluxInfo>
			//--如果list.size=0,则是vv=1;反之vv=0
			//--实现思路②：sscount=0
			String sscount=input.getStringByField("sscount");
			int vv=sscount.equals("0")?1:0;
			List<Object> values=input.getValues();
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
		this.collector = collector;

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("url","urlname","uvid","ssid","sscount","sstime","cip","pv","uv","vv"));
	}

}
