package cn.tarena.weblog;

import java.io.IOException;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cn.tarena.dao.HBaseDao;
import cn.tarena.pojo.FluxInfo;

public class HBaseBolt extends BaseRichBolt {

	private OutputCollector collector;
	@Override
	public void execute(Tuple input) {
		try {
			FluxInfo f = new FluxInfo();
			f.setUrl(input.getStringByField("url"));
			f.setUrlName(input.getStringByField("urlname"));
			f.setUvid(input.getStringByField("uvid"));
			f.setSsid(input.getStringByField("ssid"));
			f.setSscount(input.getStringByField("sscount"));
			f.setSstime(input.getStringByField("sstime"));
			f.setCip(input.getStringByField("cip"));

		//--将用户的访问的一条数据插入到HBase表中
			HBaseDao.saveToHBase(f);
			collector.ack(input);
		} catch (IOException e) {
			e.printStackTrace();
			collector.fail(input);
		}
		
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.collector = collector;

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub

	}

}
