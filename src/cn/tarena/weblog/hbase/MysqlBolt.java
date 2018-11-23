package cn.tarena.weblog.hbase;

import java.sql.Date;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cn.tarena.pojo.Tongji;
import cn.tarena.weblog.util.MysqlDao;

public class MysqlBolt extends BaseRichBolt {
	
	private OutputCollector collector;
	@Override
	public void execute(Tuple input) {
		try {
		int pv = input.getIntegerByField("pv");
		int uv = input.getIntegerByField("uv");
		int vv = input.getIntegerByField("vv");
		int newIp = input.getIntegerByField("newip");
		int newCust = input.getIntegerByField("newcust");
		String time = input.getStringByField("sstime");
		
		Tongji t= new Tongji();
		t.setSstime(new Date(Long.parseLong(time)));
		t.setPv(pv);
		t.setUv(uv);
		t.setVv(vv);
		t.setNewIp(newIp);
		t.setNewCust(newCust);
		MysqlDao.saveToMysql(t);
		collector.ack(input);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException();
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		

	}

}
