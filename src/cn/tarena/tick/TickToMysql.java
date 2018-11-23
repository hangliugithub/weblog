package cn.tarena.tick;

import java.sql.Date;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cn.tarena.pojo.TickTongji;
import cn.tarena.weblog.util.MysqlDao;

public class TickToMysql extends BaseRichBolt {
	
	@Override
	public void execute(Tuple input) {
		try {

			long endtime = input.getLongByField("endtime");
			double br = input.getDoubleByField("br");
			double avgDeep = input.getDoubleByField("avgdeep");
			double avgTime = input.getDoubleByField("avgtime");
			
			TickTongji t = new TickTongji();
			t.setBr(br);
			t.setAvgDeep(avgDeep);
			t.setAvgTime(avgTime);
			MysqlDao.tickToMysql(t);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub

	}

}
