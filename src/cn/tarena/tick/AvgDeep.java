package cn.tarena.tick;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import cn.tarena.dao.HBaseDao;
import cn.tarena.pojo.FluxInfo;

public class AvgDeep extends BaseRichBolt {

	private OutputCollector collector;
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		try {
			long endTime = input.getLongByField("endtime");
			long starTime = endTime-1000*60*300;
			String regex ="^.*$";
			List<FluxInfo> result = HBaseDao.queryBYRange(String.valueOf(starTime), String.valueOf(endTime), regex);
			
			Map<String,Set<String>> vvMap = new HashMap();
			for(FluxInfo f:result){
				String ssid = f.getSsid();
				if(vvMap.containsKey(ssid)){
					vvMap.get(ssid).add(f.getUrl());
				}else{
					Set<String> set=new HashSet<>();
					set.add(f.getUrl());
					vvMap.put(ssid, set);
				}
			}
			//--总的会话数
			int vv=vvMap.size();
			//--总的会话深度
			int totalDeep=0;
			
			for(Entry<String, Set<String>> entry:vvMap.entrySet()){
				//--把每个会话的访问深度累加到一起
				totalDeep=totalDeep+entry.getValue().size();
			}
			//--平均的会话访问深度
			double avgDeep=0.0;
			if(vv!=0){
				
				avgDeep=(totalDeep*1.0)/(vv*1.0);
			}
			List<Object> values=input.getValues();
			values.add(avgDeep);
			collector.emit(values);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.collector = collector;
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("endtime","br","avgdeep"));
	}

}
