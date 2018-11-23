package cn.tarena.tick;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import cn.tarena.dao.HBaseDao;
import cn.tarena.pojo.FluxInfo;

public class AvgTime extends BaseRichBolt {
	private OutputCollector collector;

	@Override
	public void execute(Tuple input) {
		try {
			long endTime = input.getLongByField("endtime");
			long starTime = endTime-1000*60*300;
			String regex ="^.*$";
			List<FluxInfo> result = HBaseDao.queryBYRange(String.valueOf(starTime), String.valueOf(endTime), regex);
			Map<String,List<Long>> vvMap = new HashMap();
			for(FluxInfo f : result){
				String ssid = f.getSsid();
				if(vvMap.containsKey(ssid)){
					vvMap.get(ssid).add(Long.parseLong(f.getSstime()));
				}else{
					List<Long> ssTimelist = new ArrayList();
					ssTimelist.add(Long.parseLong(f.getSstime()));
					vvMap.put(ssid, ssTimelist);
				}
			}
			int vv=vvMap.size();
			long totalTime=0l;
			
			for(Entry<String, List<Long>> entry:vvMap.entrySet()){
				//--获取了每个会话的所有时间戳
				List<Long> ssTimeList=entry.getValue();
				//--要获取当前这个会话的最大时间戳和最小时间戳
				long maxTime=Long.MIN_VALUE;
				long minTime=Long.MAX_VALUE;
				for(Long sstime:ssTimeList){
					if(maxTime<sstime){
						maxTime=sstime;
					}
					if(minTime>sstime){
						minTime=sstime;
					}
				}
				//--将每个会话的时长累加求和
				totalTime=totalTime+(maxTime-minTime);
			}
				double avgTime=0.0;
				if(vv!=0){
					avgTime=Math.round((totalTime*1.0)/(vv*1.0)) ;
				}
				List<Object> values=input.getValues();
				values.add(avgTime);
				collector.emit(values);
		} catch (IOException e) {
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
		declarer.declare(new Fields("endtime","br","avgdeep","avgtime"));
	}

}
