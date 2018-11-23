package cn.tarena.tick;

import java.io.IOException;
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

public class BrBolt extends BaseRichBolt {

	private OutputCollector collector;
	
	@Override
	public void execute(Tuple input) {
		try {
			//--获取HBase表的终止行键范围
			long endtime = input.getLongByField("endTime");
			//--获取扫描的起始范围，向前追溯15分钟
			long startTime = endtime-1000*60*15;
			//--匹配所有数据
			String regex ="^.*$";
			
			List<FluxInfo> result = HBaseDao.queryBYRange
					(String.valueOf(startTime), 
					String.valueOf(endtime), regex);
			//--计算15分钟之内的BR=跳出会话数/总的会话数
			Map<String,Integer> vvMap = new HashMap();
			for(FluxInfo f : result){
				String ssid = f.getSsid();
				if(vvMap.containsKey(ssid)){
					vvMap.put(ssid, vvMap.get(ssid)+1);
				}else{
					vvMap.put(ssid, 1);
				}
			}
			//获取总的会话数
			int vv = vvMap.size();
			//--Map(001->1,002->2,003->1,004->4)
			int brCount=0;
			for(Entry<String,Integer> entry:vvMap.entrySet()){
				
				if(entry.getValue()==1)brCount++;
			}
			double br =0; 
			if(vv!=0)br=(brCount*1.0)/(vv*1.0);
			List<Object> values = input.getValues();
			values.add(br);
			this.collector.emit(values);
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
		declarer.declare(new Fields("endtime","br"));
	}

}
