package cn.tarena.tick;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import cn.tarena.weblog.PrintBolt;

public class TIckTopology {

	public static void main(String[] args) {
		Config conf = new Config();
		TickBolt tickBolt = new TickBolt();
		
		BrBolt brBolt = new BrBolt();
		AvgDeep avgDeep = new AvgDeep();
		AvgTime avgTime=new AvgTime();
		TickToMysql tickToMysql=new TickToMysql();
		PrintBolt printBolt = new PrintBolt();
		TickTimeBolt timeBolt = new TickTimeBolt();
		
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setBolt("timeBolt", timeBolt);
		builder.setBolt("brBolt", brBolt).globalGrouping("timeBolt");
		builder.setBolt("avgDeep", avgDeep).globalGrouping("brBolt");
		builder.setBolt("avgTime", avgTime).globalGrouping("avgDeep");
		builder.setBolt("tickToMysql", tickToMysql).globalGrouping("avgTime");
		builder.setBolt("printBolt", printBolt).globalGrouping("avgTime");
		
		
		StormTopology topology = builder.createTopology();
		//集群提交
//		StormSubmitter cluster=new StormSubmitter();
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("ticktopology", conf, topology);
	}
}
