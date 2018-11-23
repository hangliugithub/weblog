package cn.tarena.weblog.hbase;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import cn.tarena.weblog.ClearBolt;
import cn.tarena.weblog.HBaseBolt;
import cn.tarena.weblog.PrintBolt;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class WeblogTology {
	public static void main(String[] args) throws Exception, Exception {
		BrokerHosts hosts = new ZkHosts("hadoop01:2181,hadoop02:2181,hadoop03:2181");
		SpoutConfig sc =new SpoutConfig(hosts,"weblog","/weblog","info");
		sc.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout spout = new KafkaSpout(sc);
		ClearBolt clearBolt = new ClearBolt();
		PvBolt pvBolt = new PvBolt();
		UvBolt uvBolt = new UvBolt();
		VvBolt vvBolt = new VvBolt();
		HBaseBolt hbaseBolt = new HBaseBolt();
		PrintBolt printBolt = new PrintBolt();
		NewIpBolt newIpBolt=new NewIpBolt();
		NewCust newCustBolt=new NewCust();
		MysqlBolt mySqlBolt=new MysqlBolt();
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("kafkaSpout", spout);
		builder.setBolt("clearBolt", clearBolt,2).shuffleGrouping("kafkaSpout");
		builder.setBolt("pvBolt", pvBolt,2).shuffleGrouping("clearBolt");
		builder.setBolt("uvBolt", uvBolt,2).shuffleGrouping("pvBolt");
		builder.setBolt("vvBolt", vvBolt).shuffleGrouping("uvBolt");
		builder.setBolt("newIpBolt", newIpBolt).shuffleGrouping("vvBolt");
		builder.setBolt("newCust", newCustBolt).shuffleGrouping("newIpBolt");
		
		builder.setBolt("printBolt", printBolt).globalGrouping("newCust");
		builder.setBolt("hbaseBolt", hbaseBolt).globalGrouping("newCust");
		builder.setBolt("mysqlBolt", mySqlBolt).globalGrouping("newCust");
		
		StormTopology topology = builder.createTopology();
//		Config config = new Config();
//		StormSubmitter.submitTopology("weblog-produce", config, topolgy);
		LocalCluster cluster = new LocalCluster();
		Config  conf = new Config();
		cluster.submitTopology("weblog", conf, topology);
	}
}
