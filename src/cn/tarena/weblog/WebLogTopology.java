package cn.tarena.weblog;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class WebLogTopology {
	public static void main(String[] args) {
		//定义zk集群
		BrokerHosts hosts = new ZkHosts("hadoop01:2181,hadoop02:2181,hadoop03:2181");
		
		//--定义Kafkas数据源的配置信息
		//1参：zk的连接地址
		//2参数：storm从Kafka消费的主题名
		//3,4参：指定ack消息存储在zk的路径
		//--下面的配置表示会把ack信息存到zookeeper /weblog/info
		SpoutConfig sc =new SpoutConfig(hosts,"weblog","/weblog","info");
		//设定消费端的数据类型
		
		sc.scheme = new SchemeAsMultiScheme(new StringScheme());
		//获取kafka的数据源
		KafkaSpout spout = new KafkaSpout(sc);
		PrintBolt printBolt = new PrintBolt();
		ClearBolt clearBolt = new ClearBolt();
		PvBolt pvBolt = new PvBolt();
		UvBolt uvBolt = new UvBolt();
		VvBolt vvBolt = new VvBolt();
		HBaseBolt hBaseBolt = new HBaseBolt();
		TopologyBuilder builder = new TopologyBuilder();
		//绑定数据源
		//控制并发 调优 默认槽道（slot)有四个 表示四个进程 开启了四个socket
		builder.setSpout("kafkaSpout", spout);
		builder.setBolt("clearBolt", clearBolt).shuffleGrouping("kafkaSpout");
		builder.setBolt("pvBolt", pvBolt).globalGrouping("clearBolt");
		builder.setBolt("uvBolt", uvBolt).globalGrouping("pvBolt");
		builder.setBolt("vvBolt", vvBolt).globalGrouping("uvBolt");
		builder.setBolt("hBaseBolt", hBaseBolt).globalGrouping("vvBolt");
		builder.setBolt("printBolt", printBolt).globalGrouping("vvBolt");
		StormTopology topology = builder.createTopology();
		LocalCluster cluster = new LocalCluster();
		Config  conf = new Config();
		cluster.submitTopology("StormTopology", conf, topology);
		
	}
}
