package cn.tarena.dao;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import cn.tarena.pojo.FluxInfo;

public class HBaseDao {
//	public static void main(String[] args) throws Exception {
//
//		Configuration conf = HBaseConfiguration.create();
//		conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
//		
//		HTable table = new HTable(conf,"ns1:web");
//		//行键的设计 sstime_uvid_ssid_两位随机数
//		String rowKey = "Sstime()"+"_"+"getUvid"+"_"+"_"+(int)(Math.random()*100);
//		Put put = new Put(rowKey.getBytes());
//		put.add("cf1".getBytes(),"url".getBytes(),"1234".getBytes());
//		put.add("cf1".getBytes(),"cip".getBytes(),"test".getBytes());
//		table.put(put);
//		table.close();
//	}

	public static void saveToHBase(FluxInfo f) throws IOException {
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		
		HTable table = new HTable(conf,"ns1:web");
		//行键的设计 sstime_uvid_ssid_两位随机数
		String rowKey = f.getSstime()+"_"+f.getUvid()+"_"+f.getSsid()+"_"+(int)(Math.random()*100);
		Put put = new Put(rowKey.getBytes());
		put.add("cf1".getBytes(),"url".getBytes(),f.getUrl().getBytes());
		put.add("cf1".getBytes(),"urlname".getBytes(),f.getUrlName().getBytes());
		put.add("cf1".getBytes(),"uvid".getBytes(),f.getUvid().getBytes());
		put.add("cf1".getBytes(),"ssid".getBytes(),f.getSsid().getBytes());
		put.add("cf1".getBytes(),"sscount".getBytes(),f.getSscount().getBytes());
		put.add("cf1".getBytes(),"sstime".getBytes(),f.getSstime().getBytes());
		put.add("cf1".getBytes(),"cip".getBytes(),f.getCip().getBytes());
		table.put(put);
		table.close();
	}

	/**
	 * 通过扫描的起始时间戳和终止的时间戳结合行键过滤HBase查询数据
	 * @param startTime
	 * @param endtime
	 * @param regex 正则表达式
	 * @return
	 * @throws IOException 
	 */
	public static List<FluxInfo> queryBYRange(String startTime, String endtime ,String regex) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
		
		HTable table = new HTable(conf,"weblog".getBytes());
		Scan scan = new Scan();
		scan.setStartRow(startTime.getBytes());
		scan.setStopRow(endtime.getBytes());
		
		Filter filter = new RowFilter(CompareOp.EQUAL,new RegexStringComparator(regex));
		
		scan.setFilter(filter);
		//--集合Scan兑奖，执行查询封装到结果集ResultSanner
		ResultScanner rs = table.getScanner(scan);
		//--获取行数据迭代器
		Iterator<Result> it = rs.iterator();
		//List
		List<FluxInfo> list = new LinkedList<>();
		while(it.hasNext()){
			//
			Result result = it.next();
			FluxInfo f = new FluxInfo();
			f.setUrl(new String(result.getValue("cf1".getBytes(), "url".getBytes())));
			f.setUrlName(new String(result.getValue("cf1".getBytes(), "urlname".getBytes())));
			f.setUvid(new String(result.getValue("cf1".getBytes(), "uvid".getBytes())));
			f.setSsid(new String(result.getValue("cf1".getBytes(), "ssid".getBytes())));
			f.setSscount(new String(result.getValue("cf1".getBytes(), "sscount".getBytes())));
			f.setSstime(new String(result.getValue("cf1".getBytes(), "sstime".getBytes())));
			f.setCip(new String(result.getValue("cf1".getBytes(), "cip".getBytes())));
		}
		
		return list;
	}


	public static List<FluxInfo> queryByColum(String columnFamily, String columnName, String columnValue) throws IOException {
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum","hadoop01:2181,hadoop02:2181,hadoop03:2181");
		
		HTable table=new HTable(conf, "weblog".getBytes());
		Scan scan=new Scan();
		//--列值过滤器
		Filter filter=new SingleColumnValueFilter(
							columnFamily.getBytes(), 
							columnName.getBytes(), 
							CompareOp.EQUAL, 
							columnValue.getBytes());
		scan.setFilter(filter);
		ResultScanner rs= table.getScanner(scan);
		Iterator<Result> it=rs.iterator();
		List<FluxInfo> list=new LinkedList<>();
		while(it.hasNext()){
			Result r=it.next();
			FluxInfo f=new FluxInfo();
			String url=new String(r.getValue("cf1".getBytes(),"url".getBytes()));
			String urlname=new String(r.getValue("cf1".getBytes(),"urlname".getBytes()));
			String uvid=new String(r.getValue("cf1".getBytes(),"uvid".getBytes()));
			String ssid=new String(r.getValue("cf1".getBytes(),"ssid".getBytes()));
			String sstime=new String(r.getValue("cf1".getBytes(),"sstime".getBytes()));
			String sscount=new String(r.getValue("cf1".getBytes(),"sscount".getBytes()));
			String cip=new String(r.getValue("cf1".getBytes(),"cip".getBytes()));
			f.setUrl(url);
			f.setUrlName(urlname);
			f.setUvid(uvid);
			f.setSsid(ssid);
			f.setSstime(sstime);
			f.setSscount(sscount);
			f.setCip(cip);
			list.add(f);
		}
		table.close();
		return list;
	}

	
	
}
