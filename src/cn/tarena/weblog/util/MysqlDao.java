package cn.tarena.weblog.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import cn.tarena.pojo.TickTongji;
import cn.tarena.pojo.Tongji;

public class MysqlDao {

	public static void saveToMysql(Tongji t) throws Exception {
		Class.forName("com.mysql.jdbc.Driver");
		Connection con=DriverManager.getConnection(
				    "jdbc:mysql://hadoop01:3306/weblog","root","root");
		PreparedStatement ps =con.prepareStatement(
				   "insert into  tongji values(?,?,?,?,?,?)");
		
		ps.setDate(1, t.getSstime());
		ps.setInt(2, t.getPv());
		ps.setInt(3, t.getUv());
		ps.setInt(4, t.getVv());
		ps.setInt(5, t.getNewIp());
		ps.setInt(6, t.getNewCust());
		
		//--执行插入
		ps.executeUpdate();
		con.close();
		
		
	}

	public static void tickToMysql(TickTongji tt) throws Exception {
		Class.forName("com.mysql.jdbc.Driver");
		Connection con=DriverManager.getConnection(
				    "jdbc:mysql://hadoop01:3306/weblog","root","root");
		
		PreparedStatement ps=con.prepareStatement("insert into tongji2 values(?,?,?,?)");
		ps.setDate(1, tt.getTime());
		ps.setDouble(2, tt.getBr());
		ps.setDouble(3, tt.getAvgTime());
		ps.setDouble(4, tt.getAvgDeep());
		
		ps.executeUpdate();
		con.close();
	}

}
