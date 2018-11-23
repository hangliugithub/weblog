package cn.tarena.pojo;

import java.sql.Date;

public class TickTongji {
	
	private Date time;
	private double br;
	private double avgDeep;
	private double avgTime;
	public Date getTime() {
		return time;
	}
	public void setTime(Date time) {
		this.time = time;
	}
	public double getBr() {
		return br;
	}
	public void setBr(double br) {
		this.br = br;
	}
	public double getAvgDeep() {
		return avgDeep;
	}
	public void setAvgDeep(double avgDeep) {
		this.avgDeep = avgDeep;
	}
	public double getAvgTime() {
		return avgTime;
	}
	public void setAvgTime(double avgTime) {
		this.avgTime = avgTime;
	}
	
}
