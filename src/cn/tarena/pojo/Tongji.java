package cn.tarena.pojo;

import java.sql.Date;

public class Tongji {
	private Date sstime;
	private int pv;
	private int uv;
	private int vv;
	private int newIp;
	private int newCust;

	public Date getSstime() {
		return sstime;
	}

	public void setSstime(Date sstime) {
		this.sstime = sstime;
	}

	public int getPv() {
		return pv;
	}

	public void setPv(int pv) {
		this.pv = pv;
	}

	public int getUv() {
		return uv;
	}

	public void setUv(int uv) {
		this.uv = uv;
	}

	public int getVv() {
		return vv;
	}

	public void setVv(int vv) {
		this.vv = vv;
	}

	public int getNewIp() {
		return newIp;
	}

	public void setNewIp(int newIp) {
		this.newIp = newIp;
	}

	public int getNewCust() {
		return newCust;
	}

	public void setNewCust(int newCust) {
		this.newCust = newCust;
	}
}
