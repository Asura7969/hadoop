package org.hadoop.mapreduce.weather;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * WritableComparable接口： 如果自定义类作为key用 必须实现该接口。
 * 
 * Writable接口 ：如果自定义一个类作为value用  。定义序列化和反序列化
 * 	readFields
 * 	write
 * Comparable接口
 * 	compareTo
 * 
 * @author root
 *
 */
public class MyKey implements WritableComparable<MyKey>{

	private int year;
	private int month;
	private double hot;
	private int day;
	
	public MyKey(int year, int month, double hot, int day) {
		super();
		this.year = year;
		this.month = month;
		this.hot = hot;
		this.day = day;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public int getYear() {
		return year;
	}
	
	public MyKey(int year, int month, double hot) {
		super();
		this.year = year;
		this.month = month;
		this.hot = hot;
	}

	
	public MyKey() {
		super();
	}

	public void setYear(int year) {
		this.year = year;
	}
	public int getMonth() {
		return month;
	}
	public void setMonth(int month) {
		this.month = month;
	}
	public double getHot() {
		return hot;
	}
	public void setHot(double hot) {
		this.hot = hot;
	}
	
	public void readFields(DataInput in) throws IOException {
		this.year=in.readInt();
		this.month=in.readInt();
		this.hot=in.readDouble();
		this.day=in.readInt();
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeInt(month);
		out.writeDouble(hot);
		out.writeInt(day);
	}
	
	/**
	 * 在当前的设计中需要用该方法排序比较
	 */
	public int compareTo(MyKey o) {

		int res =(this.year < o.getYear()) ? -1 : ((this.year == o.getYear()) ? 0 : 1);
		if(res==0){

			int r2 =(this.month < o.getMonth()) ? -1 : ((this.month == o.getMonth()) ? 0 : 1);
			if(r2==0){

				int r3 = (this.getDay() < o.getDay()) ? -1 : ((this.getDay() == o.getDay()) ? 0 : 1);
				if(r3==0){
					return -Double.compare(this.hot, o.getHot());
				}
				return r3;
			}
			return r2;
		}
		return res;
	}
	
}
