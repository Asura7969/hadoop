package org.hadoop.mapreduce.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroup extends WritableComparator{

	public MyGroup() {
		super(MyKey.class,true);
	}
	
	
	public int compare(WritableComparable a, WritableComparable b) {
		MyKey k1 =(MyKey) a;
		MyKey k2 =(MyKey) b;
		int res =Integer.compare(k1.getYear(), k2.getYear());
		if(res==0){
			int r2 =Integer.compare(k1.getMonth(), k2.getMonth());
			return r2;
		}
		return res;
	}
		
}
