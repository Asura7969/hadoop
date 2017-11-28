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
		int res =(k1.getYear() < k2.getYear()) ? -1 : ((k1.getYear() == k2.getYear()) ? 0 : 1);
		if(res==0){
			int r2 =(k1.getMonth() < k2.getMonth()) ? -1 : ((k1.getMonth() == k2.getMonth()) ? 0 : 1);
			return r2;
		}
		return res;
	}
		
}
