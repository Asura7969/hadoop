package org.hadoop.mapreduce.friend;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * Created by root on 2017/11/26.
 *
 * 实现WritableComparable接口可以序列化对象并且根据这个对象做比较
 *      readFields()和write()方法是序列化和反序列化函数
 *      compareTo()方法是比较函数
 */
public class User implements WritableComparable<User>{

    private String user;
    private String other;
    private int count;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getOther() {
        return other;
    }

    public void setOther(String other) {
        this.other = other;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int compareTo(User o) {
        int r = this.getUser().compareTo(o.getUser());
        if(r == 0){
            return (this.getCount() < o.getCount()) ? -1 : ((this.getCount() == o.getCount()) ? 0 : 1);
        }
        return r;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(user);
        out.writeUTF(other);
        out.writeInt(count);
    }

    public void readFields(DataInput in) throws IOException {
        this.user = in.readUTF();
        this.other = in.readUTF();
        this.count = in.readInt();
    }
}
