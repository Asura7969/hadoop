package org.hadoop.mapreduce_hbase.read_hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.List;

/**
 * Created by gongwenzhou on 2017/11/29.
 *
 * 读取hbase中表的单词并做统计
 */
public class WCMapper extends TableMapper<Text,IntWritable> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //获取指定列族列名的cell列表
        List<Cell> columnCells = value.getColumnCells("cf".getBytes(), "wordName".getBytes());
        //List<Cell> cells = value.listCells();

        for (Cell cell : columnCells) {
            String s = CellUtil.cloneValue(cell).toString();
            System.out.println("map:" + s);
            context.write(new Text(s),new IntWritable(1));
        }
    }
}
