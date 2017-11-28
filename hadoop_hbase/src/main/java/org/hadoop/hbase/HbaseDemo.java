package org.hadoop.hbase;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by root on 2017/11/28.
 *
 * HBase_API
 */
public class HbaseDemo {

    public HBaseAdmin hadmin;

    public HTable htable;

    public String TN = "phone2017";

    public Random r = new Random();


    @Before
    public void beign() throws Exception {
        Configuration conf = new Configuration();
        // 如果是伪分布  zk集群 就是一台服务器
        conf.set("hbase.zookeeper.quorum", "node02,node03,node04");

        hadmin = new HBaseAdmin(conf);
        htable = new HTable(conf, TN);
    }

    @After
    public void end() throws Exception {
        if(hadmin != null) {
            hadmin.close();
        }
        if(htable != null) {
            htable.close();
        }
    }

    /**
     * 创建表
     * @throws Exception
     */
    @Test
    public void createTable() throws Exception {

        if(hadmin.tableExists(TN)) {
            hadmin.disableTable(TN);
            hadmin.deleteTable(TN);
        }

        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TN));

        HColumnDescriptor family = new HColumnDescriptor("cf1");
        family.setInMemory(true);
        family.setMaxVersions(1);
        family.setBlockCacheEnabled(true);

        desc.addFamily(family);

        hadmin.createTable(desc);
    }

    /**
     * 插入数据
     * @throws Exception
     */
    @Test
    public void insertCell() throws Exception {
        Put put = new Put("001".getBytes());
        put.add("cf".getBytes(), "name".getBytes(), "xiaoming".getBytes());
        put.add("cf".getBytes(), "sex".getBytes(), "boy".getBytes());
        put.add("cf".getBytes(), "age".getBytes(), "17".getBytes());

        htable.put(put);
    }

    @Test
    public void getCell() throws Exception {
        Get get = new Get("001".getBytes());

        get.addColumn("cf".getBytes(), "name".getBytes());

        Result rs = htable.get(get);
        Cell cell = rs.getColumnLatestCell("cf".getBytes(), "name".getBytes());

        String name = new String(CellUtil.cloneValue(cell));
        System.out.println(name);
    }

    /**
     * rowkey的设计： 手机号_(longmaxvalue-时间戳)
     * 通话详单： 对方手机号pnum、主叫被叫类型type(1,0)、通话时间...
     * 十个人   每个人一天产生100条通话记录
     * @throws Exception
     */
    @Test
    public void insertDB1() throws Exception {

        List<Put> puts = new ArrayList<Put>();

        for (int i = 0; i < 10; i++) {
            // 自己的手机号
            String num = getPhone("186");

            for (int j = 0; j < 100; j++) {
                // 对手手机号
                String pnum = getPhone("177");
                // 通话时间
                String datestr = getDate("2017");

                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
                Date date = sdf.parse(datestr);
                long datelong = date.getTime();

                // rowkey的设计：  目的  保证查询的速度 以及通过时间做降序排序
                String rowkey = num + "_" + (Long.MAX_VALUE - datelong);

                Put put = new Put(rowkey.getBytes());
                put.add("cf".getBytes(), "pnum".getBytes(), pnum.getBytes());
                put.add("cf".getBytes(), "type".getBytes(), (r.nextInt(2)+"").getBytes());
                put.add("cf".getBytes(), "date".getBytes(), datestr.getBytes());

                puts.add(put);
            }
        }

        htable.put(puts);
    }


    /**
     * 查询某个手机号  某个月的通话记录
     *  18680585605 一月份的通话记录
     */
    @Test
    public void scanDB1() throws Exception {
        Scan scan = new Scan();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String startRow = "18680585605_" + (Long.MAX_VALUE-sdf.parse("20170201000000").getTime());

        String stopRow = "18680585605_" + (Long.MAX_VALUE-sdf.parse("20170101000000").getTime());

        // 设置scan范围查找 起始
        scan.setStartRow(startRow.getBytes());
        // 设置scan范围查找 结束
        scan.setStopRow(stopRow.getBytes());

        ResultScanner rss = htable.getScanner(scan);
        for (Result rs : rss) {
            System.out.print(new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "pnum".getBytes()))));
            System.out.print(" - " + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "type".getBytes()))));
            System.out.println(" - " + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "date".getBytes()))));
        }
    }

    /**
     * 查询某个手机号  所有的主叫类型的通话记录
     * 	18680585605  type=1
     * 过滤器
     * @throws Exception
     */
    @Test
    public void scanDB2() throws Exception {
        //MUST_PASS_ALL:必须满足所有过滤器
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        // 前缀的过滤器
        PrefixFilter filter1 = new PrefixFilter("18680585605_".getBytes());
        list.addFilter(filter1);

        // 列值的过滤  type=1
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter("cf".getBytes(), "type".getBytes(), CompareOp.EQUAL, "1".getBytes());
        list.addFilter(filter2);

        Scan scan = new Scan();
        scan.setFilter(list);

        ResultScanner rss = htable.getScanner(scan);
        for (Result rs : rss) {
            String rowKey = new String(rs.getColumnLatestCell("cf1".getBytes(),"type".getBytes()).getRow());
            System.out.print("rowKey:" + rowKey + "_" + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "pnum".getBytes()))));
            System.out.print(" - " + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "type".getBytes()))));
            System.out.println(" - " + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "date".getBytes()))));
        }
    }

    //作业： 删除Cell单元格

    /**
     * 随机返回手机号码
     * @param prefix 手机号码前缀 eq：186
     * @return 手机号码:18612341234
     */
    public String getPhone(String prefix) {
        return prefix + String.format("%08d", r.nextInt(99999999));
    }


    /**
     * 随机返回日期 yyyyMMddHHmmss
     * @param year 年
     * @return 日期 :20160101020203
     */
    public String getDate(String year) {
        return year + String.format("%02d%02d%02d%02d%02d",
                new Object[]{r.nextInt(12)+1,r.nextInt(29),
                        r.nextInt(24),r.nextInt(24),r.nextInt(24)});
    }

}
