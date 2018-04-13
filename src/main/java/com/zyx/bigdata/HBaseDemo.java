package com.zyx.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.KeyValue;
import org.eclipse.jdt.internal.core.SourceType;

/**
 *
 * 使用Java代码操作 HBase,JavaAPI插入数据一直无法，需要在本地配置域名
 */
public class HBaseDemo {
    private static final String ZKIP="192.168.1.51,192.168.1.60,192.168.1.61";
    private static final String ZKPORT="2181";
    private static Configuration conf = null;
    private Connection connection=null;
    private Table table=null;
    private Scan scan=null;

    public HBaseDemo(String tableName){
        //配置HBase连接信息
        conf = HBaseConfiguration.create();
        //客户端连接为 zookeeper zookeeper自动查找活动的 HMaster
        conf.set("hbase.zookeeper.quorum",ZKIP);
        conf.set("hbase.zookeeper.property.clientPort", ZKPORT);
        try {
            connection=ConnectionFactory.createConnection(conf);
            table=connection.getTable(TableName.valueOf(tableName));
            System.out.println("连接"+table);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 测试
     */

    public static void main(String[] args) throws Exception {
        HBaseDemo hbase=new HBaseDemo("user");
//        hbase.put("12345","info1","username","1234");
//        String result=hbase.getResultByColumn("12345","info1","username");
//        System.out.println(result);
//        hbase.scanDataByValueFilter("info1","name","just");
//        hbase.scanDataByRowkeyFilter("^1234");
        hbase.scanDataByFilterList();
        hbase.close();



    }

    /**
     *  1.1单行插入数据
     */

    public void put(String rowKey, String familyName, String columnName, String value) throws IOException {
        Put put=new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
        table.put(put);
    }
    /**
     * 1.2 多行插入，直接以PUt的list插入
     * 1000000条数据插入测试大约 1分半钟
     */

    public void putAll() throws IOException {
        List<Put> puts = new ArrayList<Put>(10000);
        for (int i = 0; i < 1000001; i++) {
            Put put = new Put(Bytes.toBytes("kr" + i));
            put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("name"), Bytes.toBytes("maxadmin" + i));
            put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("age"), Bytes.toBytes(i));
            puts.add(put);
            if (i % 10000 == 0) {
                table.put(puts);
                puts = new ArrayList<Put>(10000);
            }
        }
        table.put(puts);
        table.close();
    }

    /**
     * 2.1让表失效
     */
    public void disableTable(String tableName) throws Exception {
        HBaseAdmin admin=new HBaseAdmin(conf);
        admin.disableTable(tableName);
    }

    /**
     *
     * 2.2 删除表
     */
    public void dropTable(String tableName) throws Exception {
        HBaseAdmin admin=new HBaseAdmin(conf);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }
    /**
     * 2.3删除特定rowkey
     */
    public void deleteRowkey( String rowKey) throws Exception {
        Delete de =new Delete(Bytes.toBytes(rowKey));
        table.delete(de);
    }
    /**
     * 2.4 删除特定列
     */
    public void deleteColumn(String rowKey, String falilyName, String columnName) throws Exception {

        Delete de =new Delete(Bytes.toBytes(rowKey));
        de.addColumn(Bytes.toBytes(falilyName), Bytes.toBytes(columnName));
        table.delete(de);
    }
    /**
     * 3.1 查询特定列,result返回的是整个rowkey
     */
    public String  getResultByColumn(String rowKey, String familyName, String columnName)
            throws Exception {
        Get get=new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName),Bytes.toBytes(columnName));
        Result result=table.get(get);
        String data=Bytes.toString(result.value());
        return data;
    }
    /**
     * 3.2 全表扫描
     */
    public void scan(String start,String end,String[] columFamily,String[] column) throws IOException {
        scan = new Scan();
        scan.setStartRow(Bytes.toBytes(start));
        scan.setStopRow(Bytes.toBytes(end));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for(int i=0;i<columFamily.length;i++){
                for(int j=0;j<column.length;j++){
                    System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("username"))));
                }

            }

        }
    }

    /**
     * 3.3 列值过来器,CompareOp比较器可取的值
     * LESS,
     * LESS_OR_EQUAL,
     * EQUAL,
     * NOT_EQUAL,
     * GREATER_OR_EQUAL,
     * GREATER,
     * NO_OP;
     *
     *
     */
    public void scanDataByValueFilter(String ColumnFamily,String ColumnName,String CompareValue) throws IOException {
        // 创建全表扫描的scan,
       SingleColumnValueFilter filter=new SingleColumnValueFilter(Bytes.toBytes(ColumnFamily),Bytes.toBytes(ColumnName),
               CompareFilter.CompareOp.EQUAL,Bytes.toBytes(CompareValue));
        Scan scan=new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for(Result result:scanner){
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes(ColumnFamily),Bytes.toBytes(ColumnName))));
            System.out.println(Bytes.toString(result.getRow()));
        }
    }

    /**
     * 3.4rowkey过滤器
     *
     */
    public void scanDataByRowkeyFilter(String regex) throws IOException {
        //匹配以“1234开头的rowkwy”
        RowFilter filter=new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(regex));
        Scan scan=new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for(Result result:scanner){
            System.out.println("列值:"+Bytes.toString(result.getValue(Bytes.toBytes("info1"),Bytes.toBytes("name"))));
            System.out.println(Bytes.toString(result.getRow()));
        }
    }
    /**
     * 3.4rowkey过滤器集合
     *
     */
    public void scanDataByFilterList() throws IOException {
        // 创建全表扫描的scan
        Scan scan = new Scan();
        //过滤器集合：MUST_PASS_ALL（and）,MUST_PASS_ONE(or)
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        //匹配rowkey以wangsenfeng开头的
        RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^12345"));
        //匹配name的值等于wangsenfeng
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("info1"),
                Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes("1234"));
        filterList.addFilter(filter);
        filterList.addFilter(filter2);
        scan.setFilter(filterList);
        ResultScanner scanner = table.getScanner(scan);
        for(Result result:scanner){
            System.out.println("列值:"+Bytes.toString(result.getValue(Bytes.toBytes("info1"),Bytes.toBytes("name"))));
            System.out.println(Bytes.toString(result.getRow()));
        }

    }





    public void close() throws IOException {
        table.close();
    }


}
