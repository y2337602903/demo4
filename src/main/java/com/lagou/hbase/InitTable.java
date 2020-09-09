package com.lagou.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InitTable {
    Configuration conf = null;
    Connection conn = null;

    @Before
    public void init() throws IOException {
        //获取一个配置文件对象
        conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum", "linux121,linux122");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        //通过conf获取到hbase集群的连接
        conn = ConnectionFactory.createConnection(conf);
    }

    //创建一张hbase表
    @Test
    public void createTable() throws IOException {
        //获取HbaseAdmin对象用来创建表
        HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
        //创建Htabledesc描述器，表描述器
        final HTableDescriptor relative = new HTableDescriptor(TableName.valueOf("relative"));
        //指定列族
        relative.addFamily(new HColumnDescriptor("friends"));
        admin.createTable(relative);
        System.out.println("relative表创建成功！！");
    }

    //插入基础数据
    @Test
    public void putData() throws IOException {
        //需要获取一个table对象
        final Table relative = conn.getTable(TableName.valueOf("relative"));
        List<Put> puts=new ArrayList<Put>();
        //这里定义的是生产多少个人的关系数据
        int count=5;
        for(int i=1;i<count+1;i++){
            //指定rowkey
            Put put = new Put(Bytes.toBytes("uid"+i));
            for(int j=1;j<count+1;j++){
                if(i==j){
                    continue;
                }else {
                    put.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid"+j), Bytes.toBytes("uid"+j));
                }
            }
            puts.add(put);
        }
        //批量插入
        relative.put(puts);
        relative.close();
        System.out.println("插入数据到relative表成功！！");
    }

    //全表扫描
    @Test
    public void scanData() throws IOException {
        //准备table对象
        final Table worker = conn.getTable(TableName.valueOf("relative"));
        //准备scan对象
        final Scan scan = new Scan();

        //执行扫描
        final ResultScanner resultScanner = worker.getScanner(scan);
        for (Result result : resultScanner) {
            //获取到result中所有cell对象
            final Cell[] cells = result.rawCells();
            //遍历打印
            for (Cell cell : cells) {
                final String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
                final String f = Bytes.toString(CellUtil.cloneFamily(cell));
                final String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                final String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println("rowkey-->" + rowkey + "--;cf-->" + f + ";column--->" + column + "--;value-->" + value);
            }
        }

        worker.close();
    }


    //删除一条数据
    @Test
    public void deleteData() throws IOException {
        //需要获取一个table对象
        final Table relative = conn.getTable(TableName.valueOf("relative"));

        //准备delete对象
        final Delete delete = new Delete(Bytes.toBytes("uid2"));
        delete.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("uid3"));


     /*   List deletes=new ArrayList<Delete>();
        Scan scan = new Scan();
        //取得被删除的所有cell
        List<Cell> cells = delete.getFamilyCellMap().get(Bytes.toBytes("friends"));
        for (Cell cell : cells) {
            //取得被删除的列
            byte[] deletedColumn = CellUtil.cloneQualifier(cell);
            //取得当前的人
            byte[] thisColumn = CellUtil.cloneRow(cell);
            //查询被删除者的名单中是否有当前人
            scan.setStartRow(deletedColumn);
            ResultScanner resultScanner = relative.getScanner(scan);
            Result next = resultScanner.next();
            //有的话，被删除者也删除当前人
            if(next!=null&&!next.isEmpty()){
                Cell[] cells2 = next.rawCells();
                for (Cell cell2 : cells2) {
                    if(new String(thisColumn).equals(new String(CellUtil.cloneQualifier(cell2)))){
                        Delete appendDelete = new Delete(deletedColumn);
                        appendDelete.addColumn(CellUtil.cloneFamily(cell), thisColumn);
                        deletes.add(appendDelete);
                    }
                }
            }

        }
        relative.delete(deletes);*/



        //执行删除
        relative.delete(delete);


/*
        System.out.println("delete.getRow() = " + new String(delete.getRow()));

        List<Cell> cells = delete.getFamilyCellMap().get(Bytes.toBytes("friends"));
        for (Cell cell : cells) {
            final String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
            final String f = Bytes.toString(CellUtil.cloneFamily(cell));
            final String column = Bytes.toString(CellUtil.cloneQualifier(cell));
            final String value = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println("rowkey-->" + rowkey + "--;cf-->" + f + ";column--->" + column + "--;value-->" + value);
        }*/







        //关闭table对象
        relative.close();
        System.out.println("删除数据成功！！");
    }


    //释放连接
    @After
    public void realse() {
        if (conn != null) {
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
