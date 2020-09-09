package com.lagou.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PostDelete extends BaseRegionObserver {


    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
        //获取relative表table对象
        final HTableInterface relative =  e.getEnvironment().getTable(TableName.valueOf("relative"));

        List deletes=new ArrayList<Delete>();
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
                       appendDelete.addColumn(CellUtil.cloneFamily(cell2), thisColumn);
                       deletes.add(appendDelete);
                   }
                }
            }

        }
        //执行批量删除
        relative.delete(deletes);
        //关闭table对象
        relative.close();
    }

}
