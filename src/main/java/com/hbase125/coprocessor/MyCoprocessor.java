package com.hbase125.coprocessor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.ArrayList;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MyCoprocessor extends BaseRegionObserver {

    private static final Logger logger = LoggerFactory.getLogger(MyCoprocessor.class);

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {

    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {

    }

    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e,
                       final Put put, final WALEdit edit, final Durability durability) throws IOException {

        logger.info("prePut start");

        // 获取当前操作的表名
        String tableName = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();

        logger.info("prePut start for table: {}", tableName);

        // 获取当前 Put 对象中所有列族及其对应的列
        NavigableMap<byte[], List<Cell>> familyCellMap = put.getFamilyCellMap();

        // 遍历所有列族及其对应的列
        for (Map.Entry<byte[], List<Cell>> entry : familyCellMap.entrySet()) {
            byte[] family = entry.getKey();  // 获取列族
            List<Cell> cells = entry.getValue();  // 获取列族中的所有列

            // 遍历列族中的所有列
            for (Cell cell : cells) {
                byte[] qualifier = CellUtil.cloneQualifier(cell);  // 获取列名
                byte[] value = CellUtil.cloneValue(cell);  // 获取列的值

                logger.info("Original value - Family: {}, Qualifier: {}, Value: {}",
                        Bytes.toString(family), Bytes.toString(qualifier), Bytes.toString(value));

                if(Arrays.equals(family,"info".getBytes()) && Arrays.equals(qualifier,"name".getBytes())){
                    // 创建一个新的 Put 对象，用于保存修改后的值
                    Put newPut = new Put(put.getRow());

                    // 修改值为 'new_value'
                    byte[] newValue = Bytes.toBytes("new_value");

                    // 将修改后的值添加到新的 Put 对象中
                    newPut.addColumn(family, qualifier, newValue);

                    logger.info("Modified value - Family: {}, Qualifier: {}, New Value: {}",
                            Bytes.toString(family), Bytes.toString(qualifier), Bytes.toString(newValue));

                    // 清除原来的 Put 对象中的值，替换为修改后的值
                    put.getFamilyCellMap().clear();
                    put.getFamilyCellMap().putAll(newPut.getFamilyCellMap());
                }
            }
        }

        logger.info("prePut stop");

    }

    @Override
    public void postGetOp(final ObserverContext<RegionCoprocessorEnvironment> e,
                         final Get get, final List<Cell> results) throws IOException {

        logger.info("postGetOp start");

        // 获取当前操作的表名
        String tableName = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();

        logger.info("postGetOp start for table: {}", tableName);

        // 获取当前系统时间作为时间戳
        long currentTimestamp = System.currentTimeMillis();

        // 遍历结果集
        List<Cell> tempResults = new ArrayList<>();
        for (Cell cell : results) {
            if (Bytes.equals(CellUtil.cloneFamily(cell), Bytes.toBytes("info")) &&
                    Bytes.equals(CellUtil.cloneQualifier(cell), Bytes.toBytes("name"))) {
                // 创建一个新的 KeyValue 对象，替换原值为 new_value
                KeyValue newKv = new KeyValue(CellUtil.cloneRow(cell),
                        Bytes.toBytes("info"),
                        Bytes.toBytes("name"),
                        currentTimestamp,
                        Bytes.toBytes("Jri"));
                tempResults.add(newKv);
                logger.info("--- Modified value ---");
            } else {
                // 保留其他列
                tempResults.add(cell);
            }
        }

        // 清空原结果集，并添加修改后的结果
        results.clear();
        results.addAll(tempResults);

        logger.info("postGetOp stop");

    }

}
