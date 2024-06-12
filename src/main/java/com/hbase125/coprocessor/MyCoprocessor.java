package com.hbase125.coprocessor;

import java.io.IOException;
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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bouncycastle.crypto.engines.SM4Engine;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.crypto.CipherParameters;
import org.bouncycastle.crypto.BlockCipher;
import org.bouncycastle.crypto.modes.CBCBlockCipher;
import org.bouncycastle.crypto.paddings.PaddedBufferedBlockCipher;
import org.bouncycastle.crypto.paddings.PKCS7Padding;
import org.bouncycastle.crypto.params.ParametersWithIV;


public class MyCoprocessor extends BaseRegionObserver {

    private static final Logger logger = LoggerFactory.getLogger(MyCoprocessor.class);

    private static MyPolicy.PolicyInfo staticPolicyInfo = null;

    private MyConfigure configure = null;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        configure = new MyConfigure("/home/hbconf/extconfig.xml");
        super.start(e);
    }


    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        configure = null;
        super.stop(env);
    }


    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e,
                       final Put put, final WALEdit edit, final Durability durability) throws IOException {
        logger.info("prePut start");

        /*
        * 获取当前操作的表名,比较是否为配置文件中的表
        * 不在配置中直接退出，不做处理
        */
        String tableName = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
        MyConfigure.Table table = null;
        for (MyConfigure.Table t : configure.getConfiguration().getTables()) {
            if (t.getName().equals(tableName)) {
                table = t;
                break;
            }
        }
        if (table == null) {
            logger.info("prePut stop");
            return;
        }

        // 创建一个新的 Put 对象用于保存修改后的值
        Put newPut = new Put(put.getRow());

        //遍历结果集，获取Put操作中所有的列簇
        NavigableMap<byte[], List<Cell>> familyCellMap = put.getFamilyCellMap();
        for (Map.Entry<byte[], List<Cell>> entry : familyCellMap.entrySet()) {
            byte[] family = entry.getKey();
            List<Cell> cells = entry.getValue();

            /*
             * 直接将当前 family 的所有数据放入 newPut 中
             * 仅符合加密条件的数据才会被修改
             */
            for (Cell cell : cells) {
                newPut.add(cell);
            }

            MyConfigure.Family tableFamily = null;
            for (MyConfigure.Family f : table.getFamilies()) {
                if (f.getName().equals(Bytes.toString(family))) {
                    tableFamily = f;
                    break;
                }
            }
            if (tableFamily == null) {
                continue;
            }

            // 遍历列簇Map，获取列族中的所有列名
            for (Cell cell : cells) {
                byte[] qualifier = CellUtil.cloneQualifier(cell);

                MyConfigure.Qualifier configQualifier = null;
                for (MyConfigure.Qualifier q : tableFamily.getQualifiers()) {
                    if (q.getName().equals(Bytes.toString(qualifier))) {
                        configQualifier = q;
                        break;
                    }
                }
                if (configQualifier == null) {
                    continue;
                }

                // 获取到配置加密列名，调用策略
                MyPolicy.ASN1Request asn1Request = MyPolicy.createASN1Request(configQualifier.getUrl(), configQualifier.getPid());
                if(asn1Request == null) {
                    logger.error("configQualifier Pid is null or Url is null");
                    throw new IOException("Failed to create ASN1Request for configQualifier");
                }

                staticPolicyInfo = MyPolicy.FindPolicy(asn1Request);
                if(staticPolicyInfo == null) {
                    logger.error("find policy error, PolicyInfo is null");
                    throw new IOException("Failed to find policy for configQualifier");
                }

                byte[] value = CellUtil.cloneValue(cell);
                byte[] keyBytes = staticPolicyInfo.getKey();
                byte[] ivBytes = staticPolicyInfo.getIv();

                try {
                    byte[] newValue = sm4Encrypt(keyBytes, value, ivBytes);

                    // 删除原始数据并添加加密后的数据
                    newPut.getFamilyCellMap().get(family).remove(cell);

                    KeyValue newKv = new KeyValue(CellUtil.cloneRow(cell),
                            family,
                            qualifier,
                            cell.getTimestamp(),
                            newValue);

                    newPut.add(newKv);

                } catch (Exception ex) {
                    logger.error("sm4Encrypt failed");
                    throw new IOException("Encryption failed", ex);
                }
            }
        }
        // 清除原来的 Put 对象中的值，替换为修改后的值
        put.getFamilyCellMap().clear();
        put.getFamilyCellMap().putAll(newPut.getFamilyCellMap());

        logger.info("prePut stop");
    }


    @Override
    public void postGetOp(final ObserverContext<RegionCoprocessorEnvironment> e,
                         final Get get, final List<Cell> results) throws IOException {

        logger.info("postGetOp start");

        // 获取当前系统时间作为时间戳
        long currentTimestamp = System.currentTimeMillis();

        /*
         * 获取当前操作的表名,比较是否为配置文件中的表
         * 不在配置中直接退出，不做处理
         */
        String tableName = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
        MyConfigure.Table table = null;
        for (MyConfigure.Table t : configure.getConfiguration().getTables()) {
            if (t.getName().equals(tableName)) {
                table = t;
                break;
            }
        }
        if (table == null) {
            logger.info("postGetOp stop");
            return;
        }

        // 创建一个新的 Cell 用于保存修改后的值
        List<Cell> tempResults = new ArrayList<>();

        for (Cell cell : results) {
            byte[] family = CellUtil.cloneFamily(cell);
            byte[] qualifier = CellUtil.cloneQualifier(cell);

            // 不符合加密规则会保留该Cell
            tempResults.add(cell);

            MyConfigure.Family tableFamily = null;
            for (MyConfigure.Family f : table.getFamilies()) {
                if (f.getName().equals(Bytes.toString(family))) {
                    tableFamily = f;
                    break;
                }
            }
            if (tableFamily == null) {
                continue;
            }

            MyConfigure.Qualifier configQualifier = null;
            for (MyConfigure.Qualifier q : tableFamily.getQualifiers()) {
                if (q.getName().equals(Bytes.toString(qualifier))) {
                    configQualifier = q;
                    break;
                }
            }
            if (configQualifier == null) {
                continue;
            }

            // 获取到配置加密列名，调用策略
            MyPolicy.ASN1Request asn1Request = MyPolicy.createASN1Request(configQualifier.getUrl(), configQualifier.getPid());
            if(asn1Request == null) {
                logger.error("configQualifier Pid is null or Url is null");
                throw new IOException("Failed to create ASN1Request for configQualifier");
            }

            staticPolicyInfo = MyPolicy.FindPolicy(asn1Request);
            if(staticPolicyInfo == null) {
                logger.error("find policy error, PolicyInfo is null");
                throw new IOException("Failed to find policy for configQualifier");
            }

            byte[] value = CellUtil.cloneValue(cell);
            byte[] keyBytes = staticPolicyInfo.getKey();
            byte[] ivBytes = staticPolicyInfo.getIv();

            try {
                byte[] newValue = sm4Decrypt(keyBytes, value, ivBytes);

                // 创建一个新的 KeyValue 对象，将配置过的加密数据解密处理，替换
                tempResults.remove(tempResults.size() - 1);

                KeyValue newKv = new KeyValue(CellUtil.cloneRow(cell),
                        family,
                        qualifier,
                        currentTimestamp,
                        newValue);

                tempResults.add(newKv);

            } catch (Exception ex) {
                logger.error("sm4Decrypt failed");
                throw new IOException("Decryption failed", ex);
            }
        }
        // 清空原结果集，添加修改后的结果
        results.clear();
        results.addAll(tempResults);

        logger.info("postGetOp stop");
    }


    public boolean postScannerNext(ObserverContext<RegionCoprocessorEnvironment> e,
                                   InternalScanner s,
                                   List<Result> results,
                                   int limit, boolean hasMore) throws IOException {

        logger.info("postScannerNext start");

        // 获取当前系统时间作为时间戳
        long currentTimestamp = System.currentTimeMillis();

        /*
         * 获取当前操作的表名,比较是否为配置文件中的表
         * 不在配置中直接退出，不做处理
         */
        String tableName = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
        MyConfigure.Table table = null;
        for (MyConfigure.Table t : configure.getConfiguration().getTables()) {
            if (t.getName().equals(tableName)) {
                table = t;
                break;
            }
        }
        if (table == null) {
            logger.info("postScannerNext stop");
            return hasMore;
        }

        // 创建一个新的 Result 用于保存修改后的值
        List<Result> newResults = new ArrayList<>();

        /*
         * 和postGet方法类似,但scan有多个result数据集
         * 一次scan等同于多个Get,在代码中拆分为多次Get的处理逻辑
         */
        for(Result result : results) {
            List<Cell> cells = result.listCells();

            // 创建一个新的 Cell 用于保存修改后的值
            List<Cell> tempResults = new ArrayList<>();

            for (Cell cell : cells) {
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);

                //不符合加密规则会保留该Cell
                tempResults.add(cell);

                MyConfigure.Family tableFamily = null;
                for (MyConfigure.Family f : table.getFamilies()) {
                    if (f.getName().equals(Bytes.toString(family))) {
                        tableFamily = f;
                        break;
                    }
                }
                if (tableFamily == null) {
                    continue;
                }

                MyConfigure.Qualifier configQualifier = null;
                for (MyConfigure.Qualifier q : tableFamily.getQualifiers()) {
                    if (q.getName().equals(Bytes.toString(qualifier))) {
                        configQualifier = q;
                        break;
                    }
                }
                if (configQualifier == null) {
                    continue;
                }

                // 获取到配置加密列名，调用策略
                MyPolicy.ASN1Request asn1Request = MyPolicy.createASN1Request(configQualifier.getUrl(), configQualifier.getPid());
                if(asn1Request == null) {
                    logger.error("configQualifier Pid is null or Url is null");
                    throw new IOException("Failed to create ASN1Request for configQualifier");
                }

                staticPolicyInfo = MyPolicy.FindPolicy(asn1Request);
                if(staticPolicyInfo == null) {
                    logger.error("find policy error, PolicyInfo is null");
                    throw new IOException("Failed to find policy for configQualifier");
                }

                byte[] value = CellUtil.cloneValue(cell);
                byte[] keyBytes = staticPolicyInfo.getKey();
                byte[] ivBytes = staticPolicyInfo.getIv();

                try {
                    byte[] newValue = sm4Decrypt(keyBytes, value, ivBytes);

                    // 创建一个新的 KeyValue 对象，将配置过的加密数据解密处理，替换
                    tempResults.remove(tempResults.size() - 1);

                    KeyValue newKv = new KeyValue(CellUtil.cloneRow(cell),
                            family,
                            qualifier,
                            currentTimestamp,
                            newValue);

                    tempResults.add(newKv);

                } catch (Exception ex) {
                    logger.error("sm4Decrypt failed");
                    throw new IOException("Decryption failed", ex);
                }
            }
            // 添加到scan结果集，每次对Get修改后的结果
            newResults.add(Result.create(tempResults));
        }
        // 最后清理所有Result 将新的scan结果集插入
        results.clear();
        results.addAll(newResults);

        logger.info("postScannerNext stop");
        return hasMore;
    }


    public static byte[] sm4Encrypt(byte[] key, byte[] data, byte[] iv) throws Exception {
        SM4Engine sm4Engine = new SM4Engine();
        CipherParameters params = new ParametersWithIV(new KeyParameter(key), iv);

        BlockCipher cbc = new CBCBlockCipher(sm4Engine);
        PaddedBufferedBlockCipher cipher = new PaddedBufferedBlockCipher(cbc, new PKCS7Padding());
        cipher.init(true, params);

        byte[] output = new byte[cipher.getOutputSize(data.length)];
        int len = cipher.processBytes(data, 0, data.length, output, 0);
        len += cipher.doFinal(output, len);
        return output;
    }


    public static byte[] sm4Decrypt(byte[] key, byte[] cipherText, byte[] iv) throws Exception {
        SM4Engine sm4Engine = new SM4Engine();
        CipherParameters params = new ParametersWithIV(new KeyParameter(key), iv);

        BlockCipher cbc = new CBCBlockCipher(sm4Engine);
        PaddedBufferedBlockCipher cipher = new PaddedBufferedBlockCipher(cbc, new PKCS7Padding());
        cipher.init(false, params);

        byte[] output = new byte[cipher.getOutputSize(cipherText.length)];
        int len = cipher.processBytes(cipherText, 0, cipherText.length, output, 0);
        len += cipher.doFinal(output, len);

        byte[] plainText = new byte[len];
        System.arraycopy(output, 0, plainText, 0, len);

        return plainText;
    }



}
