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
import org.bouncycastle.util.encoders.Hex;
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

import com.hbase125.coprocessor.MyConfigure;

public class MyCoprocessor extends BaseRegionObserver {

    private static final Logger logger = LoggerFactory.getLogger(MyCoprocessor.class);

    private MyConfigure configure = new MyConfigure("/home/hbconf/extconfig.xml");

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        //ReadFile("/home/hbconf/extconfig.xml","table","family","Qualifier");
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        configure = null;
    }

    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e,
                       final Put put, final WALEdit edit, final Durability durability) throws IOException {
        logger.info("prePut start");
        // 获取当前操作的表名
        String tableName = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();

        MyConfigure.Table table = null;
        for (MyConfigure.Table t : configure.getConfiguration().getTables()) {
            if (t.getName().equals(tableName)) {
                table = t;
                break;
            }
        }
        if (table == null) {
            return;
        }

        MyConfigure.Family tableFamily = null;
        // 获取当前 Put 对象中所有列族及其对应的列
        NavigableMap<byte[], List<Cell>> familyCellMap = put.getFamilyCellMap();
        // 遍历所有列族及其对应的列
        for (Map.Entry<byte[], List<Cell>> entry : familyCellMap.entrySet()) {
            byte[] family = entry.getKey();  // 获取列族

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
            // 获取列族中的所有列
            List<Cell> cells = entry.getValue();
            // 遍历列族中的所有列
            for (Cell cell : cells) {
                byte[] qualifier = CellUtil.cloneQualifier(cell);  // 获取列名

                for (MyConfigure.Qualifier q : tableFamily.getQualifiers()) {
                    if (q.getName().equals(Bytes.toString(qualifier))) {
                        configQualifier = q;
                        break;
                    }
                }
                if (configQualifier == null) {
                    continue;
                }

                logger.info("Encrypt start");
                byte[] value = CellUtil.cloneValue(cell);  // 获取列的值
                String keyHex = "0123456789ABCDEFFEDCBA9876543210";
                byte[] keyBytes = Hex.decode(keyHex);
                byte[] ivBytes = "1111111111111111".getBytes();

                try {
                    byte[] newValue = sm4Encrypt(keyBytes, value, ivBytes);

                    // 创建一个新的 Put 对象，用于保存修改后的值
                    Put newPut = new Put(put.getRow());
                    // 将修改后的值添加到新的 Put 对象中
                    newPut.addColumn(family, qualifier, newValue);
                    // 清除原来的 Put 对象中的值，替换为修改后的值
                    put.getFamilyCellMap().clear();
                    put.getFamilyCellMap().putAll(newPut.getFamilyCellMap());

                } catch (Exception ignored) {
                    logger.error("sm4Encrypt failed", ignored);
                    return;
                }
                logger.info("Encrypt end");
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


/*
    public static void ReadFile(String filepath, String RootKey, String ItemKey, String QualifierKey) {

        File xmlFile = new File(filepath);
        try {
            // 创建DocumentBuilderFactory对象
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();

            // 解析XML文件并获取Document对象
            Document doc = dBuilder.parse(xmlFile);
            doc.getDocumentElement().normalize();

            // 获取所有table节点
            NodeList tableList = doc.getElementsByTagName(RootKey);

            // 遍历每个table节点
            for (int i = 0; i < tableList.getLength(); i++) {
                Node tableNode = tableList.item(i);

                if (tableNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element tableElement = (Element) tableNode;

                    // 获取table节点的name属性值
                    String tableName = tableElement.getElementsByTagName("name").item(0).getTextContent();

                    logger.info("Table Name: {}", tableName);

                    // 获取所有family节点
                    NodeList familyList = tableElement.getElementsByTagName(ItemKey);

                    // 遍历每个family节点
                    for (int j = 0; j < familyList.getLength(); j++) {
                        Node familyNode = familyList.item(j);

                        if (familyNode.getNodeType() == Node.ELEMENT_NODE) {
                            Element familyElement = (Element) familyNode;

                            // 获取family节点的name属性值
                            String familyName = familyElement.getElementsByTagName("name").item(0).getTextContent();

                            logger.info("Family Name: {}", familyName);

                            // 获取所有Qualifier节点
                            NodeList qualifierList = familyElement.getElementsByTagName(QualifierKey);

                            // 遍历每个Qualifier节点
                            for (int k = 0; k < qualifierList.getLength(); k++) {
                                Node qualifierNode = qualifierList.item(k);

                                if (qualifierNode.getNodeType() == Node.ELEMENT_NODE) {
                                    Element qualifierElement = (Element) qualifierNode;

                                    // 获取Qualifier节点的属性值
                                    String qualifierName = qualifierElement.getElementsByTagName("name").item(0).getTextContent();
                                    String qualifierUrl = qualifierElement.getElementsByTagName("url").item(0).getTextContent();
                                    String qualifierPid = qualifierElement.getElementsByTagName("pid").item(0).getTextContent();
                                    // 输出读取的属性
                                    logger.info("Qualifier Name: {}", qualifierName);
                                    logger.info("Qualifier Url: {}", qualifierUrl);
                                    logger.info("Qualifier Pid: {}", qualifierPid);
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception ignored) {

        }

    }
*/

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
        return output;
    }

}
