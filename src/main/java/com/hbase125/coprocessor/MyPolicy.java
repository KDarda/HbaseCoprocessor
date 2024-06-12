package com.hbase125.coprocessor;

import org.bouncycastle.asn1.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class MyPolicy {

    private static final Logger logger = LoggerFactory.getLogger(MyPolicy.class);

    public static class PolicyInfo {
        private int policyType;
        private int policyVersion;

        private int dbType;
        private String dbVersion;
        private String dbUrl;
        private int dbPort;

        private String dbInstanceName;
        private String dbName;
        private String dbTableName;
        private String dbColumnName;
        private int dbColumnType;
        private String dbUserName;
        private String dbUserPasswd;

        private String keyId;
        private String keyAuthCode;

        private byte[] key;
        private byte[] iv;
        private int cipherType;
        private int hashType;

        public byte[] getKey() {
            return key;
        }

        public byte[] getIv() {
            return iv;
        }

        @Override
        public String toString() {
            return "PolicyInfo{" +
                    "policyType=" + policyType +
                    ", policyVersion=" + policyVersion +
                    ", dbType=" + dbType +
                    ", dbVersion='" + dbVersion + '\'' +
                    ", dbUrl='" + dbUrl + '\'' +
                    ", dbPort=" + dbPort +
                    ", dbInstanceName='" + dbInstanceName + '\'' +
                    ", dbName='" + dbName + '\'' +
                    ", dbTableName='" + dbTableName + '\'' +
                    ", dbColumnName='" + dbColumnName + '\'' +
                    ", dbColumnType=" + dbColumnType +
                    ", dbUserName='" + dbUserName + '\'' +
                    ", dbUserPasswd='" + dbUserPasswd + '\'' +
                    ", keyId='" + keyId + '\'' +
                    ", keyAuthCode='" + keyAuthCode + '\'' +
                    ", key=" + Arrays.toString(key) +
                    ", iv=" + Arrays.toString(iv) +
                    ", cipherType=" + cipherType +
                    ", hashType=" + hashType +
                    '}';
        }
    }

    private static PolicyInfo parsePolicy(byte[] data) throws IOException {
        ASN1InputStream asn1InputStream = new ASN1InputStream(data);
        ASN1Sequence sequence = (ASN1Sequence) asn1InputStream.readObject();
        asn1InputStream.close();

        PolicyInfo policyInfo = new PolicyInfo();

        int index = 0;
        policyInfo.policyType = ASN1Integer.getInstance(sequence.getObjectAt(index++)).getValue().intValue();
        policyInfo.policyVersion = ASN1Integer.getInstance(sequence.getObjectAt(index++)).getValue().intValue();

        policyInfo.dbType = ASN1Integer.getInstance(sequence.getObjectAt(index++)).getValue().intValue();
        policyInfo.dbVersion = new String(ASN1OctetString.getInstance(sequence.getObjectAt(index++)).getOctets());
        policyInfo.dbUrl = new String(ASN1OctetString.getInstance(sequence.getObjectAt(index++)).getOctets());
        policyInfo.dbPort = ASN1Integer.getInstance(sequence.getObjectAt(index++)).getValue().intValue();

        policyInfo.dbInstanceName = new String(ASN1OctetString.getInstance(sequence.getObjectAt(index++)).getOctets());
        policyInfo.dbName = new String(ASN1OctetString.getInstance(sequence.getObjectAt(index++)).getOctets());
        policyInfo.dbTableName = new String(ASN1OctetString.getInstance(sequence.getObjectAt(index++)).getOctets());
        policyInfo.dbColumnName = new String(ASN1OctetString.getInstance(sequence.getObjectAt(index++)).getOctets());
        policyInfo.dbColumnType = ASN1Integer.getInstance(sequence.getObjectAt(index++)).getValue().intValue();
        policyInfo.dbUserName = new String(ASN1OctetString.getInstance(sequence.getObjectAt(index++)).getOctets());
        policyInfo.dbUserPasswd = new String(ASN1OctetString.getInstance(sequence.getObjectAt(index++)).getOctets());

        policyInfo.keyId = new String(ASN1OctetString.getInstance(sequence.getObjectAt(index++)).getOctets());
        policyInfo.keyAuthCode = new String(ASN1OctetString.getInstance(sequence.getObjectAt(index++)).getOctets());

        policyInfo.key = ASN1OctetString.getInstance(sequence.getObjectAt(index++)).getOctets();
        policyInfo.iv = ASN1OctetString.getInstance(sequence.getObjectAt(index++)).getOctets();
        policyInfo.cipherType = ASN1Integer.getInstance(sequence.getObjectAt(index++)).getValue().intValue();
        policyInfo.hashType = ASN1Integer.getInstance(sequence.getObjectAt(index++)).getValue().intValue();

        return policyInfo;
    }

    public static class ASN1Request {
        private final String url;
        private final byte[] asn1Bytes;

        public ASN1Request(String url, byte[] asn1Bytes) {
            this.url = url;
            this.asn1Bytes = asn1Bytes;
        }

        public byte[] getAsn1Bytes() {return asn1Bytes;}
        public String getUrl() {return url;}
    }

    public static ASN1Request createASN1Request(String url, String pid) {

        if(url == null && pid == null) {
            logger.error("pid is null or url is null");
            return null;
        }

        ASN1EncodableVector vector = new ASN1EncodableVector();

        vector.add(new DEROctetString(pid.getBytes())); // Replace with your actual values

        vector.add(new ASN1Integer(32)); // Replace with your actual values
        vector.add(new DEROctetString("SQL Server2019-15.0.2000.5-64".getBytes())); // Replace with your actual values
        vector.add(new DEROctetString("db_instance_name".getBytes())); // Replace with your actual values
        vector.add(new DEROctetString("db_name".getBytes())); // Replace with your actual values
        vector.add(new DEROctetString("db_table_name".getBytes())); // Replace with your actual values
        vector.add(new DEROctetString("db_column_name".getBytes())); // Replace with your actual values
        vector.add(new DEROctetString("db_user_name".getBytes())); // Replace with your actual values

        byte[] asn1Bytes;
        DERSequence sequence = new DERSequence(vector);
        try {
            asn1Bytes = sequence.getEncoded();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return new ASN1Request(url, asn1Bytes);
    }

    public static PolicyInfo FindPolicy(ASN1Request asn1Request){

        String url = asn1Request.getUrl();
        byte[] asn1Bytes = asn1Request.getAsn1Bytes();

        // Send the ASN1 bytes via HTTP POST and get the response bytes
        byte[] responseBytes = HttpTools.sendPostRequest(url, asn1Bytes);
        if (responseBytes != null) {
            try {
                PolicyInfo newPolicyInfo = parsePolicy(responseBytes);

                logger.info("Received a new policy. Updating the current policy.");
                return newPolicyInfo;

            } catch (IOException e) {
                logger.error("Failed to parse the response bytes: {}", e.getMessage());
                return null;
            }
        } else {
            logger.error("Failed to receive response from the server.");
        }
        return null;
    }


}
