import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.params.ParametersWithIV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.io.File;
import java.security.SecureRandom;

import org.bouncycastle.crypto.engines.SM4Engine;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.crypto.modes.CBCBlockCipher;
import org.bouncycastle.crypto.paddings.PaddedBufferedBlockCipher;
import org.bouncycastle.crypto.paddings.PKCS7Padding;


public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        try {
            // 指定XML文件路径
            File xmlFile = new File("/home/hbconf/extconfig.xml");

            // 创建DocumentBuilderFactory对象
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();

            // 解析XML文件并获取Document对象
            Document doc = dBuilder.parse(xmlFile);
            doc.getDocumentElement().normalize();

            // 获取所有table节点
            NodeList tableList = doc.getElementsByTagName("table");

            // 遍历每个table节点
            for (int i = 0; i < tableList.getLength(); i++) {
                Node tableNode = tableList.item(i);

                if (tableNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element tableElement = (Element) tableNode;

                    // 获取table节点的name属性值
                    String tableName = tableElement.getElementsByTagName("name").item(0).getTextContent();

                    System.out.println("Table Name: " + tableName);

                    // 获取所有family节点
                    NodeList familyList = tableElement.getElementsByTagName("family");

                    // 遍历每个family节点
                    for (int j = 0; j < familyList.getLength(); j++) {
                        Node familyNode = familyList.item(j);

                        if (familyNode.getNodeType() == Node.ELEMENT_NODE) {
                            Element familyElement = (Element) familyNode;

                            // 获取family节点的name属性值
                            String familyName = familyElement.getElementsByTagName("name").item(0).getTextContent();

                            System.out.println("Family Name: " + familyName);

                            // 获取所有Qualifier节点
                            NodeList qualifierList = familyElement.getElementsByTagName("Qualifier");

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

                                    System.out.println("Qualifier Name: " + qualifierName);
                                    System.out.println("Qualifier Url: " + qualifierUrl);
                                    System.out.println("Qualifier Pid: " + qualifierPid);
                                }
                            }
                        }
                    }
                }
            }

        } catch (Exception ignored) {
        }

        byte[] key = new byte[16]; // SM4密钥长度为16字节
        SecureRandom random = new SecureRandom();
        random.nextBytes(key);

        // 初始化加密器
        SM4Engine engine = new SM4Engine();
        PaddedBufferedBlockCipher cipher = new PaddedBufferedBlockCipher(new CBCBlockCipher(engine), new PKCS7Padding());
        KeyParameter keyParameter = new KeyParameter(key);
        byte[] iv = new byte[16]; // SM4初始化向量长度为16字节
        random.nextBytes(iv);

//        // 加密
//        cipher.init(true, new ParametersWithIV(keyParameter, iv));
//        byte[] plaintext = "Hello, World!".getBytes();
//        byte[] ciphertext = new byte[cipher.getOutputSize(plaintext.length)];
//        int len = cipher.processBytes(plaintext, 0, plaintext.length, ciphertext, 0);
//        try {
//            cipher.doFinal(ciphertext, len);
//        } catch (InvalidCipherTextException e) {
//            throw new RuntimeException(e);
//        }
//
//        System.out.println("Ciphertext: " + bytesToHex(ciphertext));
//
//        // 解密
//        cipher.init(false, new ParametersWithIV(keyParameter, iv));
//        byte[] decryptedText = new byte[cipher.getOutputSize(ciphertext.length)];
//        len = cipher.processBytes(ciphertext, 0, ciphertext.length, decryptedText, 0);
//        try {
//            cipher.doFinal(decryptedText, len);
//        } catch (InvalidCipherTextException e) {
//            throw new RuntimeException(e);
//        }
//
//        System.out.println("Decrypted Text: " + new String(decryptedText).trim());

    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
        }
        return sb.toString();
    }
}


