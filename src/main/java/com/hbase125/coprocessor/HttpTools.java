package com.hbase125.coprocessor;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HttpTools {
    private static final Logger logger = LoggerFactory.getLogger(HttpTools.class);

    public static CloseableHttpClient getHttpClient() {

        return HttpClients.createDefault();
    }

    public static byte[] sendPostRequest(String url, byte[] asn1Bytes) {
        HttpPost post = new HttpPost(url);
        HttpEntity entity = new ByteArrayEntity(asn1Bytes);
        post.setEntity(entity);
        post.setHeader("Content-type", "application/octet-stream");

        try (CloseableHttpResponse response = getHttpClient().execute(post)) {
            if (response.getStatusLine().getStatusCode() == 200) {

                HttpEntity responseEntity = response.getEntity();
                if (responseEntity != null) {
                    return EntityUtils.toByteArray(responseEntity);
                }
            } else {
                logger.error("Response Code: {}", response.getStatusLine().getStatusCode());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }
}
