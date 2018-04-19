package com.alibaba.datax.plugin.writer.httpwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Sets;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhangsihao on 2018/4/18.
 */
public class HttpWriter {
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration conf = null;

        @Override
        public void init() {
            LOG.info("init() begin");
            this.conf = this.getPluginJobConf();
            this.validateParameter();
            LOG.info("init() end");
        }

        private void validateParameter() {
            String url = this.conf.getNecessaryValue(Key.URL, HttpWriterErrorCode.REQUIRED_VALUE);
            String method = this.conf.getString(Key.METHOD, "POST").toUpperCase();
            if (!Sets.newHashSet("POST", "PUT").contains(method)) {
                throw DataXException.asDataXException(HttpWriterErrorCode.ILLEGAL_VALUE, String.format("仅支持POST和PUT方法，不支持您配置的方法：[%s]", method));
            }
            int batchSize = this.conf.getInt(Key.BATCH_SIZE, 1000);
            if (batchSize <= 0) {
                throw DataXException.asDataXException(HttpWriterErrorCode.ILLEGAL_VALUE, String.format("batchSize必须大于0，您配置了[%d]", batchSize));
            }
            List<String> column = this.conf.getList(Key.COLUMN, String.class);
            if (null == column || column.size() == 0) {
                throw DataXException.asDataXException(HttpWriterErrorCode.REQUIRED_VALUE, "必须配置列名");
            }
        }

        @Override
        public void destroy() {
            LOG.info("destroy()");
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> result = new ArrayList<Configuration>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i ++) {
                result.add(this.conf.clone());
            }
            return result;
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration conf = null;
        private String url;
        private String method;
        private Integer batchSize;
        private List<String> column;

        private CloseableHttpClient httpClient;
        private List<Object> buf;

        @Override
        public void init() {
            this.conf = this.getPluginJobConf();
            this.url = conf.getString(Key.URL);
            this.method = conf.getString(Key.METHOD, "POST");
            this.batchSize = conf.getInt(Key.BATCH_SIZE, 1000);
            this.column = conf.getList(Key.COLUMN, String.class);
            this.buf = new ArrayList<Object>(this.batchSize);
            httpClient = HttpClients.createDefault();
        }

        @Override
        public void destroy() {
            try {
                httpClient.close();
            } catch (IOException e) {
                LOG.warn("Failed to close http client.", e);
            }
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            Record record;
            while ((record = lineReceiver.getFromReader()) != null) {
                try {
                    Map<String, Object> body = new HashMap<String, Object>();
                    for (int i = 0; i < this.column.size(); i ++) {
                        Column column = record.getColumn(i);
                        String columnName = this.column.get(i);
                        body.put(columnName, column.getRawData());
                    }
                    addToBuf(body);
                } catch (Exception e) {
                    LOG.warn("Found dirty data.", e);
                    this.getTaskPluginCollector().collectDirtyRecord(record, e);
                }

                if (this.buf.size() >= this.batchSize) {
                    flush();
                }
            }
            if (this.buf.size() > 0) {
                flush();
            }
        }

        private void addToBuf(Object body) {
            this.buf.add(body);
        }

        private void flush() {
            CloseableHttpResponse response = null;
            try {
                HttpUriRequest request = null;
                if ("POST".equalsIgnoreCase(this.method)) {
                    request = new HttpPost(this.url);
                    ((HttpPost) request).setEntity(new StringEntity(JSON.toJSONString(this.buf)));
                } else if ("PUT".equalsIgnoreCase(this.method)) {
                    request = new HttpPut(this.url);
                    ((HttpPut) request).setEntity(new StringEntity(JSON.toJSONString(this.buf)));
                }
                this.buf = new ArrayList<Object>(this.batchSize);
                if (request == null) {
                    throw DataXException.asDataXException(HttpWriterErrorCode.ILLEGAL_VALUE, "非法的method");
                }
                request.setHeader("Content-Type", "application/json");

                response = httpClient.execute(request);
            } catch (Exception e) {
                LOG.error("Connection failed.", e);
            }
            if (response == null || response.getStatusLine() == null) {
                throw DataXException.asDataXException(HttpWriterErrorCode.CONNECTION_ERROR, "连接错误");
            } else if (response.getStatusLine().getStatusCode() >= 400) {
                throw DataXException.asDataXException(HttpWriterErrorCode.CONNECTION_ERROR,
                        String.format("连接错误 StatusCode:[%d]", response.getStatusLine().getStatusCode()));
            } else {
                try {
                    response.close();
                } catch (IOException e) {
                    LOG.warn("Failed to close connection.", e);
                }
            }
        }
    }
}
