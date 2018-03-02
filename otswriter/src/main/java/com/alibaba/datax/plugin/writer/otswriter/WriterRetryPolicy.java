package com.alibaba.datax.plugin.writer.otswriter;

import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public class WriterRetryPolicy implements RetryStrategy {
    OTSConf conf;

    public WriterRetryPolicy(OTSConf conf) {
        this.conf = conf;
    }

    @Override
    public RetryStrategy clone() {
        return new WriterRetryPolicy(conf);
    }

    @Override
    public int getRetries() {
        return conf.getRetry();
    }

    @Override
    public long nextPause(String s, Exception e) {
        return System.currentTimeMillis() + conf.getSleepInMillisecond();
    }
}
