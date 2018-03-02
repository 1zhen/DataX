package com.alibaba.datax.plugin.writer.otswriter.model;

import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.model.Error;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 添加这个类的主要目的是为了解决当用户遇到CU不够时，打印大量的日志
 * @author redchen
 *
 */
public class LogExceptionManager {
    
    private long count = 0;
    private long updateTimestamp = 0;
    
    private static final Logger LOG = LoggerFactory.getLogger(LogExceptionManager.class);
    
    private synchronized void countAndReset() {
        count++;
        long cur = System.currentTimeMillis();
        long interval = cur - updateTimestamp;
        if (interval >= 10000) {
            LOG.warn("Call callable fail, OTSNotEnoughCapacityUnit, total times:"+ count +", time range:"+ (interval/1000) +"s, times per second:" + ((float)count / (interval/1000)));
            count = 0;
            updateTimestamp = cur;
        }
    }
    
    public synchronized void addException(Exception exception) {
        if (exception instanceof TableStoreException) {
            TableStoreException e = (TableStoreException)exception;
            if (e.getErrorCode().equals(ErrorCode.NOT_ENOUGH_CAPACITY_UNIT)) {
                countAndReset();
            } else {
                LOG.warn(
                        "Call callable fail, OTSException:ErrorCode:{}, ErrorMsg:{}, RequestId:{}", 
                        new Object[]{e.getErrorCode(), e.getMessage(), e.getRequestId()}
                        );
            }
        } else {
            LOG.warn("Call callable fail, {}", exception.getMessage());
        }
    }
    
    public synchronized void addException(Error error, String requestId) {
        if (error.getCode().equals(ErrorCode.NOT_ENOUGH_CAPACITY_UNIT)) {
            countAndReset();
        } else {
            LOG.warn(
                    "OTSException:ErrorCode:{}, ErrorMsg:{}, RequestId:{}", 
                    new Object[]{error.getCode(), error.getMessage(), requestId}
                    );
        }
    }
}
