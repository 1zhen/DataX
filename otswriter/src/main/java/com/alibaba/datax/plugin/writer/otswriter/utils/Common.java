package com.alibaba.datax.plugin.writer.otswriter.utils;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.plugin.writer.otswriter.model.*;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;
import org.apache.commons.math3.util.Pair;

public class Common {

    public static String getDetailMessage(Exception exception) {
        if (exception instanceof TableStoreException) {
            TableStoreException e = (TableStoreException) exception;
            return "OTSException[ErrorCode:" + e.getErrorCode() + ", ErrorMessage:" + e.getMessage() + ", RequestId:" + e.getRequestId() + "]";
        } else if (exception instanceof ClientException) {
            ClientException e = (ClientException) exception;
            return "ClientException[ErrorCode:" + e.getTraceId() + ", ErrorMessage:" + e.getMessage() + "]";
        } else if (exception instanceof IllegalArgumentException) {
            IllegalArgumentException e = (IllegalArgumentException) exception;
            return "IllegalArgumentException[ErrorMessage:" + e.getMessage() + "]";
        } else {
            return "Exception[ErrorMessage:" + exception.getMessage() + "]";
        }
    }

    public static PrimaryKey getPKFromRecord(List<OTSPKColumn> pkColumns, Record r) {
        PrimaryKeyBuilder pkBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        int pkCount = pkColumns.size();
        for (int i = 0; i < pkCount; i++) {
            Column col = r.getColumn(i);
            OTSPKColumn expect = pkColumns.get(i);

            if (col.getRawData() == null) {
                throw new IllegalArgumentException(String.format(OTSErrorMessage.PK_COLUMN_VALUE_IS_NULL_ERROR, expect.getName()));
            }
            PrimaryKeyValue pk = ColumnConversion.columnToPrimaryKeyValue(col, expect);
            pkBuilder.addPrimaryKeyColumn(expect.getName(), pk);
        }
        return pkBuilder.build();
    }

    public static List<Pair<String, ColumnValue>> getAttrFromRecord(int pkCount, List<OTSAttrColumn> attrColumns, Record r) {
        List<Pair<String, ColumnValue>> attr = new ArrayList<Pair<String, ColumnValue>>(r.getColumnNumber());
        for (int i = 0; i < attrColumns.size(); i++) {
            Column col = r.getColumn(i + pkCount);
            OTSAttrColumn expect = attrColumns.get(i);

            if (col.getRawData() == null) {
                attr.add(new Pair<String, ColumnValue>(expect.getName(), null));
                continue;
            }

            ColumnValue cv = ColumnConversion.columnToColumnValue(col, expect);
            attr.add(new Pair<String, ColumnValue>(expect.getName(), cv));
        }
        return attr;
    }

    public static RowChange columnValuesToRowChange(String tableName, OTSOpType type, PrimaryKey pk, List<Pair<String, ColumnValue>> values) {
        switch (type) {
            case PUT_ROW:
                RowPutChangeWithRecord rowPutChange = new RowPutChangeWithRecord(tableName);
                rowPutChange.setPrimaryKey(pk);

                for (Pair<String, ColumnValue> en : values) {
                    if (en.getValue() != null) {
                        rowPutChange.addColumn(en.getKey(), en.getValue());
                    } 
                }

                return rowPutChange;
            case UPDATE_ROW:
                RowUpdateChangeWithRecord rowUpdateChange = new RowUpdateChangeWithRecord(tableName);
                rowUpdateChange.setPrimaryKey(pk);

                for (Pair<String, ColumnValue> en : values) {
                    if (en.getValue() != null) {
                        rowUpdateChange.put(en.getKey(), en.getValue());
                    } else {
                        rowUpdateChange.deleteColumns(en.getKey());
                    }
                }
                return rowUpdateChange;
            case DELETE_ROW:
                RowDeleteChangeWithRecord rowDeleteChange = new RowDeleteChangeWithRecord(tableName);
                rowDeleteChange.setPrimaryKey(pk);
                return rowDeleteChange;
            default:
                throw new IllegalArgumentException(String.format(OTSErrorMessage.UNSUPPORT_PARSE, type, "RowChange"));
        }
    }

    public static long getDelaySendMilliseconds(int hadRetryTimes, int initSleepInMilliSecond) {

        if (hadRetryTimes <= 0) {
            return 0;
        }

        int sleepTime = initSleepInMilliSecond;
        for (int i = 1; i < hadRetryTimes; i++) {
            sleepTime += sleepTime;
            if (sleepTime > 30000) {
                sleepTime = 30000;
                break;
            } 
        }
        return sleepTime;
    }
}
