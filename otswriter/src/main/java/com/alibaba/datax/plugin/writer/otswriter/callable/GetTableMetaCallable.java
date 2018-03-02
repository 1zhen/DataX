package com.alibaba.datax.plugin.writer.otswriter.callable;

import java.util.concurrent.Callable;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.alicloud.openservices.tablestore.model.TableMeta;

public class GetTableMetaCallable implements Callable<TableMeta>{

    private SyncClient ots = null;
    private String tableName = null;
    
    public GetTableMetaCallable(SyncClient ots, String tableName) {
        this.ots = ots;
        this.tableName = tableName;
    }
    
    @Override
    public TableMeta call() throws Exception {
        DescribeTableRequest describeTableRequest = new DescribeTableRequest();
        describeTableRequest.setTableName(tableName);
        DescribeTableResponse result = ots.describeTable(describeTableRequest);
        return result.getTableMeta();
    }

}
