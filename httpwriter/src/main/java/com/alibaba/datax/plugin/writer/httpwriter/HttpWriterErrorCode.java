package com.alibaba.datax.plugin.writer.httpwriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by zhangsihao on 2018/4/18.
 */
public enum HttpWriterErrorCode implements ErrorCode {
    REQUIRED_VALUE("HttpWriter-00", "您缺失了必须填写的参数值。"),
    ILLEGAL_VALUE("HttpWriter-01", "您填写的参数值不合法。"),
    CONNECTION_ERROR("HttpWriter-02", "连接错误。");

    private final String code;
    private final String description;

    HttpWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code, this.description);
    }
}
