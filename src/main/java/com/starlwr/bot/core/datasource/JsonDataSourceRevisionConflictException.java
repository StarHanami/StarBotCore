package com.starlwr.bot.core.datasource;

import com.starlwr.bot.core.exception.DataSourceException;

public class JsonDataSourceRevisionConflictException extends DataSourceException {
    private final String expectedRevision;
    private final String actualRevision;

    public JsonDataSourceRevisionConflictException(String expectedRevision, String actualRevision) {
        super("JSON 数据源已被其他进程修改");
        this.expectedRevision = expectedRevision;
        this.actualRevision = actualRevision;
    }

    public String getExpectedRevision() { return expectedRevision; }
    public String getActualRevision() { return actualRevision; }
}
