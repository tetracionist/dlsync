package com.snowflake.dlsync.models;

public class StateScript extends Script {


    public StateScript(String scriptPath, String databaseName, String schemaName, String objectName, ScriptObjectType objectType, String content) {
        super(scriptPath, databaseName, schemaName, objectName, objectType, content);
    }
    public StateScript(String databaseName, String schemaName, String objectName, ScriptObjectType objectType, String content) {
        this(null, databaseName, schemaName, objectName, objectType, content);
    }


    @Override
    public String getId() {
        return getFullObjectName();
    }
}
