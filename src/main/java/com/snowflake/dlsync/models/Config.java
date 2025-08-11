package com.snowflake.dlsync.models;

import lombok.Data;

import java.util.List;
import java.util.Properties;

@Data
public class Config {
    private String version;
    private Boolean continueOnFailure;
    private List<String> scriptExclusion;
    private List<DependencyOverride> dependencyOverride;
    private List<String> configTables;
    private Properties connection;

    public boolean isScriptExcluded(Script script) {
        if(scriptExclusion == null) {
            return false;
        }
        return scriptExclusion.contains(script.getFullObjectName());
    }
    

    public boolean isContinueOnFailure() {
        return continueOnFailure != null && continueOnFailure;
    }
}