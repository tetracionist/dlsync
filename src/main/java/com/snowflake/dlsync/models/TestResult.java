package com.snowflake.dlsync.models;

import lombok.Data;

@Data
public class TestResult {
    private String result;
    private String message;

    public TestResult(String result, String message) {
        this.result = result;
        this.message = message;
    }

    public TestResult(Exception e) {
        this.result = "ERROR";
        this.message = e.getMessage();
    }

}
