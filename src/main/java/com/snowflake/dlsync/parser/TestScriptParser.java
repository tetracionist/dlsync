package com.snowflake.dlsync.parser;

import com.snowflake.dlsync.models.TestScript;
import com.snowflake.dlsync.parser.antlr.SnowflakeParser;

import java.util.HashMap;
import java.util.Map;

public class TestScriptParser extends ScriptParser {
    private Map<String, String> cteMap = new HashMap<>();
    public TestScriptParser(TestScript testScript) {
        super(testScript.getContent());
        objectName = testScript.getObjectName();
    }

    @Override
    public void enterCommon_table_expression(SnowflakeParser.Common_table_expressionContext ctx) {
        String cteName = ctx.getChild(0).getText();
        var selectCtx = ctx.select_statement_in_parentheses();
        String cteQuery =  content.substring(selectCtx.getStart().getStartIndex(), selectCtx.getStop().getStopIndex()+1);
        cteMap.put(cteName.toUpperCase(), cteQuery);
    }

    @Override
    public void enterSelect_statement_in_parentheses(SnowflakeParser.Select_statement_in_parenthesesContext ctx) {
        if(ctx.getParent() instanceof SnowflakeParser.Query_statementContext) {
            mainQuery = content.substring(ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex()+1);
        }
    }

    public Map<String, String> getCteMap() {
        return cteMap;
    }

}
