package com.snowflake.dlsync.parser;

import com.snowflake.dlsync.parser.antlr.SnowflakeLexer;
import com.snowflake.dlsync.parser.antlr.SnowflakeParser;
import com.snowflake.dlsync.parser.antlr.SnowflakeParserBaseListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.HashSet;
import java.util.Set;


public class ScriptParser extends SnowflakeParserBaseListener {
    protected String content;
    protected String objectName;
    protected String mainQuery;
    protected Set<String> objectReferences = new HashSet<>();
    public ScriptParser(String content) {
        this.content = content;
    }

    public void parse() {
        SnowflakeLexer lexer = new SnowflakeLexer(CharStreams.fromString(content));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SnowflakeParser parser = new SnowflakeParser(tokens);
        ParseTree tree = parser.snowflake_file();
        ParseTreeWalker walker = new ParseTreeWalker();
        walker.walk(this, tree);
    }

    @Override
    public void enterObject_name(SnowflakeParser.Object_nameContext ctx) {
        objectReferences.add(ctx.getText());
    }

    public String getContent() {
        return content;
    }

    public String getObjectName() {
        return objectName;
    }

    public String getMainQuery() {
        return mainQuery;
    }

    public Set<String> getObjectReferences() {
        return objectReferences;
    }
}
