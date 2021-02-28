package me.jiayizhang.sqlparser;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;

public class TaskParser {
    public static TaskMetaData ParseTask(String taskName, String taskFile) {
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.configBuilder()
                        .setParserFactory(FlinkSqlParserImpl.FACTORY)
                        .setCaseSensitive(false)
                        .setQuoting(Quoting.BACK_TICK)
                        .setQuotedCasing(Casing.TO_UPPER)
                        .setUnquotedCasing(Casing.TO_UPPER)
                        .build()).build();

        TaskMetaData task = new TaskMetaData(taskName);
        for (String x : taskFile.split(";")) {
            String stripedStr = x.strip();

            if (stripedStr.length() <= 0 || stripedStr.startsWith("--")) {
                continue;
            }
            ConnectorMetaData metaData = new ConnectorMetaData();
            try {
                SqlParser parser = SqlParser.create(stripedStr, config.getParserConfig());
                SqlNode root = parser.parseStmt();
                root.accept(FlinkSqlNodeTreeVisitor.ProduceFlinkVisitor(metaData));
                task.NewSqlCall(metaData);
            } catch (SqlParseException e) {
                System.out.println(stripedStr.length());
                System.out.println(stripedStr);
                e.printStackTrace();
            }
        }
        return task;
    }
}
