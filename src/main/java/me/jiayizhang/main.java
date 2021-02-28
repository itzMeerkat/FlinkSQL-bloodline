package me.jiayizhang;


import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


public class main {
    static void ReadAndParse(Path path) {
//        System.out.println(path.toString());
        try {
            String content = new String(Files.readAllBytes(path));
            String taskName = path.getFileName().toString();
            System.out.printf("****************** %s ******************\n", taskName);
            TaskMetaData task = TaskParser.ParseTask(taskName, content);


            task.GetSources().forEach((e)-> System.out.println(e.MakeKey()));
            task.GetSinks().forEach((e)-> System.out.println(e.MakeKey()));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    public static void main(String[] args) {
        String path = "";
        try {
            Files.walk(Paths.get(path))
                    .filter((n) -> n.toString().endsWith(".sql"))
                    .forEach(main::ReadAndParse);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}