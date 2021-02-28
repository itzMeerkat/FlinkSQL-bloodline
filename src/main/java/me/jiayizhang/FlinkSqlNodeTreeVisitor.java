package me.jiayizhang;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.util.SqlVisitor;

import java.util.Arrays;

public class FlinkSqlNodeTreeVisitor<R> implements SqlVisitor<R> {
    private final ConnectorMetaData metaData;
    private String currentKey;

    public FlinkSqlNodeTreeVisitor(ConnectorMetaData m) {
        metaData = m;
    }

    public static FlinkSqlNodeTreeVisitor ProduceFlinkVisitor(ConnectorMetaData meta) {
        return new FlinkSqlNodeTreeVisitor(meta);
    }

    @Override
    public R visit(SqlLiteral sqlLiteral) {
        System.out.println("Should not occur");
        return null;
    }


    @Override
    public R visit(SqlCall sqlCall) {
//        System.out.printf("%s -> %s\n", metaData.sqlCallType,sqlCall.getKind().lowerName);
        if (sqlCall.getKind().lowerName.equals("create_table") || sqlCall.getKind().lowerName.equals("insert"))
            metaData.sqlCallType = sqlCall.getKind().lowerName;

        //            System.out.println(metaData.Name);
        switch (sqlCall.getKind().lowerName) {
            case "create_table" -> {
                metaData.Name = sqlCall.getOperandList().get(0).toString();
                SqlNode x = sqlCall.getOperandList().get(3);
                x.accept(this);
            }
            case "insert" -> {
                SqlNode to = sqlCall.getOperandList().get(1);
                metaData.Name = to.toString();
                return null;
            }
            case "create_view" -> {
                System.out.printf("Skipping CREATE VIEW %s\n", sqlCall.getOperandList().get(0).toString());
                return null;
            }
            case "other" -> {
                SqlNode key = sqlCall.getOperandList().get(0);
                SqlNode val = sqlCall.getOperandList().get(1);
                String k,v;
                k = key.toString().replace("'","").strip();
                v = val.toString().replace("'","").strip();
                metaData.parseArgument(k,v);
            }
        }
        return null;
    }


    @Override
    public R visit(SqlNodeList sqlNodeList) {
        for (SqlNode n : sqlNodeList) {
            n.accept(this);
        }
        return null;
    }

    @Override
    public R visit(SqlIdentifier sqlIdentifier) {
//        System.out.printf("Identifier %s\n", sqlIdentifier.toString());
        return null;
    }

    @Override
    public R visit(SqlDataTypeSpec sqlDataTypeSpec) {
//        System.out.println("sqlDataTypeSpec");
//        System.out.println(sqlDataTypeSpec.toString());
        return null;
    }

    @Override
    public R visit(SqlDynamicParam sqlDynamicParam) {
//        System.out.println("sqlDynamicParam");
        return null;
    }

    @Override
    public R visit(SqlIntervalQualifier sqlIntervalQualifier) {
//        System.out.println("sqlIntervalQualifier");
        return null;
    }
}