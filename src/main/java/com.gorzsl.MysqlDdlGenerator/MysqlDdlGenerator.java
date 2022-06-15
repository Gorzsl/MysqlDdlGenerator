package com.gorzsl.MysqlDdlGenerator;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author YangWeiDong
 * @date 2022年06月08日 15:01
 */
public class MysqlDdlGenerator {
    public static void main(String[] args) throws IOException {
        if (args.length != 6){
            System.out.println("参数错误");
            return;
        }
        String ip = args[0];
        String port = args[1];
        String schema = args[2];
        String username = args[3];
        String password = args[4];
        String sqlUrl = args[5];
        String sql = new String(Files.readAllBytes(Paths.get(sqlUrl)));

        MysqlDdlGenerator generator = new MysqlDdlGenerator(ip, port, schema, username, password);
        String ddl = generator.getDdl(sql);

        File file =new File("ddl.txt");
        if(!file.exists()){
            file.createNewFile();
        }
        FileWriter fileWriter = new FileWriter(file.getName(),true);
        fileWriter.write(ddl);
        fileWriter.close();
    }

    private DruidDataSource dataSource;
    private String schema;
    private final String defaultType = "varchar(12345)";

    public MysqlDdlGenerator(String ip, String port, String schema, String username, String password) {
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://"+ip+":"+port+"/"+schema+"?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Hongkong");
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setInitialSize(1);
        dataSource.setMinIdle(1);
        dataSource.setMaxActive(30);
        dataSource.setMaxWait(60000);
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        dataSource.setMinEvictableIdleTimeMillis(300000);

        this.schema = schema;
    }

    public String getDdl(String sql){
        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement statement = (SQLSelectStatement) parser.parseStatement();
        MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) statement.getSelect().getQuery();

        SQLTableSource sqlTableSource = sqlSelectQueryBlock.getFrom();
        Map<String, TableItem> tableMap = new HashMap<>();
        getTableAliasMap(sqlTableSource, tableMap, null);

        for (TableItem tableItem : tableMap.values()) {
            initColumn(tableItem);
        }
        List<ColumnItem> selectItem = collectSelectQueryFields(sqlSelectQueryBlock, tableMap);

        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE `Untitled` (");
        for (ColumnItem columnItem : selectItem) {
            ddl.append("`").append(columnItem.getName()).append("` ").append(columnItem.getType()).append(",");
        }
        ddl.delete(ddl.length()-1, ddl.length());
        ddl.append(");");
        return ddl.toString();
    }

    private void getTableAliasMap(SQLTableSource sqlTableSource, Map<String, TableItem> map, TableItem tableItemTmp){
        if (sqlTableSource instanceof SQLExprTableSource) {
            SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) sqlTableSource;
            TableItem tableItem;
            if (tableItemTmp != null) {
                tableItem = tableItemTmp;
            } else {
                tableItem = new TableItem();
            }
            tableItem.setSchema(sqlExprTableSource.getSchema());
            tableItem.setTableName(sqlExprTableSource.getTableName());
            if (tableItem.getAlias() == null) {
                tableItem.setAlias(sqlExprTableSource.getAlias());
            }
            map.put(tableItem.getAlias(), tableItem);
        } else if (sqlTableSource instanceof SQLJoinTableSource) {
            SQLJoinTableSource sqlJoinTableSource = (SQLJoinTableSource) sqlTableSource;
            SQLTableSource leftTableSource = sqlJoinTableSource.getLeft();
            getTableAliasMap(leftTableSource, map, null);
            SQLTableSource rightTableSource = sqlJoinTableSource.getRight();
            getTableAliasMap(rightTableSource, map, null);
        } else if (sqlTableSource instanceof SQLSubqueryTableSource) {
            SQLSubqueryTableSource subQueryTableSource = (SQLSubqueryTableSource) sqlTableSource;
            MySqlSelectQueryBlock sqlSelectQuery = (MySqlSelectQueryBlock) subQueryTableSource.getSelect().getQuery();
            TableItem tableItem;
            if (tableItemTmp != null) {
                tableItem = tableItemTmp;
            } else {
                tableItem = new TableItem();
            }
            getTableAliasMap(sqlSelectQuery.getFrom(), map, tableItem);
            tableItem.setAlias(subQueryTableSource.getAlias());
        }
    }

    private void initColumn(TableItem tableItem) {
        String sql = "select column_name name, column_type type from information_schema.columns where table_name = '"+tableItem.getTableName()+"' and table_schema = '"+(tableItem.getSchema()==null?schema:tableItem.getSchema())+"' order by ordinal_position";
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement pstmt = conn
                    .prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                pstmt.setFetchSize(Integer.MIN_VALUE);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        ColumnItem columnItem = new ColumnItem();
                        columnItem.setName(rs.getString("name"));
                        columnItem.setType(rs.getString("type"));
                        tableItem.getColumnItems().add(columnItem);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<ColumnItem> collectSelectQueryFields(MySqlSelectQueryBlock sqlSelectQueryBlock, Map<String, TableItem> tableMap) {
        return sqlSelectQueryBlock.getSelectList().stream().map(selectItem -> {
            ColumnItem fieldItem = new ColumnItem();
            fieldItem.setName(selectItem.getAlias());
            if (selectItem.getExpr() instanceof SQLPropertyExpr){
                SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) selectItem.getExpr();
                if (fieldItem.getName() == null){
                    fieldItem.setName(sqlPropertyExpr.getName());
                }

                String tableAlias = sqlPropertyExpr.getOwnernName();
                TableItem tableItem = tableMap.get(tableAlias);
                if (tableItem != null){
                    for (ColumnItem columnItem : tableItem.getColumnItems()) {
                        if (sqlPropertyExpr.getName().equalsIgnoreCase(columnItem.getName())){
                            fieldItem.setType(columnItem.getType());
                            break;
                        }
                    }
                }
            } else {
                fieldItem.setType(defaultType);
            }
            return fieldItem;
        }).collect(Collectors.toList());
    }

    public static class TableItem {
        private String schema;
        private String tableName;
        private String alias;
        private List<ColumnItem> columnItems = new ArrayList<>();

        public String getSchema() {
            return schema;
        }

        public void setSchema(String schema) {
            this.schema = schema;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getAlias() {
            return alias;
        }

        public void setAlias(String alias) {
            this.alias = alias;
        }

        public List<ColumnItem> getColumnItems() {
            return columnItems;
        }
    }

    public static class ColumnItem {
        private String name;
        private String type;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }
}
