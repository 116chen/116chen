package com.example.learningjava;


import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Sql2ThriftStruct {

    public static void main(String[] args) throws IOException {
        String table = "CREATE TABLE `t_capacity_project_sub_type`\n" +
                "(\n" +
                "    `id`          bigint      not null COMMENT '主键',\n" +
                "    `project_id`  bigint      not null COMMENT '活动id',\n" +
                "    `type_id`     varchar(32) null     default '' COMMENT '[行业|产业带类目|活动基地仓]id',\n" +
                "    `sub_type_id` varchar(32) null     default '' COMMENT '活动对应的[行业|产业带类目|活动基地仓]二级类目id',\n" +
                "    `create_time` timestamp   null     default CURRENT_TIMESTAMP COMMENT '创建时间',\n" +
                "    `update_time` timestamp   not null default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP comment '更新时间',\n" +
                "    PRIMARY KEY (`id`),\n" +
                "    UNIQUE KEY `uk_project_id_sub_type_type` (`project_id`, `sub_type_id`, `type_id`)\n" +
                ") ENGINE = InnoDB\n" +
                "  DEFAULT CHARSET = utf8m4 comment ='活动附加信息表';";

        String tableNamePrefix = "t_capacity_";


        List<String> ignoreFieldString = Arrays.asList("capacity_task_id", "update_time");

        Sql2ThriftStruct sql2ThriftStruct = new Sql2ThriftStruct();
        String res = sql2ThriftStruct.convert2ThriftStruct(table, tableNamePrefix, ignoreFieldString);
        System.out.println(res);
    }

    public List<String> splitStringLine(String stringSql) {
        String[] split = stringSql.split("\n");
        return Arrays.stream(split).collect(Collectors.toList());
    }

    public String convert2ThriftStruct(String file, String tablePrefix, List<String> ignoreFieldList) throws IOException {

        List<String> stringList = splitStringLine(file);

        if (stringList.isEmpty()) {
            return "";
        }

        StructBuilder structBuilder = new StructBuilder();
        for (int i = 0; i < stringList.size(); i++) {
            if (i == 0) {
                String structNameFromTableName = getStructNameFromTableName(stringList.get(0), tablePrefix);
                structBuilder.setTableName(structNameFromTableName);
            }

            String s = stringList.get(i);
            {
                String tableRemark = tableRemark(s);
                if (tableRemark != null && tableRemark.length() > 0) {
                    structBuilder.setTableRemark(tableRemark);
                    continue;
                }
            }

            {
                String typeName = typeName(s);
                if (typeName.equals("false")) {
                    continue;
                }
                String fieldName = fieldName(s);
                if (ignoreFieldList.contains(fieldName) || ignoreFieldList.contains(cameCase(fieldName))) {
                    continue;
                }
                structBuilder.addField(new Field(require(s), typeName, cameCase(fieldName), remark(s)));
            }

        }
        return structBuilder.build();
    }

    public String require(String line) {
        return line.contains("NOT NUL") ? "required" : "optional";
    }

    public String tableRemark(String line) {
        if (line.contains("ENGINE")) {
            String contentFromKeywords = getContentFromKeywords(line, "'(.*)'");
            return contentFromKeywords;
        }
        return "";
    }

    public String typeName(String line) {
        if (line.contains("bigint")) {
            return "i64";
        } else if (line.contains("tinyint")) {
            return "i32";
        } else if (line.contains("varchar")) {
            return "string";
        } else if (line.contains("timestamp")) {
            return "i64";
        } else if (line.contains("int")) {
            return "i32";
        }
        return "false";
    }

    public String remark(String line) {
        return getContentFromKeywords(line, "COMMENT.*'(.*)'");
    }

    public String fieldName(String line) {
        String contentFromKeywords = getContentFromKeywords(line, "`(.*)`");
        return contentFromKeywords;
    }

    public String cameCase(String raw) {
        String[] s = raw.split("_");
        if (s.length == 1) {
            return s[0];
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append(s[0]);
            for (int i = 1; i < s.length; i++) {
                sb.append(s[i].substring(0, 1).toUpperCase());
                if (s[i].length() > 1) {
                    sb.append(s[i].substring(1));
                }
            }
            return sb.toString();
        }
    }

    public String getStructNameFromTableName(String firstSqlLine, String tablePrefix) {
        String contentFromKeywords = getContentFromKeywords(firstSqlLine, "`(.*)`");
        String replace = contentFromKeywords.replace(tablePrefix, "");
        String s = cameCase(replace);
        s = s.substring(0, 1).toUpperCase() + s.substring(1);
        return s;
    }


    /**
     * 从字符串中提取数据
     *
     * @param string
     * @param pattern
     * @return
     */
    public String getContentFromKeywords(String string, String pattern) {
        Pattern compile = Pattern.compile(pattern);
        Matcher matcher = compile.matcher(string);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }


    class StructBuilder {

        int seq = 1;

        String tableRemark;

        String tableName;

        List<Field> fieldList;

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public void setTableRemark(String tableRemark) {
            this.tableRemark = tableRemark;
        }

        public void addField(Field field) {
            if (fieldList == null) {
                fieldList = new ArrayList<>();
            }
            field.seq = seq++;
            fieldList.add(field);
        }

        public String build() {
            if (tableName == null || fieldList == null) {
                return "error";
            }
            StringBuilder sb = new StringBuilder();
            sb.append("//").append(tableRemark).append("\r\n");
            sb.append("struct ").append(tableName).append("{").append("\r\n");
            for (int i = 0; i < fieldList.size(); i++) {
                Field field = fieldList.get(i);
                sb.append("    ").append(field.seq).append(":").append(field.require).append(" ").append(field.typeName).append(" ").append(field.fieldName).append(" ").append("//").append(field.remark).append("\r\n");
            }
            sb.append("}");
            return sb.toString();
        }
    }

    class Field {
        int seq;
        String require;
        String typeName;
        String fieldName;
        String remark;

        public Field(String require, String typeName, String fieldName, String remark) {
            this.require = require;
            this.typeName = typeName;
            this.fieldName = fieldName;
            this.remark = remark;
        }
    }
}
