# MysqlDdlGenerator
根据视图sql生成对应物理表的ddl语句

编辑start.bat的ip/port/schema/dbUsername/dbPwd参数，编辑sql.conf输入要分析的sql文本，执行start.bat脚本，生成ddl.txt
查询字段如果不是简单字段 字段类型会默认为varchar(12345)