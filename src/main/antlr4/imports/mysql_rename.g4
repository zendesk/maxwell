grammar mysql_rename;
import mysql_literal_tokens, mysql_idents;

rename_table: RENAME (TABLE | TABLES) rename_table_spec (',' rename_table_spec)*;
rename_table_spec: table_name TO table_name;

