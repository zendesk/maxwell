grammar mysql_drop;
import mysql_literal_tokens, mysql_idents;

drop_database: DROP (DATABASE | SCHEMA) if_exists? name;
drop_table: DROP TEMPORARY? TABLE if_exists? table_name (',' table_name)* drop_table_options*;
drop_table_options: (RESTRICT | CASCADE);

if_exists: IF EXISTS;
