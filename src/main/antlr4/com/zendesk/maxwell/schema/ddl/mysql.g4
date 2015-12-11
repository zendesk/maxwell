grammar mysql;

import mysql_literal_tokens, mysql_idents, mysql_alter_database, mysql_alter_table, mysql_create_database, mysql_create_table, mysql_drop, mysql_rename;

parse: statement?
       EOF;

statement:
    alter_table
  | alter_database
  | create_database
  | create_table
  | drop_database
  | drop_table
  | rename_table
  | BEGIN
  ;
