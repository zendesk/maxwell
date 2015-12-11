grammar mysql;

import mysql_literal_tokens,
       mysql_idents,
       mysql_alter_table,
       mysql_alter_database,
       mysql_create_database,
       mysql_create_table,
       mysql_drop,
       mysql_rename,
       mysql_view;

parse: statement?
       EOF;

statement:
    alter_table
  | alter_view
  | alter_database
  | create_database
  | create_table
  | create_view
  | drop_database
  | drop_table
  | rename_table
  | BEGIN
  ;
