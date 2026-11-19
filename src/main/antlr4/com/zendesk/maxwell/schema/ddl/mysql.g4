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

parse: (set_statement_for? statement)?
       EOF;

// MariaDB: SET STATEMENT var=value[, ...] FOR <statement>
// the variables only affect how the statement is executed, so we ignore them.
set_statement_for:
    SET STATEMENT set_statement_var (',' set_statement_var)* FOR;

set_statement_var: name '=' (literal | name_all_tokens);

statement:
    alter_table
  | alter_view
  | alter_database
  | create_database
  | create_table
  | create_view
  | drop_database
  | drop_table
  | drop_view
  | rename_table
  | BEGIN
  ;
