grammar mysql_alter_table;

import mysql_literal_tokens, mysql_idents, column_definitions;

alter_table: alter_table_preamble alter_specifications;

alter_table_preamble: ALTER alter_flags? TABLE table_name;
alter_flags: (ONLINE | OFFLINE | IGNORE);

alter_specifications: alter_specification (',' alter_specification)*;
alter_specification:
  add_column
  | add_column_parens
  | change_column
  | drop_column
  | modify_column
  | drop_primary_key
  | alter_rename_table
  | convert_to_character_set
  | default_character_set
  | table_creation_option+
  | ignored_alter_specifications
  ;

// the various alter_table commands available
add_column: ADD COLUMN? column_definition col_position?;
add_column_parens: ADD COLUMN? '(' column_definition (',' column_definition)* ')';
change_column: CHANGE COLUMN? old_col_name column_definition col_position?;
drop_column: DROP COLUMN? old_col_name;
  old_col_name: name;
modify_column: MODIFY COLUMN? column_definition col_position?;
drop_primary_key: DROP PRIMARY KEY;
alter_rename_table: RENAME (TO | AS) table_name;
convert_to_character_set: CONVERT TO charset_token charset_name collation?;
ignored_alter_specifications:
    ADD index_definition
    | ALTER COLUMN? name ((SET DEFAULT literal) | (DROP DEFAULT))
    | DROP INDEX index_name
    | DISABLE KEYS
    | ENABLE KEYS
    | ORDER BY index_columns
    /*
     I'm also leaving out the following from the alter table definition because who cares:
     | DISCARD TABLESPACE
     | IMPORT TABLESPACE
     | ADD PARTITION (partition_definition)
     | DROP PARTITION partition_names
     | COALESCE PARTITION number
     | REORGANIZE PARTITION [partition_names INTO (partition_definitions)]
     | ANALYZE PARTITION {partition_names | ALL}
     | CHECK PARTITION {partition_names | ALL}
     | OPTIMIZE PARTITION {partition_names | ALL}
     | REBUILD PARTITION {partition_names | ALL}
     | REPAIR PARTITION {partition_names | ALL}
     | PARTITION BY partitioning_expression
     | REMOVE PARTITIONING

     because who cares.
     */
     | ALGORITHM '='? algorithm_type
     | LOCK '='? lock_type
    ;
  algorithm_type: DEFAULT | INPLACE | COPY;
  lock_type: DEFAULT | NONE | SHARED | EXCLUSIVE;
