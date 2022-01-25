grammar mysql_alter_table;

import mysql_literal_tokens, mysql_idents, column_definitions, mysql_partition;

alter_table: alter_table_preamble alter_specifications? alter_partition_specification?;

alter_table_preamble: ALTER alter_flags? TABLE table_name;
alter_flags: (ONLINE | OFFLINE | IGNORE);

alter_specifications: alter_specification (',' alter_specification)*;
alter_specification:
  add_column
  | add_column_parens
  | change_column
  | drop_column
  | modify_column
  | rename_column
  | drop_key
  | drop_primary_key
  | alter_rename_table
  | convert_to_character_set
  | default_character_set
  | table_creation_option+
  | ignored_alter_specifications
  ;

// the various alter_table commands available
add_column: ADD COLUMN? column_definition col_position?;
add_column_parens: ADD COLUMN? '(' (column_definition|index_definition) (',' (column_definition|index_definition))* ')';
change_column: CHANGE COLUMN? full_column_name column_definition col_position?;
drop_column: DROP COLUMN? full_column_name CASCADE?;
modify_column: MODIFY COLUMN? column_definition col_position?;
drop_key: DROP FOREIGN? (INDEX|KEY) name;
drop_primary_key: DROP PRIMARY KEY;
alter_rename_table: RENAME (TO | AS)? table_name;
convert_to_character_set: CONVERT TO charset_token charset_name collation?;
rename_column: RENAME COLUMN name TO name;

alter_partition_specification:
      ADD PARTITION if_not_exists? skip_parens
    | DROP PARTITION if_exists? partition_names
    | TRUNCATE PARTITION partition_names
    | DISCARD PARTITION partition_names TABLESPACE
    | IMPORT PARTITION partition_names TABLESPACE
    | COALESCE PARTITION INTEGER_LITERAL
    | REORGANIZE PARTITION (partition_names INTO skip_parens)?
    | EXCHANGE PARTITION IDENT WITH TABLE table_name ((WITH|WITHOUT) VALIDATION)?
    | ANALYZE PARTITION partition_names
    | CHECK PARTITION partition_names
    | OPTIMIZE PARTITION partition_names
    | REBUILD PARTITION partition_names
    | REPAIR PARTITION partition_names
    | REMOVE PARTITIONING
    | UPGRADE PARTITIONING
    | partition_by;

ignored_alter_specifications:
    ADD index_definition
    | ALTER INDEX name (VISIBLE | INVISIBLE)
    | ALTER COLUMN? name ((SET DEFAULT literal) | (DROP DEFAULT) | (SET (VISIBLE | INVISIBLE)))
    | DROP INDEX index_name
    | DISABLE KEYS
    | ENABLE KEYS
    | ORDER BY alter_ordering (',' alter_ordering)*
    | FORCE
    | DISCARD TABLESPACE
    | IMPORT TABLESPACE
    | ALGORITHM '='? algorithm_type
    | LOCK '='? lock_type
    | RENAME (INDEX|KEY) name TO name
    | DROP CHECK name
    | DROP CONSTRAINT name
    ;
  algorithm_type: DEFAULT | INPLACE | COPY | INSTANT;
  lock_type: DEFAULT | NONE | SHARED | EXCLUSIVE;

partition_names: id (',' id)*;
alter_ordering: alter_ordering_column (ASC|DESC)?;
alter_ordering_column:
    name '.' name '.' name
  | name '.' name
  | name;
