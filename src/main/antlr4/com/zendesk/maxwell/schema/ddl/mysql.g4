grammar mysql;
import mysql_literal_tokens, mysql_idents, column_definitions, table_creation_options;

parse: statement
       EOF;

statement:
	alter_table 
	| create_database
    | create_table
    | drop_database
    | drop_table
    | rename_table
    | BEGIN
    ;



create_database:
	CREATE DATABASE if_not_exists? name (default_character_set | default_collate)*;
	
create_table: 
    create_table_preamble 
    (
      ( create_specifications table_creation_options* )
      | create_like_tbl
    );
    

create_table_preamble: CREATE TEMPORARY? TABLE (IF NOT EXISTS)? table_name;
create_specifications: '(' create_specification (',' create_specification)* ')'; 

create_specification: 
	column_definition 
	| index_definition;
		

create_like_tbl: LIKE table_name;

drop_database: DROP (DATABASE | SCHEMA) if_exists? name;

drop_table: DROP TEMPORARY? TABLE if_exists? table_name (',' table_name)* drop_table_options*;
drop_table_options: (RESTRICT | CASCADE);

rename_table: RENAME TABLE rename_table_spec (',' rename_table_spec)*;
rename_table_spec: table_name TO table_name;

alter_table: alter_table_preamble alter_specifications (engine_statement)?;

alter_table_preamble: ALTER alter_flags? TABLE table_name;
alter_flags: (ONLINE | OFFLINE | IGNORE);

alter_specifications: alter_specification (',' alter_specification)*;
alter_specification: add_column
                     | add_column_parens
                     | change_column
                     | modify_column
                     | drop_column
                     | ignored_alter_specifications
                     | alter_rename_table
                     | convert_to_character_set
                     | default_character_set
                     ; 
                   
add_column: ADD COLUMN? column_definition col_position?;
add_column_parens: ADD COLUMN? '(' column_definition (',' column_definition)* ')';
change_column: CHANGE COLUMN? old_col_name column_definition col_position?;
modify_column: MODIFY COLUMN? column_definition col_position?;
drop_column: DROP COLUMN? old_col_name;
alter_rename_table: RENAME (TO | AS) table_name;

convert_to_character_set: CONVERT TO charset_token charset_name collation?;
default_character_set: DEFAULT? charset_token '='? charset_name collation?;
default_collate: DEFAULT? collation;

/* it's not documented, but either "charset 'utf8'" or "character set 'utf8'" is valid. */
charset_token: (CHARSET | (CHARACTER SET));

old_col_name: name;

ignored_alter_specifications:
	  ADD index_definition
    | ALTER COLUMN? name ((SET DEFAULT literal) | (DROP DEFAULT))
    | DROP PRIMARY KEY
    | DROP INDEX index_name
    | DISABLE KEYS
    | ENABLE KEYS
    | ORDER BY name_list
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
    ; 

index_definition:
	(index_type_1 | index_type_2 | index_type_3 | index_type_4 | index_type_5 );
	
index_type_1:
	index_or_key index_name? index_type? index_column_list index_options*;

index_type_2:
	index_constraint? PRIMARY KEY index_type? index_column_list index_options*;

index_type_3:	
	index_constraint? UNIQUE index_or_key index_name? index_type? index_column_list index_options*;

index_type_4:
	(FULLTEXT | SPATIAL) index_or_key index_name? index_column_list index_options*;
	
index_type_5:
	index_constraint? FOREIGN KEY index_name? index_column_list;
	
// TODO: foreign key references.  goddamn.
	
index_or_key: (INDEX|KEY);
index_constraint: (CONSTRAINT name?);
index_name: name;
index_type: USING (BTREE | HASH);
index_options: 
	( KEY_BLOCK_SIZE '=' INTEGER_LITERAL )
	| index_type
	| WITH PARSER name // no idea if 'parser_name' is an id.  seems like a decent assumption.
	; 
index_column_list: '(' name_list ')';
name_list: name (',' name )*; 

engine_statement: ENGINE '=' IDENT;

if_exists: IF EXISTS;
if_not_exists: IF NOT EXISTS; 

db_name: name '.';
table_name: (db_name name)
			| name
			;

name: id | tokens_available_for_names;


