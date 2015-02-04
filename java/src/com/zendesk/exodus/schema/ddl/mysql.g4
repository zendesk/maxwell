grammar mysql;
import mysql_literal_tokens, mysql_idents, column_definitions;

parse: alter_tbl_statement EOF;
alter_tbl_statement: alter_tbl_preamble alter_specifications (engine_statement)?;

alter_tbl_preamble: ALTER alter_flags? TABLE table_name;
alter_flags: (ONLINE | OFFLINE | IGNORE);


alter_specifications: alter_specification (',' alter_specification)*;
alter_specification: add_column
                     | add_column_parens
                     | change_column
                     | modify_column
                     | drop_column
                     | ignored_alter_specifications
                     | rename_table
                     | convert_to_character_set
                     | default_character_set
                     ; 
                   
add_column: ADD COLUMN? column_definition col_position?;
add_column_parens: ADD COLUMN? '(' column_definition (',' column_definition)* ')';
change_column: CHANGE COLUMN? old_col_name column_definition col_position?;
modify_column: MODIFY COLUMN? column_definition col_position?;
drop_column: DROP COLUMN? old_col_name;
rename_table: RENAME (TO | AS) table_name;

convert_to_character_set: CONVERT TO charset_token charset_name collation?;
default_character_set: DEFAULT? charset_token '=' charset_name ( COLLATE '=' (IDENT | STRING_LITERAL) )?;

/* it's not documented, but either "charset 'utf8'" or "character set 'utf8'" is valid. */
charset_token: (CHARSET | (CHARACTER SET));

old_col_name: id;


ignored_alter_specifications:
	  ADD index_or_key index_name? index_type? index_column_list index_options*
    | ADD index_constraint? PRIMARY KEY index_type? index_column_list index_options*
    | ADD index_constraint? UNIQUE index_or_key index_name? index_type? index_column_list index_options*
    | ADD (FULLTEXT | SPATIAL) index_or_key index_name? index_column_list index_options*
    | ADD index_constraint? FOREIGN KEY index_name? index_column_list
    | ALTER COLUMN? id ((SET DEFAULT literal) | (DROP DEFAULT))
    | DROP PRIMARY KEY
    | DROP INDEX index_name
    | DISABLE KEYS
    | ENABLE KEYS
    | ORDER BY id_list
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
    
index_or_key: (INDEX|KEY);
index_constraint: (CONSTRAINT id?);
index_name: id;
index_type: USING (BTREE | HASH);
index_options: 
	( KEY_BLOCK_SIZE '=' INTEGER_LITERAL )
	| index_type
	| WITH PARSER id // no idea if 'parser_name' is an id.  seems like a decent assumption.
	; 
index_column_list: '(' id_list ')';
id_list: id (',' id )*; 

engine_statement: ENGINE '=' IDENT;

db_name: id '.';
table_name: (db_name id)
			| id
			;



