grammar mysql;
import mysql_literal_tokens, mysql_idents;

parse: alter_tbl_statement EOF;
alter_tbl_statement: alter_tbl_preamble alter_specifications (engine_statement)?;

alter_tbl_preamble: ALTER alter_flags? TABLE table_name;
alter_flags: (ONLINE | OFFLINE | IGNORE);

alter_specifications: alter_specification (',' alter_specification)*;
alter_specification: add_column
                     | add_column_parens
                     | change_column
                     | drop_column
                     | ignored_alter_specifications
                     ; 
                //   | add_column_parens
                 //  | add_index
                  // | add_constraint;
                   
add_column: ADD COLUMN? column_definition col_position?;
add_column_parens: ADD COLUMN? '(' column_definition (',' column_definition)* ')';
change_column: CHANGE COLUMN? old_col_name column_definition col_position?;
drop_column: DROP COLUMN? old_col_name;

col_position: FIRST | (AFTER id);
column_definition:
	col_name=id
	data_type 
	(column_options)*
	;

old_col_name: id;

data_type:
    generic_type
    | signed_type
    | string_type
//	| enum_type
//	| set_type
	;
	
// from http://dev.mysql.com/doc/refman/5.1/en/create-table.html
generic_type: // types from which we're going to ignore any flags/length ars
	  col_type=(BIT | BINARY) length?
	| col_type=(DATE | TIME | TIMESTAMP | DATETIME | YEAR | TINYBLOB | MEDIUMBLOB | LONGBLOB | BLOB )
	| col_type=VARBINARY length
	;

signed_type: // we need the UNSIGNED flag here
      col_type=(TINYINT | SMALLINT | MEDIUMINT | INT | INTEGER | BIGINT )   
                length? 
                int_flags*
    | col_type=(REAL | DOUBLE | FLOAT | DECIMAL | NUMERIC)
    		    decimal_length?
			    int_flags*
    ;

string_type: // getting the encoding here 
	  col_type=(CHAR | VARCHAR)
	           length?
	           charset_def?
    | col_type=(TINYTEXT | TEXT | MEDIUMTEXT | LONGTEXT) 
  		        charset_def?
	  ;

charset_def: (character_set | collation)+;

character_set: CHARACTER SET IDENT;
collation: COLLATE IDENT;

column_options:
	  nullability
	| default_value
	| (AUTO_INCREMENT)
	| (UNIQUE | PRIMARY)? KEY 
	| COMMENT STRING_LITERAL
	| COLUMN_FORMAT (FIXED|DYNAMIC|DEFAULT)
	| STORAGE (DISK|MEMORY|DEFAULT)
;



nullability: (NOT NULL | NULL);
default_value: DEFAULT (literal | NULL);


ignored_alter_specifications:
	  ADD index_or_key id_name? index_type? index_column_list index_options*
    | ADD index_constraint? PRIMARY KEY index_type? index_column_list index_options*
    | ADD index_constraint? UNIQUE index_or_key id_name? index_type? index_column_list index_options*
    | ADD (FULLTEXT | SPATIAL) index_or_key id_name? index_column_list index_options*
    | ADD index_constraint? FOREIGN KEY id_name? index_column_list
    | ALTER COLUMN? id ((SET DEFAULT literal) | (DROP DEFAULT))
    ; 
    
index_or_key: (INDEX|KEY);
index_constraint: (CONSTRAINT id?);
id_name: id;
index_type: USING (BTREE | HASH);
index_options: 
	( KEY_BLOCK_SIZE '=' INTEGER_LITERAL )
	| index_type
	| WITH PARSER id // no idea if 'parser_name' is an id.  seems like a decent assumption.
	; 
index_column_list: '(' id ( ',' id )* ')';

length: '(' INTEGER_LITERAL ')';
int_flags: ( UNSIGNED | ZEROFILL );
decimal_length: '(' INTEGER_LITERAL ',' INTEGER_LITERAL ')';
	 
engine_statement: ENGINE '=' IDENT;

table_name: id
		    | id '.' id;

id: ( IDENT | QUOTED_IDENT );


