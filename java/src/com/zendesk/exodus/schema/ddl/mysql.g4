grammar mysql;
import mysql_literal_tokens, mysql_idents;

parse: alter_tbl_statement EOF;
alter_tbl_statement: alter_tbl_preamble alter_specifications (engine_statement)?;

alter_tbl_preamble: ALTER alter_flags? TABLE table_name;
alter_flags: (ONLINE | OFFLINE | IGNORE);

alter_specifications: alter_specification (',' alter_specification)*;
alter_specification: add_column;
                //   | add_column_parens
                 //  | add_index
                  // | add_constraint;
                   
add_column: ADD COLUMN? col_name column_definition col_position?;
col_position: FIRST | (AFTER id);
col_name: id;

column_definition:
	data_type 
	(column_options)*
	;

data_type:
    generic_type
    | signed_type
    | string_type
//	| enum_type
//	| set_type
	;

generic_type: // types from which we're going to ignore any flags/length ars
	  col_type=(BIT | BINARY) LENGTH?
	| col_type=(DATE | TIME | TIMESTAMP | DATETIME | YEAR | TINYBLOB | MEDIUMBLOB | LONGBLOB | BLOB )
	| col_type=VARBINARY LENGTH
	;

signed_type: // we need the UNSIGNED flag here
      col_type=(TINYINT | SMALLINT | MEDIUMINT | INT | INTEGER | BIGINT )   
                LENGTH? 
                int_flags*
    | col_type=(REAL | DOUBLE | FLOAT | DECIMAL | NUMERIC)
    		    DECIMAL_LENGTH?
			    int_flags*
    ;

string_type: // getting the encoding here 
	  col_type=(CHAR | VARCHAR)
	           LENGTH?
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

LENGTH: '(' [0-9]+ ')';
int_flags: ( UNSIGNED | ZEROFILL );
DECIMAL_LENGTH: '(' [0-9]+ ',' [0-9]+ ')';
	 
engine_statement: ENGINE '=' IDENT;

table_name: id
		    | id '.' id;

id: ( IDENT | QUOTED_IDENT );
