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
col_position: FIRST | (AFTER ID);
col_name: ID;

column_definition:
	data_type 
	(nullability)?
	;

data_type:
	bit_type 
	| int_type
	| decimal_type
	| flat_type
	| binary_type
	| varbinary_type
	| string_type
//	| enum_type
//	| set_type
	;
		

bit_type:        col_type=BIT LENGTH?;
int_type:        col_type=(TINYINT | SMALLINT | MEDIUMINT | INT | INTEGER | BIGINT )   
			     LENGTH?
			     int_flags*
			     ;
decimal_type:    col_type=(REAL | DOUBLE | FLOAT | DECIMAL | NUMERIC)
			     DECIMAL_LENGTH?
			     int_flags*
			     ; 

flat_type:       col_type=(DATE | TIME | TIMESTAMP | DATETIME | YEAR | TINYBLOB | MEDIUMBLOB | LONGBLOB | BLOB );
string_type:     col_type=(CHAR | VARCHAR | TINYTEXT | TEXT | MEDIUMTEXT | LONGTEXT) 
			     charset_def?
			     ;
binary_type:     col_type=BINARY LENGTH?;
varbinary_type:  col_type=VARBINARY LENGTH;


charset_def: (character_set | collation)+;

character_set: CHARACTER SET IDENT;
collation: COLLATE IDENT;

nullability: (NOT NULL | NULL);

LENGTH: '(' [0-9]+ ')';
int_flags: ( UNSIGNED | ZEROFILL );
DECIMAL_LENGTH: '(' [0-9]+ ',' [0-9]+ ')';
	 
engine_statement: ENGINE '=' IDENT;

table_name: ID
		    | ID '.' ID;

// WS  :   [ \t\n\r]+ -> skip ;
