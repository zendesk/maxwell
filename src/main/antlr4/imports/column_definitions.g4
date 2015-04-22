grammar column_definitions;
import mysql_literal_tokens, mysql_idents;


column_definition:
	col_name=name
	data_type 
	column_options*
	;

col_position: FIRST | (AFTER id);

data_type:
    generic_type
    | signed_type
    | string_type
	| enumerated_type
	;

	
// from http://dev.mysql.com/doc/refman/5.1/en/create-table.html
generic_type: // types from which we're going to ignore any flags/length 
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

enumerated_type:
	  col_type=(ENUM | SET)
	  '(' enumerated_values ')'
	  charset_def? 
	  ;


column_options:
	  nullability
	| default_value
	| primary_key
	| ON UPDATE CURRENT_TIMESTAMP
	| UNIQUE KEY 
	| AUTO_INCREMENT
	| COMMENT STRING_LITERAL
	| COLUMN_FORMAT (FIXED|DYNAMIC|DEFAULT)
	| STORAGE (DISK|MEMORY|DEFAULT)
;

primary_key: PRIMARY KEY;

enumerated_values: enum_value (',' enum_value)*;
enum_value: STRING_LITERAL;

charset_def: (character_set | collation)+;
character_set: ((CHARACTER SET) | CHARSET) charset_name;
collation: COLLATE '='? (IDENT | STRING_LITERAL | QUOTED_IDENT);

nullability: (NOT NULL | NULL);
default_value: DEFAULT (literal | NULL | CURRENT_TIMESTAMP);
length: '(' INTEGER_LITERAL ')';
int_flags: ( UNSIGNED | ZEROFILL );
decimal_length: '(' INTEGER_LITERAL ',' INTEGER_LITERAL ')';