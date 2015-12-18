grammar column_definitions;
import mysql_literal_tokens, mysql_idents;


column_definition:
	col_name=name
	data_type
	;

col_position: FIRST | (AFTER id);

data_type:
    generic_type
  | signed_type
  | string_type
	| enumerated_type
	;


// all from http://dev.mysql.com/doc/refman/5.1/en/create-table.html
generic_type:
    col_type=(BIT | BINARY | YEAR | TIME | TIMESTAMP | DATETIME) length? column_options*
	| col_type=(DATE | TINYBLOB | MEDIUMBLOB | LONGBLOB | BLOB |  BOOLEAN | BOOL ) column_options*
	| col_type=VARBINARY length column_options*
	;


signed_type: // we need the UNSIGNED flag here
      col_type=(TINYINT | INT1 | SMALLINT | INT2 | MEDIUMINT | INT3 | INT | INTEGER | INT4 | BIGINT | INT8 )
                length?
                int_flags*
                column_options*
    | col_type=(REAL | DOUBLE | FLOAT | DECIMAL | NUMERIC)
    		    decimal_length?
    		    int_flags*
    		    column_options*
    ;

string_type: // getting the encoding here
	  col_type=(CHAR | VARCHAR)
	           length?
	           BINARY?
	           (charset_def | column_options)*
    | col_type=(TINYTEXT | TEXT | MEDIUMTEXT | LONGTEXT)
               BINARY?
               (charset_def | column_options)*
    | long_flag col_type=VARCHAR BINARY? (charset_def | column_options)*
    | long_flag col_type=(BINARY|VARBINARY) (charset_def | column_options)*
	  ;

long_flag: LONG;

enumerated_type:
	  col_type=(ENUM | SET)
	  '(' enumerated_values ')'
	   (charset_def | column_options)*
	  ;


column_options:
	  nullability
	| default_value
	| primary_key
	| ON UPDATE ( CURRENT_TIMESTAMP | now_function )
	| UNIQUE KEY?
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

nullability: (NOT NULL | NULL);
default_value: DEFAULT (literal | NULL | CURRENT_TIMESTAMP | now_function | TRUE | FALSE );
length: '(' INTEGER_LITERAL ')';
int_flags: ( UNSIGNED | ZEROFILL );
decimal_length: '(' INTEGER_LITERAL ( ',' INTEGER_LITERAL )? ')';

now_function: NOW '(' ')';
