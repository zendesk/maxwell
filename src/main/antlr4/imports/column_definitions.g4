grammar column_definitions;
import mysql_literal_tokens, mysql_idents;


column_definition:
	col_name=name
	data_type
	;

col_position: FIRST | (AFTER name);

data_type:
    generic_type
  | signed_type
  | string_type
	| enumerated_type
	;


// all from http://dev.mysql.com/doc/refman/5.1/en/create-table.html
generic_type:
    col_type=(BIT | BINARY | BLOB | YEAR | TIME | TIMESTAMP | DATETIME) length? column_options*
	| col_type=(DATE | TINYBLOB | MEDIUMBLOB | LONGBLOB | BOOLEAN | BOOL ) column_options*
	| col_type=VARBINARY length column_options*

	;


signed_type: // we need the UNSIGNED flag here
      col_type=(TINYINT | INT1 | SMALLINT | INT2 | MEDIUMINT | INT3 | INT | INTEGER | INT4 | BIGINT | INT8 )
                length?
                int_flags*
                column_options*
    | col_type=(REAL | FLOAT | DECIMAL | NUMERIC)
    		    decimal_length?
    		    int_flags*
    		    column_options*
    | col_type=DOUBLE PRECISION?
		decimal_length?
		int_flags*
		column_options*
    ;

string_type locals [Boolean utf8 = false]:
      (NATIONAL {$utf8=true;})?
      col_type=(CHAR | CHARACTER | VARCHAR) length?  (column_options | string_column_options)*
    | (NATIONAL {$utf8=true;})?
      (CHARACTER|CHAR) col_type=VARYING length (string_column_options | column_options)*
    | col_type=(NCHAR | NVARCHAR) length? (string_column_options | column_options)* {$utf8=true;}
    | NCHAR col_type=VARCHAR length? (column_options | string_column_options)* {$utf8=true;}
    | col_type=(TINYTEXT | MEDIUMTEXT | LONGTEXT) (column_options | string_column_options)*
    | col_type=TEXT length? (column_options | string_column_options)*
    | long_flag col_type=(VARCHAR | BINARY) (column_options | string_column_options)*
    | long_flag col_type=VARBINARY column_options*
    | col_type=LONG (column_options | string_column_options)*
    ;

long_flag: LONG;

enumerated_type:
	  col_type=(ENUM | SET)
	  '(' enumerated_values ')'
	  (column_options | string_column_options)*
	  ;

string_column_options: charset_def | collation | BINARY;

column_options:
	  nullability
	| default_value
	| primary_key
	| ON UPDATE ( CURRENT_TIMESTAMP length? | now_function )
	| UNIQUE KEY?
	| KEY
	| AUTO_INCREMENT
	| COMMENT string_literal
	| COLUMN_FORMAT (FIXED|DYNAMIC|DEFAULT)
	| STORAGE (DISK|MEMORY|DEFAULT)
;

primary_key: PRIMARY KEY;

enumerated_values: enum_value (',' enum_value)*;
enum_value: string_literal;

charset_def: character_set | ASCII;
character_set: ((CHARACTER SET) | CHARSET) charset_name;

nullability: (NOT NULL | NULL);
default_value: DEFAULT (literal | NULL | CURRENT_TIMESTAMP length? | now_function | TRUE | FALSE );
length: '(' INTEGER_LITERAL ')';
int_flags: ( SIGNED | UNSIGNED | ZEROFILL );
decimal_length: '(' INTEGER_LITERAL ( ',' INTEGER_LITERAL )? ')';

now_function: NOW '(' ')';
