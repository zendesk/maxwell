grammar column_definitions;
import mysql_literal_tokens, mysql_idents;
import mysql_indices; // for REFERENCES

full_column_name:
  col_name=name
  | name '.' col_name=name
  | name '.' name '.' col_name=name
  ;

column_definition: (
    col_name=name |
    name '.' col_name=name |
    name '.' name '.' col_name=name
  )
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
	| col_type=(GEOMETRY | GEOMETRYCOLLECTION | LINESTRING | MULTILINESTRING
	             | MULTIPOINT | MULTIPOLYGON | POINT | POLYGON ) column_options*
	| col_type=JSON column_options*
	| col_type=VARBINARY length column_options*
	;


signed_type: // we need the UNSIGNED flag here
      col_type=(TINYINT | INT1 | SMALLINT | INT2 | MEDIUMINT | INT3 | INT | INTEGER | INT4 | BIGINT | INT8 | FIXED | FLOAT4 | FLOAT8 | MIDDLEINT )
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
    | col_type=SERIAL column_options*
    ;

string_type locals [Boolean utf8 = false]:
      (NATIONAL {$utf8=true;})?
      col_type=(CHAR | CHARACTER | VARCHAR) length?  (column_options | BYTE | UNICODE)*
    | (NATIONAL {$utf8=true;})?
      (CHARACTER|CHAR) col_type=VARYING length (column_options | BYTE | UNICODE)*
    | col_type=(NCHAR | NVARCHAR) length? column_options* {$utf8=true;}
    | NCHAR col_type=VARCHAR length? column_options* {$utf8=true;}
    | col_type=(TINYTEXT | MEDIUMTEXT | LONGTEXT) (column_options | BYTE | UNICODE)*
    | col_type=TEXT length? (column_options | BYTE | UNICODE)*
    | long_flag col_type=(VARCHAR | BINARY) (column_options | UNICODE)*
    | long_flag col_type=VARBINARY column_options*
    | col_type=LONG (column_options | BYTE | UNICODE)*
    ;

long_flag: LONG;

enumerated_type:
	  col_type=(ENUM | SET)
	  '(' enumerated_values ')'
	  column_options*
	  ;

column_options:
	  nullability
	| charset_def
	| collation
	| default_value
	| primary_key
	| visibility
	| ON UPDATE ( CURRENT_TIMESTAMP current_timestamp_length? | now_function )
	| UNIQUE KEY?
	| KEY
	| AUTO_INCREMENT
	| BINARY
	| COMMENT string_literal
	| COLUMN_FORMAT (FIXED|DYNAMIC|COMPRESSED|DEFAULT)
	| STORAGE (DISK|MEMORY|DEFAULT)
	| (VIRTUAL | PERSISTENT | STORED)
	| (GENERATED ALWAYS)? AS skip_parens
	| reference_definition
	| CHECK skip_parens
	| SRID INTEGER_LITERAL
	| NOT SECONDARY
;

primary_key: PRIMARY KEY;

enumerated_values: enum_value (',' enum_value)*;
enum_value: string_literal;

charset_def: character_set | ASCII;
character_set: ((CHARACTER SET) | CHARSET) charset_name;
visibility: VISIBLE | INVISIBLE;

nullability: (NOT NULL | NULL);
default_value: DEFAULT
  ( literal_with_weirdo_multistring
  | CURRENT_TIMESTAMP current_timestamp_length?
  | now_function
  | localtime_function
  | skip_parens );

length: '(' INTEGER_LITERAL ')';
int_flags: ( SIGNED | UNSIGNED | ZEROFILL );
decimal_length: '(' INTEGER_LITERAL ( ',' INTEGER_LITERAL )? ')';

now_function: NOW now_function_length;
now_function_length: length | '(' ')';
current_timestamp_length: length | '(' ')';
localtime_function: (LOCALTIME | LOCALTIMESTAMP) ('(' ')')?;
