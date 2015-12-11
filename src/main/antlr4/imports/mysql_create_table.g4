grammar mysql_create_table;

import mysql_literal_tokens, mysql_idents, mysql_indices;

create_table:
  create_table_preamble
  (
    create_specifications ( table_creation_option ','? )*
    | create_like_tbl
  );

create_table_preamble: CREATE TEMPORARY? TABLE if_not_exists? table_name;
create_specifications: '(' create_specification (',' create_specification)* ')';

create_specification:
  column_definition
  | index_definition;

create_like_tbl: LIKE table_name;

table_creation_option:
	  creation_engine
	| creation_auto_increment
	| creation_avg_row_length
	| creation_character_set
	| creation_checksum
	| creation_collation
	| creation_comment
	| creation_connection
	| creation_data_directory
	| creation_delay_key_write
	| creation_index_directory
	| creation_insert_method
	| creation_key_block_size
	| creation_max_rows
	| creation_min_rows
	| creation_pack_keys
	| creation_password
	| creation_row_format
	| creation_tablespace
	| creation_union;


creation_engine: ENGINE '='? (string | MEMORY);
creation_auto_increment: AUTO_INCREMENT '='? integer;
creation_avg_row_length: AVG_ROW_LENGTH '='? integer;
creation_character_set: DEFAULT? ((CHARACTER SET) | CHARSET) '='? charset_name;
creation_checksum:  CHECKSUM '=' integer;
creation_collation: DEFAULT? COLLATE '='? string;
creation_comment: COMMENT '='? STRING_LITERAL;
creation_connection: CONNECTION '='? STRING_LITERAL;
creation_data_directory: DATA DIRECTORY '='? STRING_LITERAL;
creation_delay_key_write: DELAY_KEY_WRITE '='? integer;
creation_index_directory: INDEX DIRECTORY '='? STRING_LITERAL;
creation_insert_method: INSERT_METHOD '='? (NO | FIRST | LAST);
creation_key_block_size: KEY_BLOCK_SIZE '='? integer;
creation_max_rows: MAX_ROWS '='? integer;
creation_min_rows: MIN_ROWS '='? integer;
creation_pack_keys: PACK_KEYS '='? (integer | DEFAULT);
creation_password: PASSWORD '='? STRING_LITERAL;
creation_row_format: ROW_FORMAT '='? (DEFAULT | DEFAULT | DYNAMIC | FIXED | COMPRESSED | REDUNDANT | COMPACT);
creation_tablespace: TABLESPACE string (STORAGE (DISK | MEMORY | DEFAULT))?;
creation_union: UNION '=' name (',' name)*;
