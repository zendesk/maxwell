grammar mysql_create_database;

import mysql_literal_tokens, mysql_idents;

create_database:
  CREATE (DATABASE | SCHEMA) if_not_exists? name (default_character_set | default_collation)*;

