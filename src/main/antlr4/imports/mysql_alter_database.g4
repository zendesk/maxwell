grammar mysql_alter_database;

import mysql_literal_tokens, mysql_idents;

alter_database: ALTER (DATABASE | SCHEMA) name? alter_database_definition;
alter_database_definition:
    (default_character_set | default_collation)+
  | UPGRADE DATA DIRECTORY NAME
  | alter_encryption;

alter_encryption: ENCRYPTION '='? string_literal;
