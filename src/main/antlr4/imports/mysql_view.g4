grammar mysql_view;

import mysql_literal_tokens, mysql_idents;

/*
  This in an intentionally incomplete grammar for parsing VIEW statements.
  It's designed to parse up to the (SELECT *), as that cruft is too tricky to
  capture with a regular-expression based blacklist.
*/

alter_view:
  ALTER view_options* VIEW name;

create_view:
  CREATE (OR REPLACE)? view_options* VIEW name;

view_options:
    ALGORITHM '=' (UNDEFINED | MERGE | TEMPTABLE)
  | DEFINER '=' (user | CURRENT_USER)
  | SQL SECURITY ( DEFINER | INVOKER );






