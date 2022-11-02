grammar mysql_set_statement_for;
import mysql_literal_tokens;

set_statement_var: IDENT '=' literal ','?;
set_statement_for:
  SET STATEMENT set_statement_var+ FOR;
