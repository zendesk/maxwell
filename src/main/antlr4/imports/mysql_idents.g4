grammar mysql_idents;

import mysql_literal_tokens;

// this is a bizarre little construct we use to do paren matching on an
// unparsable (for us) token stream.
skip_parens: '(' | MAXWELL_ELIDED_PARSE_ISSUE;

db_name: name;
table_name: (db_name '.' name)
            | '.' name // *shakes head in sadness*
            | name
            ;

user: user_token ('@' user_token)?;
user_token: (IDENT | QUOTED_IDENT | string_literal);

name: ( id | tokens_available_for_names | INTEGER_LITERAL);
id: ( IDENT | QUOTED_IDENT );
literal: (float_literal | integer_literal | string_literal | NULL | TRUE | FALSE);

float_literal: INTEGER_LITERAL? '.' INTEGER_LITERAL;
integer_literal: INTEGER_LITERAL;
string_literal: (STRING_LITERAL | DBL_STRING_LITERAL);

string: (IDENT | STRING_LITERAL);
integer: INTEGER_LITERAL;
charset_name: (IDENT | string_literal | QUOTED_IDENT | BINARY);

default_character_set: DEFAULT? charset_token '='? charset_name collation?;
default_collation: DEFAULT? collation;

// it's not documented, but either "charset 'utf8'" or "character set 'utf8'" is valid.
charset_token: (CHARSET | (CHARACTER SET));
collation: COLLATE '='? (IDENT | string_literal | QUOTED_IDENT);

if_not_exists: IF NOT EXISTS;


SQL_UPGRADE_COMMENT: '/*!' [0-9]* -> skip;
SQL_UPGRADE_ENDCOMMENT: '*/' -> skip;

MAXWELL_ELIDED_PARSE_ISSUE: '/__MAXWELL__/';

SQL_COMMENT: '/*' ~'!' (.)*? '*/' -> skip;

SQL_LINE_COMMENT: ('#' | '--') (~'\n')* ('\n' | EOF) -> skip;

// all of these are greedy matches: we consume double-ticks (''), escaped ticks (\') and double-backslashes ('\\') as hard as
// we can, also matching anything that's not a single tick (') character until we hit the tick.  Same thing
// with DBL_STRING_LITERAL and QUOTED_IDENT.

STRING_LITERAL: [bnxBNX]? TICK (('\\' . ) | '\'\'' | ~('\\' | '\''))* TICK;
DBL_STRING_LITERAL: DBL ('""' | ~('"'))+ DBL;
INTEGER_LITERAL: DIGIT+;

fragment TICK: '\'';
fragment DBL: '"';

fragment UNQUOTED_CHAR: [0-9a-zA-Z\u0080-\uFFFF$_];
IDENT: (UNQUOTED_CHAR)+;

fragment BACKTICK: '`';
QUOTED_IDENT: BACKTICK (('``') | ~('`'))+ BACKTICK;

fragment DIGIT: [0-9];

// we don't use these tokens, but this feeds them into antlr's token engine so that it can not
// barf trying to lex complex mysql expressions.
UNUSED_TOKENS: [:\-+*&~|^/=><!];
WS  :   [ \t\n\r]+ -> channel(HIDDEN) ;
