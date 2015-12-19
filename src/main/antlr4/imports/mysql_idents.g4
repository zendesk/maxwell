grammar mysql_idents;

import mysql_literal_tokens;

db_name: name;
table_name: (db_name '.' name)
            | name
            ;

user: user_token ('@' user_token)?;
user_token: (IDENT | QUOTED_IDENT | string_literal);

name: ( id | tokens_available_for_names | INTEGER_LITERAL);
id: ( IDENT | QUOTED_IDENT );
literal: (float_literal | integer_literal | string_literal);

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

SQL_COMMENT: '/*' ~'!' (.)*? '*/' -> skip;

SQL_LINE_COMMENT: ('#' | '--') (~'\n')* ('\n' | EOF) -> skip;

STRING_LITERAL: [a-zA-Z]? TICK ('\\\'' | '\'\'' | ~('\''))* TICK;
DBL_STRING_LITERAL: DBL ('""' | ~('"'))+ DBL;
INTEGER_LITERAL: DIGIT+;

fragment TICK: '\'';
fragment DBL: '"';

fragment UNQUOTED_CHAR: [0-9a-zA-Z\u0080-\uFFFF$_];
IDENT: (UNQUOTED_CHAR)+;

fragment BACKTICK: '`';
QUOTED_IDENT: BACKTICK (('``') | ~('`'))+ BACKTICK;

fragment DIGIT: [0-9];
WS  :   [ \t\n\r]+ -> skip ;
