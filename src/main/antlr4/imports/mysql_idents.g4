grammar mysql_idents;

import mysql_literal_tokens;

db_name: name;
table_name: (db_name '.' name)
            | name
            ;

user: user_token ('@' user_token)?;
user_token: (IDENT | QUOTED_IDENT | STRING_LITERAL);

name: ( id | tokens_available_for_names | INTEGER_LITERAL);
id: ( IDENT | QUOTED_IDENT );
literal: (float_literal | integer_literal | string_literal);

float_literal: INTEGER_LITERAL? '.' INTEGER_LITERAL;
integer_literal: INTEGER_LITERAL;
string_literal: STRING_LITERAL;

string: (IDENT | STRING_LITERAL);
integer: INTEGER_LITERAL;
charset_name: (IDENT | STRING_LITERAL | QUOTED_IDENT);

default_character_set: DEFAULT? charset_token '='? charset_name collation?;
default_collation: DEFAULT? collation;

// it's not documented, but either "charset 'utf8'" or "character set 'utf8'" is valid.
charset_token: (CHARSET | (CHARACTER SET));
collation: COLLATE '='? (IDENT | STRING_LITERAL | QUOTED_IDENT);

if_not_exists: IF NOT EXISTS;

SQL_UPGRADE_COMMENT: '/*!' [0-9]* -> skip;
SQL_UPGRADE_ENDCOMMENT: '*/' -> skip;

SQL_COMMENT: '/*' ~'!' (.)*? '*/' -> skip;

SQL_LINE_COMMENT: ('#' | '--') (~'\n')* ('\n' | EOF) -> skip;

STRING_LITERAL: TICK ('\\\'' | '\'\'' | ~('\''))* TICK;

INTEGER_LITERAL: DIGIT+;

fragment TICK: '\'';

fragment UNQUOTED_CHAR: [0-9a-zA-Z\u0080-\u00FF$_];
IDENT: (UNQUOTED_CHAR)+;

QUOTED_IDENT: '`' (~'`')+? '`';

fragment DIGIT: [0-9];
WS  :   [ \t\n\r]+ -> skip ;
