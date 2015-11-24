grammar mysql_idents;

import mysql_literal_tokens;

name: ( id | tokens_available_for_names );
id: ( IDENT | QUOTED_IDENT );
literal: (INTEGER_LITERAL | STRING_LITERAL | FLOAT_LITERAL);
string: (IDENT | STRING_LITERAL);
integer: INTEGER_LITERAL;
charset_name: (IDENT | STRING_LITERAL | QUOTED_IDENT);

SQL_UPGRADE_COMMENT: '/*!' [0-9]* -> skip;
SQL_UPGRADE_ENDCOMMENT: '*/' -> skip;

SQL_COMMENT: '/*' ~'!' (.)*? '*/' -> skip;

SQL_LINE_COMMENT: ('#' | '--') (~'\n')* ('\n' | EOF) -> skip;

STRING_LITERAL: TICK ('\\\'' | '\'\'' | ~('\''))* TICK;

FLOAT_LITERAL: DIGIT* '.' DIGIT+;

INTEGER_LITERAL: DIGIT+;

fragment TICK: '\'';

fragment UNQUOTED_CHAR: [0-9a-zA-Z\u0080-\u00FF$_];
IDENT: (UNQUOTED_CHAR)+;

fragment QUOTED_CHAR: ~('/' | '\\' | '.' | '`');
QUOTED_IDENT: '`' QUOTED_CHAR+? '`';

fragment DIGIT: [0-9];
WS  :   [ \t\n\r]+ -> skip ;