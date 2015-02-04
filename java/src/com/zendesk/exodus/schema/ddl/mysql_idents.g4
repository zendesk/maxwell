grammar mysql_idents;

literal: (INTEGER_LITERAL | STRING_LITERAL | FLOAT_LITERAL);
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