lexer grammar mysql_idents;

ID: IDENT | QUOTED_IDENT;

IDENT: (UNQUOTED_CHAR)+;
fragment UNQUOTED_CHAR: [0-9a-zA-Z\u0080-\u00FF$_];

QUOTED_IDENT: '`' QUOTED_CHAR+? '`';

fragment QUOTED_CHAR: ~('/' | '\\' | '.' | '`');


WS  :   [ \t\n\r]+ -> skip ;