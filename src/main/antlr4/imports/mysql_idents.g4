grammar mysql_idents;

import mysql_literal_tokens;

// this is a bizarre little construct we use to do paren matching on an
// unparsable (for us) token stream.
skip_parens: '(' | MAXWELL_ELIDED_PARSE_ISSUE;

db_name: name;
table_name: (db_name '.' name_all_tokens)
            | '.' name // *shakes head in sadness*
            | name
            ;

user: user_token ('@' user_token)?;
user_token: (IDENT | QUOTED_IDENT | string_literal);

name: ( id | tokens_available_for_names | INTEGER_LITERAL | DBL_STRING_LITERAL );
name_all_tokens: ( id | all_tokens | INTEGER_LITERAL | DBL_STRING_LITERAL );
id: ( IDENT | QUOTED_IDENT );
literal: (float_literal | broken_float_literal | integer_literal | string_literal | byte_literal | NULL | TRUE | FALSE);
literal_with_weirdo_multistring: (float_literal | broken_float_literal | integer_literal | string_literal+ | byte_literal | NULL | TRUE | FALSE);

float_literal: ('+'|'-')? INTEGER_LITERAL? '.' INTEGER_LITERAL;
broken_float_literal: ('+'|'-')? INTEGER_LITERAL '.';
integer_literal: ('+'|'-')? INTEGER_LITERAL;
string_literal: (STRING_LITERAL | DBL_STRING_LITERAL);
byte_literal:
            IDENT // a bit hacky, but IDENT matches byte literals (0b010101) and honestly we don't care.
            | STRING_LITERAL INTEGER_LITERAL; // matches 'b'01010, 'B'101010

string: (IDENT | STRING_LITERAL);
integer: INTEGER_LITERAL;
charset_name: (IDENT | string_literal | QUOTED_IDENT | BINARY | ASCII | DEFAULT);

default_character_set: DEFAULT? charset_token '='? charset_name collation?;
default_collation: DEFAULT? collation;

// it's not documented, but either "charset 'utf8'" or "character set 'utf8'" is valid.
charset_token: (CHARSET | (CHARACTER SET) | (CHAR SET));
collation: COLLATE '='? (IDENT | string_literal | QUOTED_IDENT | DEFAULT);

tablespace: TABLESPACE '='? (id | string_literal);

if_not_exists: IF NOT EXISTS;


SQL_UPGRADE_COMMENT: '/*!' [0-9]* -> skip;
SQL_UPGRADE_ENDCOMMENT: '*/' -> skip;

MAXWELL_ELIDED_PARSE_ISSUE: '/__MAXWELL__/';

SQL_COMMENT: '/*' ~'!' (.)*? '*/' -> skip;
SQL_EMPTY_COMMENT: '/**/' -> skip;

SQL_LINE_COMMENT: ('#' | '--') (~'\n')* ('\n' | EOF) -> skip;

// the logic here is that the character following a backslash may never
// end a string -- there will always be a character following it.
// So \' will be consumed, as will \\.  earlier we were trying to
// explicitly match \', but this had issues around the string '\\', as the
// tokenizer made the first backslash part of the string, and the second backslash
// got attached to the tick character.

STRING_LITERAL: [bnxBNX]? TICK (('\\' . ) | '\'\'' | ~('\\' | '\''))* TICK;
DBL_STRING_LITERAL: DBL (('\\' .) | '""' | ~('\\' | '"'))* DBL;
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
