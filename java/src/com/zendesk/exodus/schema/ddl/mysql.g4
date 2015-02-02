grammar mysql;

parse: alter_tbl_statement EOF;
alter_tbl_statement: alter_tbl_preamble alter_specifications (engine_statement)?;
alter_specifications: alter_specification (',' alter_specification)*;
alter_specification: add_column;
                //   | add_column_parens
                 //  | add_index
                  // | add_constraint;
                   
add_column: ADD COLUMN? col_name col_definition col_position?;
col_definition: 'text';
col_position: FIRST | (AFTER ID);
col_name: ID;
                   
                   
alter_tbl_preamble: ALTER alter_flags? TABLE table_name;
alter_flags: (ONLINE | OFFLINE | IGNORE);
engine_statement: ENGINE '=' IDENT;

ADD: A D D;
AFTER: A F T E R;
ALTER: A L T E R;
COLUMN: C O L U M N;
FIRST: F I R S T;
TABLE: T A B L E;
ONLINE: O N L I N E;
OFFLINE: O F F L I N E;
IGNORE: I G N O R E;
ENGINE: E N G I N E;

table_name: ID
		    | ID '.' ID;
	
ID: IDENT | QUOTED_IDENT;

IDENT: (UNQUOTED_CHAR)+;
UNQUOTED_CHAR: [0-9a-zA-Z\u0080-\u00FF$_];

QUOTED_IDENT: '`' QUOTED_CHAR+? '`';

fragment QUOTED_CHAR: ~('/' | '\\' | '.' | '`');
fragment A:('a'|'A');
fragment B:('b'|'B');
fragment C:('c'|'C');
fragment D:('d'|'D');
fragment E:('e'|'E');
fragment F:('f'|'F');
fragment G:('g'|'G');
fragment H:('h'|'H');
fragment I:('i'|'I');
fragment J:('j'|'J');
fragment K:('k'|'K');
fragment L:('l'|'L');
fragment M:('m'|'M');
fragment N:('n'|'N');
fragment O:('o'|'O');
fragment P:('p'|'P');
fragment Q:('q'|'Q');
fragment R:('r'|'R');
fragment S:('s'|'S');
fragment T:('t'|'T');
fragment U:('u'|'U');
fragment V:('v'|'V');
fragment W:('w'|'W');
fragment X:('x'|'X');
fragment Y:('y'|'Y');
fragment Z:('z'|'Z');


WS  :   [ \t\n\r]+ -> skip ;