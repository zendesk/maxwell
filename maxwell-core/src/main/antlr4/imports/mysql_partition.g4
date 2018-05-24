grammar mysql_partition;

import mysql_idents;

partition_by:
  PARTITION BY
  partition_by_what
  partition_count?
  subpartition_by?
  partition_definitions?
  ;

partition_by_what:
    LINEAR? HASH skip_parens
  | LINEAR? KEY (ALGORITHM '=' INTEGER_LITERAL)? skip_parens
  | RANGE COLUMNS? skip_parens
  | LIST COLUMNS? skip_parens;

subpartition_by:
  SUBPARTITION BY partition_by_what (SUBPARTITIONS INTEGER_LITERAL)?;

partition_count:
  PARTITIONS INTEGER_LITERAL;

partition_definitions: '(' skip_parens_inside_partition_definitions;
skip_parens_inside_partition_definitions: PARTITION | MAXWELL_ELIDED_PARSE_ISSUE;


/** partition_options:
    PARTITION BY
        { [LINEAR] HASH(expr)
        | [LINEAR] KEY [ALGORITHM={1|2}] (column_list)
        | RANGE{(expr) | COLUMNS(column_list)}
        | LIST{(expr) | COLUMNS(column_list)} }
    [PARTITIONS num]
    [SUBPARTITION BY
        { [LINEAR] HASH(expr)
        | [LINEAR] KEY [ALGORITHM={1|2}] (column_list) }
      [SUBPARTITIONS num]
    ]
    [(partition_definition [, partition_definition] ...)]

partition_definition:
    PARTITION partition_name
        [VALUES
            {LESS THAN {(expr | value_list) | MAXVALUE}
            |
            IN (value_list)}]
        [[STORAGE] ENGINE [=] engine_name]
        [COMMENT [=] 'comment_text' ]
        [DATA DIRECTORY [=] 'data_dir']'
        [INDEX DIRECTORY [=] 'index_dir']
        [MAX_ROWS [=] max_number_of_rows]
        [MIN_ROWS [=] min_number_of_rows]
        [TABLESPACE [=] tablespace_name]
        [(subpartition_definition [, subpartition_definition] ...)]

subpartition_definition:
    SUBPARTITION logical_name
        [[STORAGE] ENGINE [=] engine_name]
        [COMMENT [=] 'comment_text' ]
        [DATA DIRECTORY [=] 'data_dir']
        [INDEX DIRECTORY [=] 'index_dir']
        [MAX_ROWS [=] max_number_of_rows]
        [MIN_ROWS [=] min_number_of_rows]
        [TABLESPACE [=] tablespace_name]
	**/

