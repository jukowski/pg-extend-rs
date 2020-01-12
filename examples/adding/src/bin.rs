extern crate pg_extend;

use pg_extend::pg_create_stmt_bin;

pg_create_stmt_bin!(
    add_one_pg_create_stmt,
    add_big_one_pg_create_stmt,
    add_small_one_pg_create_stmt,
    add_together_pg_create_stmt,
    sum_array_pg_create_stmt,
    sum_small_array_pg_create_stmt,
    sum_big_array_pg_create_stmt,
    sum_float_array_pg_create_stmt,
    sum_double_array_pg_create_stmt,
    clone_int4_pg_create_stmt,
    clone_string_pg_create_stmt
);
