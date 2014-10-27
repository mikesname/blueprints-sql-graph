package com.tinkerpop.blueprints.impls.sql;

import java.sql.ResultSet;

/**
* @author Lukas Krejci
* @since 1.0
*/
interface ElementGenerator<T> {
    T generate(SqlGraph graph, ResultSet rs);
}
