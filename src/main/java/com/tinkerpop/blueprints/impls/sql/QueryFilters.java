package com.tinkerpop.blueprints.impls.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.tinkerpop.blueprints.Contains;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.blueprints.Query;

/**
 * @author Lukas Krejci
 * @since 1.0
 */
final class QueryFilters {

    private final Map<String, List<OperatorAndValue>> filters = new HashMap<>();

    public Map<String, List<OperatorAndValue>> getFilters() {
        return filters;
    }

    public void has(String key) {
        addFilter(filters, key, new OperatorAndValue(CustomPredicates.EXISTS, null));
    }

    public void hasNot(String key) {
        addFilter(filters, key, new OperatorAndValue(CustomPredicates.DOES_NOT_EXIST, null));
    }

    public void has(String key, Object value) {
        addFilter(filters, key, new OperatorAndValue(com.tinkerpop.blueprints.Compare.EQUAL, value));
    }

    public void hasNot(String key, Object value) {
        addFilter(filters, key, new OperatorAndValue(com.tinkerpop.blueprints.Compare.NOT_EQUAL, value));
    }

    public void has(String key, Predicate predicate, Object value) {
        if (predicate == null) {
            throw new NullPointerException("null predicate");
        }

        if (isPredicateSupported(predicate)) {
            addFilter(filters, key, new OperatorAndValue(predicate, value));
        } else {
            throw new IllegalArgumentException("predicate not supported: " + predicate);
        }
    }

    public <T extends Comparable<T>> void has(String key, T value, Query.Compare compare) {
        addFilter(filters, key, new OperatorAndValue(compare, value));
    }

    public <T extends Comparable<?>> void interval(String key, T startValue, T endValue) {
        if (startValue instanceof Number) {
            addFilter(filters, key,
                new OperatorAndValue(CustomPredicates.INTERVAL, new Interval(startValue, endValue)));
        } else {
            throw new IllegalArgumentException("intervals supported only on numeric values");
        }
    }

    SqlAndParams generateStatement(String select, String mainTable, String propsTable, String propsTableFK,
        List<String> specialProps, String mainTableWhereClause) throws SQLException {

        StringBuilder bld = new StringBuilder(select);

        bld.append(" FROM ").append(mainTable);

        List<Object> params = new ArrayList<>();

        boolean whereClausePresent = false;

        if (mainTableWhereClause != null) {
            bld.append(" WHERE ").append(mainTableWhereClause);
            if (!filters.isEmpty()) {
                bld.append(" AND ");
            }
            whereClausePresent = true;
        }

        if (!filters.isEmpty()) {
            if (!whereClausePresent) {
                bld.append(" WHERE ");
            }

            Iterator<Map.Entry<String, List<OperatorAndValue>>> it = filters.entrySet()
                .iterator();
            if (it.hasNext()) {
                Map.Entry<String, List<QueryFilters.OperatorAndValue>> e = it.next();
                appendFilters(mainTable, propsTable, propsTableFK, bld, params, e.getKey(), e.getValue(), specialProps);
            }

            while (it.hasNext()) {
                bld.append(" AND ");
                Map.Entry<String, List<QueryFilters.OperatorAndValue>> e = it.next();
                appendFilters(mainTable, propsTable, propsTableFK, bld, params, e.getKey(), e.getValue(), specialProps);
            }
        }

        return new SqlAndParams(bld, params);
    }

    private void addFilter(Map<String, List<OperatorAndValue>> filters, String property, OperatorAndValue opValue) {
        List<OperatorAndValue> propFilters = filters.get(property);
        if (propFilters == null) {
            propFilters = new ArrayList<>();
            filters.put(property, propFilters);
        }

        propFilters.add(opValue);
    }

    private boolean isPredicateSupported(Predicate p) {
        return p instanceof CustomPredicates || p instanceof Query.Compare || p instanceof com.tinkerpop.blueprints.Compare
            || p instanceof Contains;
    }

    private void appendFilters(String mainTable, String propsTable, String propsTableFK, StringBuilder bld,
        List<Object> params, String name, List<QueryFilters.OperatorAndValue> opValues, List<String> namesOnMainTable) {

        Iterator<QueryFilters.OperatorAndValue> it = opValues.iterator();

        boolean isOnMainTable = namesOnMainTable.contains(name);

        if (it.hasNext()) {
            appendFilter(mainTable, propsTable, propsTableFK, bld, params, name, it.next(), isOnMainTable);
        }

        while (it.hasNext()) {
            bld.append(" AND ");
            appendFilter(mainTable, propsTable, propsTableFK, bld, params, name, it.next(), isOnMainTable);
        }
    }

    private void appendFilter(String mainTable, String propsTable, String propsTableFK, StringBuilder bld,
        List<Object> params, String name, QueryFilters.OperatorAndValue opValue, boolean isOnMainTable) {
        Predicate operator = opValue.operator;
        Object value = opValue.object;
        ValueType valueType = ValueType.of(value, false);

        if (operator instanceof QueryFilters.CustomPredicates) {
            switch ((QueryFilters.CustomPredicates) operator) {
            case EXISTS:
                // if the param is on the main table, it always exists so we don't need to test for that
                if (isOnMainTable) {
                    bld.append("1 = 1"); //this is just to produce valid SQL
                } else {
                    propertyMatchPrologue(true, bld, mainTable, propsTable, propsTableFK)
                        .append(propsTable).append(".name = ?)");
                    params.add(name);
                }
                break;
            case DOES_NOT_EXIST:
                if (isOnMainTable) {
                    //the property is on the main table (i.e. it's id or label (in case of an edge) and is always present
                    bld.append("1 = 0");
                } else {
                    propertyMatchPrologue(false, bld, mainTable, propsTable, propsTableFK)
                        .append(propsTable).append(".name = ?)");
                    params.add(name);
                }
                break;
            case INTERVAL:
                if (isOnMainTable) {
                    mainPropertyComparison(">=", bld, mainTable, name);
                    bld.append(" AND ");
                    mainPropertyComparison("<", bld, mainTable, name);
                } else {
                    propertyMatchPrologue(true, bld, mainTable, propsTable, propsTableFK)
                        .append(propsTable).append(".name = ? AND ")
                        .append(propsTable).append(".numeric_value >= ? AND ")
                        .append(propsTable).append(".numeric_value < ?)");
                    params.add(name);
                }
                params.add(((QueryFilters.Interval) value).from);
                params.add(((QueryFilters.Interval) value).to);
                break;
            }
        } else if (operator instanceof Query.Compare) {
            if (!isOnMainTable) {
                params.add(name);
            }
            params.add(value);
            String op = null;
            switch ((Query.Compare) operator) {
            case EQUAL:
                op = "=";
                break;
            case GREATER_THAN:
                op = ">";
                break;
            case GREATER_THAN_EQUAL:
                op = ">=";
                break;
            case LESS_THAN:
                op = "<";
                break;
            case LESS_THAN_EQUAL:
                op = "<=";
                break;
            case NOT_EQUAL:
                op = "<>";
                break;
            }

            if (isOnMainTable) {
                mainPropertyComparison(op, bld, mainTable, name);
            } else {
                propertyComparison(op, valueType, bld, mainTable, propsTable, propsTableFK);
            }
        } else if (operator instanceof com.tinkerpop.blueprints.Compare) {
            if (!isOnMainTable) {
                params.add(name);
            }
            params.add(value);
            String op = null;
            switch ((com.tinkerpop.blueprints.Compare) operator) {
            case EQUAL:
                op = "=";
                break;
            case GREATER_THAN:
                op = ">";
                break;
            case GREATER_THAN_EQUAL:
                op = ">=";
                break;
            case LESS_THAN:
                op = "<";
                break;
            case LESS_THAN_EQUAL:
                op = "<=";
                break;
            case NOT_EQUAL:
                op = "<>";
                break;
            }
            if (isOnMainTable) {
                mainPropertyComparison(op, bld, mainTable, name);
            } else {
                propertyComparison(op, valueType, bld, mainTable, propsTable, propsTableFK);
            }
        } else if (operator instanceof Contains) {
            Iterable<?> col = (Iterable<?>) value;

            StringBuilder collection = null;

            Iterator<?> it = col.iterator();
            if (it.hasNext()) {
                collection = new StringBuilder("(");
                collection.append("?");
                if (!isOnMainTable) {
                    params.add(name);
                }
                params.add(it.next());
            }

            if (collection != null) {
                while (it.hasNext()) {
                    collection.append(", ?");
                    params.add(it.next());
                }

                collection.append(")");

                if (!isOnMainTable) {
                    collection.append(")");
                }

                //trailing space important so that we don't have to special-case the replace below
                String op = operator == Contains.IN ? "IN " : "NOT IN ";
                if (isOnMainTable) {
                    mainPropertyComparison(op, bld, mainTable, name);
                } else {
                    propertyComparison(op, valueType, bld, mainTable, propsTable, propsTableFK);
                }
                bld.replace(bld.length() - 2, bld.length(), collection.toString());
            }
        }
    }

    private StringBuilder propertyMatchPrologue(boolean match, StringBuilder bld, String mainTable, String propsTable,
        String propsTableFK) {
        bld.append("1 ").append(match ? "IN" : "NOT IN").append(" (SELECT 1 FROM ").append(propsTable).append(" WHERE ")
            .append(propsTable).append(".").append(propsTableFK).append(" = ")
            .append(mainTable).append(".id AND ");
        return bld;
    }

    private void propertyComparison(String operator, ValueType valueType, StringBuilder bld, String mainTable,
        String propsTable, String propsTableFK) {
        propertyMatchPrologue(true, bld, mainTable, propsTable, propsTableFK)
            .append(propsTable).append(".name = ? AND ")
            .append(propsTable).append(valueType.isNumeric() ? ".numeric_value" : ".string_value")
            .append(" ").append(operator).append(" ?)");
    }

    private void mainPropertyComparison(String operator, StringBuilder bld, String mainTable, String name) {
        bld.append(mainTable).append(".").append(name).append(" ").append(operator).append(" ?");
    }

    enum CustomPredicates implements Predicate {
        EXISTS,
        DOES_NOT_EXIST,
        INTERVAL {
            @Override
            @SuppressWarnings("unchecked")
            public boolean evaluate(Object first, Object second) {
                Interval interval = (Interval) second;
                return interval.from.compareTo(first) <= 0 && interval.to.compareTo(first) > 0;
            }
        };

        @Override
        public boolean evaluate(Object first, Object second) {
            return false;
        }
    }

    static class Interval {
        final Comparable from;
        final Comparable to;

        private Interval(Comparable from, Comparable to) {
            this.from = from;
            this.to = to;
        }
    }

    static class OperatorAndValue {
        final Predicate operator;
        final Object object;

        OperatorAndValue(Predicate operator, Object object) {
            this.operator = operator;
            this.object = object;
        }
    }

    static class SqlAndParams {
        final StringBuilder sql;
        final List<Object> params;

        SqlAndParams(StringBuilder sql, List<Object> params) {
            this.sql = sql;
            this.params = params;
        }
    }
}
