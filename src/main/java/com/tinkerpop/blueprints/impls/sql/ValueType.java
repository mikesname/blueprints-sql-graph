package com.tinkerpop.blueprints.impls.sql;

import java.math.BigDecimal;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

/**
* @author Lukas Krejci
* @since 1.0
*/
enum ValueType {
    BOOLEAN(Boolean.class) {
        @Override
        public Object convertFromDBType(Object input) {
            return ((BigDecimal) input).byteValue() != 0;
        }
    }, CHARACTER(Character.class) {
        @Override
        public Object convertFromDBType(Object input) {
            return ((String) input).charAt(0);
        }
    }, BYTE(Byte.class) {
        @Override
        public Object convertFromDBType(Object input) {
            return ((BigDecimal) input).byteValue();
        }
    }, SHORTINT(Short.class) {
        @Override
        public Object convertFromDBType(Object input) {
            return ((BigDecimal) input).shortValue();
        }
    }, INT(Integer.class) {
        @Override
        public Object convertFromDBType(Object input) {
            return ((BigDecimal) input).intValue();
        }
    }, LONG(Long.class) {
        @Override
        public Object convertFromDBType(Object input) {
            return ((BigDecimal) input).longValue();
        }
    }, FLOAT(Float.class) {
        @Override
        public Object convertFromDBType(Object input) {
            return ((BigDecimal) input).floatValue();
        }
    }, DOUBLE(Double.class) {
        @Override
        public Object convertFromDBType(Object input) {
            return ((BigDecimal) input).doubleValue();
        }
    }, STRING(String.class) {
        @Override
        public Object convertFromDBType(Object input) {
            NClob lob = (NClob) input;
            try {
                return lob.getSubString(1, (int) lob.length());
            } catch (SQLException e) {
                throw new IllegalStateException("Could not get a string value from DB.", e);
            }
        }
    }, NULL(Void.class) {
        @Override
        public Object convertFromDBType(Object input) {
            return null;
        }
    };

    private final Class<?> valueType;

    ValueType(Class<?> valueType) {
        this.valueType = valueType;
    }

    public abstract Object convertFromDBType(Object input);

    public boolean isNumeric() {
        switch (this) {
        case CHARACTER:
        case STRING:
            return false;
        default:
            return true;
        }
    }

    public static ValueType of(Object object, boolean toBeStored) {
        if (object == null) {
            return NULL;
        }

        if (!toBeStored) {
            if (object instanceof Iterable) {
                Iterator<?> it = ((Iterable<?>) object).iterator();
                if (!it.hasNext()) {
                    return null;
                }

                object = it.next();
            }
        }

        for (ValueType v : ValueType.values()) {
            if (v.valueType.isAssignableFrom(object.getClass())) {
                return v;
            }
        }

        return null;
    }
}
