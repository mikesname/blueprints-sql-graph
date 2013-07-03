package com.tinkerpop.blueprints.impls.sql.utils;

import java.io.*;

/**
* @author Mike Bryant (http://github.com/mikesname)
*/
public class Encoder {

    public static byte[] encodeValue(Object value) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos);
            try {
                out.writeObject(value);
                return bos.toByteArray();
            } finally {
                out.close();
                bos.close();
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Object value '" + value + "' cannot be serialized");
        }
    }

    public static Object decodeValue(byte[] value) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(value);
            ObjectInput in = new ObjectInputStream(bis);
            try {
                return in.readObject();
            } finally {
                bis.close();
                in.close();
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new IllegalArgumentException("Object value '" + value + "' cannot be serialized");
        }
    }
}
