package com.twitter.finagle.example.java.http;

import com.fasterxml.jackson.databind.ObjectMapper;

public final class JsonUtils {
    private static ObjectMapper mapper = new ObjectMapper();

    private JsonUtils() {

    }

    /**
     * Convert an Object to an array of bytes.
     */
    public static byte[] toBytes(Object value) {
        try {
            return mapper.writeValueAsBytes(value);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw new IllegalArgumentException(
              String.format("Could not transform to bytes: %s", e.getMessage()));
        }
    }
}
