package com.happy.home.commons.lang.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.joda.JodaModule;

import java.io.IOException;

/**
 * @author: lijixiao
 * @date: 2020-08-06
 */
public class JsonUtil {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JodaModule())
            .disable(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

    public static String writeObjectAsString(Object obj) {
        try {
            return OBJECT_MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("write object as json string fault", e);
        }
    }

    public static byte[] writeObjectAsBytes(Object obj) {
        try {
            return OBJECT_MAPPER.writeValueAsString(obj).getBytes();
        } catch (JsonProcessingException e) {
            throw new RuntimeException("write object as json bytes fault", e);
        }
    }

    public static <T> T readBytesAsObject(byte[] jsonBytes, TypeReference<T> typeReference){
        try {
            return OBJECT_MAPPER.readValue(jsonBytes, typeReference);
        } catch (IOException e) {
            throw new RuntimeException("read json bytes as object fault", e);
        }
    }

    public static <T> T readStringAsObject(String jsonString, TypeReference<T> typeReference){
        try {
            return OBJECT_MAPPER.readValue(jsonString, typeReference);
        } catch (IOException e) {
            throw new RuntimeException("read json string as object fault", e);
        }
    }

    public static <T> T readBytesAsObject(byte[] jsonBytes, Class<T> clazz){
        try {
            return OBJECT_MAPPER.readValue(jsonBytes, clazz);
        } catch (IOException e) {
            throw new RuntimeException("read json bytes as object fault", e);
        }
    }

    public static <T> T readStringAsObject(String jsonString, Class<T> clazz){
        try {
            return OBJECT_MAPPER.readValue(jsonString, clazz);
        } catch (IOException e) {
            throw new RuntimeException("read json string as object fault", e);
        }
    }

    public static <T> T convertObject(Object obj, TypeReference<T> typeReference){
        return OBJECT_MAPPER.convertValue(obj, typeReference);
    }

    public static <T> T convertObject(Object obj, Class<T> clazz){
        return OBJECT_MAPPER.convertValue(obj, clazz);
    }
}
