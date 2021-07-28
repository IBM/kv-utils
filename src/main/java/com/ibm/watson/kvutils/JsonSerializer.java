/*
 * Copyright 2021 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.ibm.watson.kvutils;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class JsonSerializer<T> implements KVTable.Serializer<T> {

    private static final ObjectMapper mapper = new ObjectMapper()
            .setSerializationInclusion(Include.NON_NULL)
            .setSerializationInclusion(Include.NON_DEFAULT);

    private final JavaType type;

    public JsonSerializer(Class<T> type) {
        this.type = mapper.getTypeFactory().constructType(type);
    }

    @Override
    public byte[] serialize(T obj) {
        try {
            return mapper.writeValueAsBytes(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e); //TODO EXCEPTION HANDLING
        }
    }

    @Override
    public T deserialize(byte[] data) {
        try {
            return mapper.readValue(data, type);
        } catch (IOException e) {
            throw new RuntimeException(e); //TODO EXCEPTION HANDLING
        }
    }

    @Override
    public T deserialize(InputStream in) {
        try {
            return mapper.readValue(in, type);
        } catch (IOException e) {
            throw new RuntimeException(e); //TODO EXCEPTION HANDLING
        }
    }

    @Override
    public void serialize(T obj, OutputStream out) {
        try {
            mapper.writeValue(out, obj);
        } catch (IOException e) {
            throw new RuntimeException(e); //TODO EXCEPTION HANDLING
        }
    }

}
