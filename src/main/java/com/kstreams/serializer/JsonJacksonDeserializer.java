/*
 * Copyright 2016 Bill Bejeck
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kstreams.serializer;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonJacksonDeserializer<T> implements Deserializer<T> {

    private ObjectMapper mapper = new ObjectMapper();
    private Class<T> deserializedClass;

    public JsonJacksonDeserializer(Class<T> deserializedClass) {
        this.deserializedClass = deserializedClass;
    }

    public JsonJacksonDeserializer() {
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> map, boolean b) {
        if(deserializedClass == null) {
            deserializedClass = (Class<T>) map.get("serializedClass");
        }
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
         if(bytes == null){
             System.out.println("Null bytes........");
             return null;
         }

         try {
            T instance = mapper.readValue(bytes, deserializedClass);
            System.out.println("DESERIALIZE: " + instance.toString());
            return instance;
         } catch (IOException e) {
            e.printStackTrace();
         }
         return null;
    }

    @Override
    public void close() {

    }
}
