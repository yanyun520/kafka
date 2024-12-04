/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.data;

import org.apache.kafka.connect.errors.DataException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ConnectSchema implements Schema {
    /**
     * Maps Schema.Types to a list of Java classes that can be used to represent them.
     */
    private static final Map<Type, List<Class>> SCHEMA_TYPE_CLASSES = new EnumMap<>(Type.class);
    /**
     * Maps known logical types to a list of Java classes that can be used to represent them.
     */
    private static final Map<String, List<Class>> LOGICAL_TYPE_CLASSES = new HashMap<>();

    /**
     * Maps the Java classes to the corresponding Schema.Type.
     */
    private static final Map<Class<?>, Type> JAVA_CLASS_SCHEMA_TYPES = new HashMap<>();

    static {
        // 映射基本数据类型到Java类
        SCHEMA_TYPE_CLASSES.put(Type.INT8, Collections.singletonList((Class) Byte.class)); // 将Type.INT8映射到Byte类
        SCHEMA_TYPE_CLASSES.put(Type.INT16, Collections.singletonList((Class) Short.class)); // 将Type.INT16映射到Short类
        SCHEMA_TYPE_CLASSES.put(Type.INT32, Collections.singletonList((Class) Integer.class)); // 将Type.INT32映射到Integer类
        SCHEMA_TYPE_CLASSES.put(Type.INT64, Collections.singletonList((Class) Long.class)); // 将Type.INT64映射到Long类
        SCHEMA_TYPE_CLASSES.put(Type.FLOAT32, Collections.singletonList((Class) Float.class)); // 将Type.FLOAT32映射到Float类
        SCHEMA_TYPE_CLASSES.put(Type.FLOAT64, Collections.singletonList((Class) Double.class)); // 将Type.FLOAT64映射到Double类
        SCHEMA_TYPE_CLASSES.put(Type.BOOLEAN, Collections.singletonList((Class) Boolean.class)); // 将Type.BOOLEAN映射到Boolean类
        SCHEMA_TYPE_CLASSES.put(Type.STRING, Collections.singletonList((Class) String.class)); // 将Type.STRING映射到String类

        // Bytes类型的特殊处理
        // Bytes are special and have 2 representations. byte[] causes problems because it doesn't handle equals() and
        // hashCode() like we want objects to, so we support both byte[] and ByteBuffer. Using plain byte[] can cause
        // those methods to fail, so ByteBuffers are recommended
        SCHEMA_TYPE_CLASSES.put(Type.BYTES, Arrays.asList((Class) byte[].class, (Class) ByteBuffer.class)); // 将Type.BYTES映射到byte[]和ByteBuffer类

        SCHEMA_TYPE_CLASSES.put(Type.ARRAY, Collections.singletonList((Class) List.class)); // 将Type.ARRAY映射到List类
        SCHEMA_TYPE_CLASSES.put(Type.MAP, Collections.singletonList((Class) Map.class)); // 将Type.MAP映射到Map类
        SCHEMA_TYPE_CLASSES.put(Type.STRUCT, Collections.singletonList((Class) Struct.class)); // 将Type.STRUCT映射到Struct类

        // 将映射关系反向存储
        for (Map.Entry<Type, List<Class>> schemaClasses : SCHEMA_TYPE_CLASSES.entrySet()) {
            for (Class<?> schemaClass : schemaClasses.getValue())
                JAVA_CLASS_SCHEMA_TYPES.put(schemaClass, schemaClasses.getKey());
        }

        // 映射逻辑数据类型到Java类
        LOGICAL_TYPE_CLASSES.put(Decimal.LOGICAL_NAME, Collections.singletonList((Class) BigDecimal.class)); // 将Decimal逻辑类型映射到BigDecimal类
        LOGICAL_TYPE_CLASSES.put(Date.LOGICAL_NAME, Collections.singletonList((Class) java.util.Date.class)); // 将Date逻辑类型映射到java.util.Date类
        LOGICAL_TYPE_CLASSES.put(Time.LOGICAL_NAME, Collections.singletonList((Class) java.util.Date.class)); // 将Time逻辑类型映射到java.util.Date类
        LOGICAL_TYPE_CLASSES.put(Timestamp.LOGICAL_NAME, Collections.singletonList((Class) java.util.Date.class)); // 将Timestamp逻辑类型映射到java.util.Date类

        // 逻辑类型不需要放入JAVA_CLASS_SCHEMA_TYPES中
        // We don't need to put these into JAVA_CLASS_SCHEMA_TYPES since that's only used to determine schemas for
        // schemaless data and logical types will have ambiguous schemas (e.g. many of them use the same Java class) so
        // they should not be used without schemas.
    }

    // The type of the field
    private final Type type;
    private final boolean optional;
    private final Object defaultValue;

    private final List<Field> fields;
    private final Map<String, Field> fieldsByName;

    private final Schema keySchema;
    private final Schema valueSchema;

    // Optional name and version provide a built-in way to indicate what type of data is included. Most
    // useful for structs to indicate the semantics of the struct and map it to some existing underlying
    // serializer-specific schema. However, can also be useful in specifying other logical types (e.g. a set is an array
    // with additional constraints).
    private final String name;
    private final Integer version;
    // Optional human readable documentation describing this schema.
    private final String doc;
    private final Map<String, String> parameters;
    // precomputed hash code. There is no need to re-compute every time hashCode() is called.
    private Integer hash = null;

    /**
     * Construct a Schema. Most users should not construct schemas manually, preferring {@link SchemaBuilder} instead.
     */
    public ConnectSchema(Type type, boolean optional, Object defaultValue, String name, Integer version, String doc, Map<String, String> parameters, List<Field> fields, Schema keySchema, Schema valueSchema) {
        this.type = type;
        this.optional = optional;
        this.defaultValue = defaultValue;
        this.name = name;
        this.version = version;
        this.doc = doc;
        this.parameters = parameters;

        if (this.type == Type.STRUCT) {
            this.fields = fields == null ? Collections.<Field>emptyList() : fields;
            this.fieldsByName = new HashMap<>(this.fields.size());
            for (Field field : this.fields)
                fieldsByName.put(field.name(), field);
        } else {
            this.fields = null;
            this.fieldsByName = null;
        }

        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
    }

    /**
     * Construct a Schema for a primitive type, setting schema parameters, struct fields, and key and value schemas to null.
     */
    public ConnectSchema(Type type, boolean optional, Object defaultValue, String name, Integer version, String doc) {
        this(type, optional, defaultValue, name, version, doc, null, null, null, null);
    }

    /**
     * Construct a default schema for a primitive type. The schema is required, has no default value, name, version,
     * or documentation.
     */
    public ConnectSchema(Type type) {
        this(type, false, null, null, null, null);
    }

    @Override
    public Type type() {
        return type;
    }

    @Override
    public boolean isOptional() {
        return optional;
    }

    @Override
    public Object defaultValue() {
        return defaultValue;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Integer version() {
        return version;
    }

    @Override
    public String doc() {
        return doc;
    }

    @Override
    public Map<String, String> parameters() {
        return parameters;
    }

    @Override
    public List<Field> fields() {
        if (type != Type.STRUCT)
            throw new DataException("Cannot list fields on non-struct type");
        return fields;
    }

    @Override
    public Field field(String fieldName) {
        if (type != Type.STRUCT)
            throw new DataException("Cannot look up fields on non-struct type");
        return fieldsByName.get(fieldName);
    }

    @Override
    public Schema keySchema() {
        if (type != Type.MAP)
            throw new DataException("Cannot look up key schema on non-map type");
        return keySchema;
    }

    @Override
    public Schema valueSchema() {
        if (type != Type.MAP && type != Type.ARRAY)
            throw new DataException("Cannot look up value schema on non-array and non-map type");
        return valueSchema;
    }



    /**
     * Validate that the value can be used with the schema, i.e. that its type matches the schema type and nullability
     * requirements. Throws a DataException if the value is invalid.
     * @param schema Schema to test
     * @param value value to test
     */
    public static void validateValue(Schema schema, Object value) {
        validateValue(null, schema, value);
    }

    public static void validateValue(String name, Schema schema, Object value) {
        if (value == null) {
            // 如果值为null
            if (!schema.isOptional())
                // 如果schema不是可选的
                throw new DataException("Invalid value: null used for required field: \"" + name
                        + "\", schema type: " + schema.type());
            // 返回，结束函数执行
            return;
        }

        // 获取schema期望的类列表
        List<Class> expectedClasses = expectedClassesFor(schema);
        boolean foundMatch = false;
        // 如果期望的类只有一个
        if (expectedClasses.size() == 1) {
            // 判断值是否匹配期望的类
            foundMatch = expectedClasses.get(0).isInstance(value);
        } else {
            // 遍历所有期望的类
            for (Class<?> expectedClass : expectedClasses) {
                // 判断值是否匹配某个期望的类
                if (expectedClass.isInstance(value)) {
                    foundMatch = true;
                    break;
                }
            }
        }

        // 如果没有找到匹配的类
        if (!foundMatch) {
            // 构建异常信息
            StringBuilder exceptionMessage = new StringBuilder("Invalid Java object for schema");
            if (schema.name() != null) {
                // 如果schema有名称，则添加到异常信息中
                exceptionMessage.append(" \"").append(schema.name()).append("\"");
            }
            exceptionMessage.append(" with type ").append(schema.type()).append(": ").append(value.getClass());
            if (name != null) {
                // 如果name不为null，则添加到异常信息中
                exceptionMessage.append(" for field: \"").append(name).append("\"");
            }
            throw new DataException(exceptionMessage.toString());
        }

        // 根据schema的类型进行不同的处理
        switch (schema.type()) {
            case STRUCT:
                // 如果schema类型为STRUCT
                Struct struct = (Struct) value;
                if (!struct.schema().equals(schema))
                    // 如果struct的schema与给定的schema不匹配
                    throw new DataException("Struct schemas do not match.");
                // 验证struct
                struct.validate();
                break;
            case ARRAY:
                // 如果schema类型为ARRAY
                List<?> array = (List<?>) value;
                // 遍历数组中的每个元素进行验证
                for (Object entry : array)
                    validateValue(schema.valueSchema(), entry);
                break;
            case MAP:
                // 如果schema类型为MAP
                Map<?, ?> map = (Map<?, ?>) value;
                // 遍历map中的每个键值对进行验证
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    validateValue(schema.keySchema(), entry.getKey());
                    validateValue(schema.valueSchema(), entry.getValue());
                }
                break;
        }
    }

    private static List<Class> expectedClassesFor(Schema schema) {
        List<Class> expectedClasses = LOGICAL_TYPE_CLASSES.get(schema.name());
        if (expectedClasses == null)
            expectedClasses = SCHEMA_TYPE_CLASSES.getOrDefault(schema.type(), Collections.emptyList());
        return expectedClasses;
    }

    /**
     * Validate that the value can be used for this schema, i.e. that its type matches the schema type and optional
     * requirements. Throws a DataException if the value is invalid.
     * @param value the value to validate
     */
    public void validateValue(Object value) {
        validateValue(this, value);
    }

    @Override
    public ConnectSchema schema() {
        return this;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConnectSchema schema = (ConnectSchema) o;
        return Objects.equals(optional, schema.optional) &&
                Objects.equals(version, schema.version) &&
                Objects.equals(name, schema.name) &&
                Objects.equals(doc, schema.doc) &&
                Objects.equals(type, schema.type) &&
                Objects.deepEquals(defaultValue, schema.defaultValue) &&
                Objects.equals(fields, schema.fields) &&
                Objects.equals(keySchema, schema.keySchema) &&
                Objects.equals(valueSchema, schema.valueSchema) &&
                Objects.equals(parameters, schema.parameters);
    }

    @Override
    public int hashCode() {
        if (this.hash == null) {
            this.hash = Objects.hash(type, optional, defaultValue, fields, keySchema, valueSchema, name, version, doc,
                parameters);
        }
        return this.hash;
    }

    @Override
    public String toString() {
        if (name != null)
            return "Schema{" + name + ":" + type + "}";
        else
            return "Schema{" + type + "}";
    }


    /**
     * Get the {@link Schema.Type} associated with the given class.
     *
     * @param klass the Class to
     * @return the corresponding type, or null if there is no matching type
     */
    public static Type schemaType(Class<?> klass) {
        synchronized (JAVA_CLASS_SCHEMA_TYPES) {
            Type schemaType = JAVA_CLASS_SCHEMA_TYPES.get(klass);
            if (schemaType != null)
                return schemaType;

            // Since the lookup only checks the class, we need to also try
            for (Map.Entry<Class<?>, Type> entry : JAVA_CLASS_SCHEMA_TYPES.entrySet()) {
                try {
                    klass.asSubclass(entry.getKey());
                    // Cache this for subsequent lookups
                    JAVA_CLASS_SCHEMA_TYPES.put(klass, entry.getValue());
                    return entry.getValue();
                } catch (ClassCastException e) {
                    // Expected, ignore
                }
            }
        }
        return null;
    }
}
