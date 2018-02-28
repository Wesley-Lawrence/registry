/*
 * Copyright 2016 Hortonworks.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.registries.schemaregistry.avro;

import com.hortonworks.registries.schemaregistry.*;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class AvroNestedCheckerTest {

    private static Schema simpleNestedSchema;
    private static Schema complexNestedSchema;
    private static Schema compositeSchema;
    private static SchemaVersionRetriever mySchemaVersionRetriever;

    @BeforeClass
    public static void beforeClass() throws IOException {
        Schema.Parser schemaParser = new Schema.Parser();
        simpleNestedSchema = schemaParser.parse(AvroNestedCheckerTest.class.getResourceAsStream("/avro/nested/nested-simple.avsc"));
        complexNestedSchema = schemaParser.parse(AvroNestedCheckerTest.class.getResourceAsStream("/avro/nested/nested-complex.avsc"));
        compositeSchema = schemaParser.parse(AvroNestedCheckerTest.class.getResourceAsStream("/avro/nested/nested-composite.avsc"));

        mySchemaVersionRetriever = new SchemaVersionRetriever() {
            @Override
            public SchemaVersionInfo retrieveSchemaVersion(SchemaVersionKey key) throws SchemaNotFoundException {

                if (key.getSchemaName().equals(simpleNestedSchema.getFullName())) {
                    return new SchemaVersionInfo(
                            -1L,
                            simpleNestedSchema.getFullName(),
                            -1,
                            simpleNestedSchema.toString(),
                            0L,
                            ""
                    );
                }

                if (key.getSchemaName().equals(complexNestedSchema.getFullName())) {
                    return new SchemaVersionInfo(
                            -2L,
                            complexNestedSchema.getFullName(),
                            -1,
                            complexNestedSchema.toString(),
                            0L,
                            ""
                    );
                }

                throw new SchemaNotFoundException();
            }

            @Override
            public SchemaVersionInfo retrieveSchemaVersion(SchemaIdVersion key) throws SchemaNotFoundException {
                throw new SchemaNotFoundException();
            }
        };
    }

    @Test
    public void testAvroFieldsGenerator_Simple() throws IOException {
        List<SchemaFieldInfo> results = new AvroFieldsGenerator().generateFields(simpleNestedSchema);
    }

    @Test
    public void testAvroFieldsGenerator_Complex() throws IOException {
        List<SchemaFieldInfo> results = new AvroFieldsGenerator().generateFields(complexNestedSchema);
    }

    @Test
    public void testAvroSchemaProvider_Simple() throws IOException {
        // As long as no exceptions are thrown, this passes.
        new AvroSchemaProvider().normalize(simpleNestedSchema);
    }

    @Test
    public void testAvroSchemaProvider_Complex() throws IOException {
        // As long as no exceptions are thrown, this passes.
        new AvroSchemaProvider().normalize(complexNestedSchema);
    }

    @Ignore
    @Test
    public void testAvroSchemaResolver() throws IOException, InvalidSchemaException, SchemaNotFoundException {
        new AvroSchemaResolver(mySchemaVersionRetriever).resolveSchema(compositeSchema.toString());
    }

    @Ignore
    @Test
    public void testAvroSchemaResolver_HandleUnionFields_Simple() throws IOException, InvalidSchemaException, SchemaNotFoundException {
        // As long as no exceptions are thrown, this passes.
        Schema newSchema = new AvroSchemaResolver(mySchemaVersionRetriever).handleUnionFieldsWithNull(simpleNestedSchema, new HashSet<>());
        Assert.assertTrue("Schema has multiple schema objects for a record name!", doesSchemaNotReuseRecordNameWithDifferentRecordInstances(newSchema));
    }

    @Ignore
    @Test
    public void testAvroSchemaResolver_HandleUnionFields_Complex() throws IOException, InvalidSchemaException, SchemaNotFoundException {
        // As long as no exceptions are thrown, this passes.
        Schema newSchema = new AvroSchemaResolver(mySchemaVersionRetriever).handleUnionFieldsWithNull(complexNestedSchema, new HashSet<>());
        Assert.assertTrue("Schema has multiple schema objects for a record name!", doesSchemaNotReuseRecordNameWithDifferentRecordInstances(newSchema));
    }

    @Test
    public void testInternalValidateMethod() {
        // Avro's Schema.Parser shouldn't create Schema objects that reuse a schema name, with different record instances.
        Assert.assertTrue("Simple Schema has multiple schema objects for a record name!", doesSchemaNotReuseRecordNameWithDifferentRecordInstances(simpleNestedSchema));
        Assert.assertTrue("Complex Schema has multiple schema objects for a record name!", doesSchemaNotReuseRecordNameWithDifferentRecordInstances(complexNestedSchema));
    }

    private boolean doesSchemaNotReuseRecordNameWithDifferentRecordInstances(Schema inputSchema) {
        return doesSchemaNotReuseRecordNameWithDifferentRecordInstances(inputSchema, new HashMap<>());
    }

    private boolean doesSchemaNotReuseRecordNameWithDifferentRecordInstances(Schema inputSchema, Map<String, Schema> foundRecordSchemas) {

        switch (inputSchema.getType()) {
            case RECORD:
                String name = inputSchema.getFullName();

                if (foundRecordSchemas.containsKey(name)) {
                    Schema originalSchema = foundRecordSchemas.get(name);

                    // Compare object instance, not contents. We shouldn't have different instances of the same Schema.
                    return inputSchema == originalSchema;
                }
                else {
                    foundRecordSchemas.put(name, inputSchema);

                    // Validate each possible field in the record
                    for (Schema.Field field : inputSchema.getFields()) {
                        if (!doesSchemaNotReuseRecordNameWithDifferentRecordInstances(field.schema(), foundRecordSchemas)) {
                            // A sub field is invalid, return false.
                            return false;
                        }
                    }
                    return true;
                }
            case UNION:
                // Validate each possible type in the union
                for (Schema type : inputSchema.getTypes()) {
                    if (!doesSchemaNotReuseRecordNameWithDifferentRecordInstances(type, foundRecordSchemas)) {
                        // One of the types is invalid, return false.
                        return false;
                    }
                }
                return true;
            case ARRAY:
                // Validate the Array element type
                return doesSchemaNotReuseRecordNameWithDifferentRecordInstances(inputSchema.getElementType(), foundRecordSchemas);
            case MAP:
                // Validate the Map value type (keys are always Strings)
                return doesSchemaNotReuseRecordNameWithDifferentRecordInstances(inputSchema.getValueType(), foundRecordSchemas);
            default:
                // Everything else is valid.
                return true;
        }
    }
}
