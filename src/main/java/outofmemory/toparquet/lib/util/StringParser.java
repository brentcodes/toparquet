/*  Copyright 2021 brentcodes

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 */
package outofmemory.toparquet.lib.util;

import lombok.experimental.UtilityClass;
import org.apache.avro.Schema;

import java.math.BigDecimal;
import java.util.Base64;
import java.util.function.Function;

/**
 * Extracts the appropriate value from a string given the parquet schema
 */
@UtilityClass
public class StringParser {
    public Object parse(String s, Schema schema) {
        final Schema.Type avroType = schema.getType();
        if (s == null || "".equals(s))
            return null;
        switch (avroType) {
            case BOOLEAN:
                if ("1".equals(s))
                    return true;
                return Boolean.parseBoolean(s);
            case INT:
                return parseNumber(Integer::parseInt, s);
            case LONG:
                return parseNumber(Long::parseLong, s);
            case FLOAT:
                return parseNumber(Float::parseFloat, s);
            case DOUBLE:
                return parseNumber(Double::parseDouble, s);
            case BYTES:
                return Base64.getDecoder().decode(s);
            case STRING:
               return s;
            case RECORD:
                error(avroType);
            case ENUM:
                return s;
            case ARRAY:
                error(avroType);
            case MAP:
                error(avroType);
            case UNION: // Only unions representing nullable types are supported
                // A union representing a nullable base type will always have two component types
                if (schema.getTypes().size() != 2)
                    error(avroType);
                final boolean is0Null = schema.getTypes().get(0).getType() == Schema.Type.NULL;
                final boolean is1Null = schema.getTypes().get(1).getType() == Schema.Type.NULL;
                // A union representing nullable types will have one of the types be null, and the other be the base type
                if (!(is0Null ^ is1Null))
                    error(avroType);
                if (is0Null)
                    return parse(s, schema.getTypes().get(1));
                else
                    return parse(s, schema.getTypes().get(0));
            case FIXED:
                return parseNumber(BigDecimal::new, s);
            default:
                error(avroType);
        }
        return null;
    }

    private void error(Schema.Type avroType) {
        throw new UnsupportedOperationException("Conversion not implemented for " + avroType);
    }

    Object parseNumber(Function<String, ?> parseFunction, String s) {
        try {
            return parseFunction.apply(s);
        }
        catch (NumberFormatException e) {
            return null;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}
