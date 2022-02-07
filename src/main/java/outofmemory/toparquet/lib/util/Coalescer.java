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

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.val;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.stream.Collectors;

@UtilityClass
public class Coalescer {

    /**
     * @return
     *      If input is null, the protoType is returned.
     *      If a property of the input is null, it assigned the prototype's value
     */
    @SneakyThrows
    public <T, S extends T> T coalesce(S input, T protoType) {
        if (input == null)
            return protoType;
        Class classT = protoType.getClass();

        val fields = Arrays
                .stream(classT.getDeclaredFields())
                .filter(f -> !Modifier.isStatic(f.getModifiers()))
                .collect(Collectors.toList());
        for (Field f: fields) {
            f.setAccessible(true);
            if (f.get(input) == null)
                f.set(input, f.get(protoType));
        }
        return input;
    }

}
