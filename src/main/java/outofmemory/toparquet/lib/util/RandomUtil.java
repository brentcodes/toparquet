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

@UtilityClass
public class RandomUtil {
    final private java.util.Random random = new java.util.Random();
    public int positiveInt() {
        int i = random.nextInt();
        return i & 0x7FFFFFFF;
    }

    public int nextInt() {
        return random.nextInt();
    }
}
