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
package outofmemory.toparquet.lib.domain;

import outofmemory.toparquet.lib.util.RandomUtil;

public enum ColumnTransformationTypes implements ColumnTransformationType {
    getValue {
        @Override
        public Object transform(TransformationContext contextualArguments, Object[] inputValues) {
            return inputValues[0];
        }
    },
    rand {
        @Override
        public Object transform(TransformationContext contextualArguments, Object[] inputValues) {
            return RandomUtil.positiveInt();
        }
    },
    increment {
        @Override
        public Object transform(TransformationContext contextualArguments, Object[] inputValues) {
            if (contextualArguments.sequence > contextualArguments.upperBound) {
                contextualArguments.sequence = contextualArguments.lowerBound;
            }
            return contextualArguments.sequence++;
        }
    },
    hash {
        final int someInt = 0xcc9e2d51;
        @Override
        public Object transform(TransformationContext contextualArguments, Object[] inputValues) {
            int output = 0;
            int i = 0;
            for (Object value: inputValues) {
                final int hash = value == null ? 0 : inputValues.hashCode();
                output ^= (Integer.rotateLeft(hash, i)) * someInt;
            }
            return output;
        }
    }
    ;

}
