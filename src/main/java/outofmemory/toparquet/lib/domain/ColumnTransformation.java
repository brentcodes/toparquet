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

import org.apache.avro.generic.GenericData;

import java.util.List;

/**
 * Extracts a value from any number of columns which can be used for partitioning or creating a new column
 */
public class ColumnTransformation {
    private final List<String> inputColumns;
    private final ColumnTransformationType transformationType;
    private final Object[] inputs;
    private final ColumnTransformationType.TransformationContext transformationContext = new ColumnTransformationType.TransformationContext();

    private ColumnTransformation(List<String> inputColumns, ColumnTransformationType transformationType) {
        this.inputColumns = inputColumns;
        this.transformationType = transformationType;
        this.inputs = new Object[inputColumns.size()];
    }

    public static ColumnTransformation createSimple(List<String> inputColumns, ColumnTransformationType transformationType) {
        return new ColumnTransformation(inputColumns, transformationType);
    }

    public static ColumnTransformation createBounded(List<String> inputColumns, ColumnTransformationType transformationType, int lowerBound, int upperBound) {
        final ColumnTransformation transformation = new ColumnTransformation(inputColumns, transformationType);
        transformation.transformationContext.lowerBound = lowerBound;
        transformation.transformationContext.upperBound = upperBound;
        transformation.transformationContext.sequence = lowerBound;
        return transformation;
    }

    public Object execute(GenericData.Record record) {
        for (int i = 0; i < inputs.length; ++i) {
            inputs[i] = record.get(inputColumns.get(i));
        }
        return transformationType.transform(transformationContext, inputs);
    }
}
