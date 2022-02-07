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

import junit.framework.TestCase;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import outofmemory.toparquet.lib.Converter;
import outofmemory.toparquet.lib.config.ConversionConfig;
import outofmemory.toparquet.lib.config.Partition;
import outofmemory.toparquet.lib.config.PartitionType;
import outofmemory.toparquet.lib.domain.InputErrorTolerance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class IntegrationTests {

}
