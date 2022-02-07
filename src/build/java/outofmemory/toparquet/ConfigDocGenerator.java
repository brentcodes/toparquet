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
package outofmemory.toparquet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import outofmemory.toparquet.lib.config.ConversionConfig;
import outofmemory.toparquet.lib.config.Description;
import outofmemory.toparquet.lib.util.Coalescer;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * Dynamically creates the config.md documentation during the build based on the fields in class and their annotations
 */
public class ConfigDocGenerator {
    public static void main(String[] args) {
        System.out.println("Generating Config Docs");
        String dir = args.length > 0 ? args[0] : "";
        if (dir.length() > 0 && !dir.endsWith("/"))
            dir += "/";
        ConversionConfig sample = new ConversionConfig();
        sample.setSeparator(',');
        sample = Coalescer.coalesce(sample, ConversionConfig.getDefaults());

        FileWriter fileWriter = null;
        try {
            final String outputPath = dir + "config.md";
            System.out.println("Writing Doc to: " + outputPath);
             fileWriter = new FileWriter(outputPath, false);

             fileWriter.append("\n [comment]: # ( This file was auto-generated! Do not modify it directly. ) \n");

            Class<ConversionConfig> clazz = ConversionConfig.class;
            for (Field f : clazz.getDeclaredFields()) {
                f.setAccessible(true);
                if (Modifier.isStatic(f.getModifiers())) {
                    continue;
                }
                Description description = f.getAnnotation(Description.class);
                if (description == null) continue;
                fileWriter.append("### " + f.getName() + "\n\n");
                fileWriter.append(description.value() + "\n\n");
                final boolean autodetected = description.defaultValue().equals(Description.AUTO_DETECT_VALUE);

                String defaultValue = autodetected ?
                        (f.get(ConversionConfig.getDefaults()) == null ? null : f.get(ConversionConfig.getDefaults()).toString()) :
                        description.defaultValue();

                if (defaultValue == null) {
                    defaultValue = "***null***";
                }
                else if (autodetected){
                    defaultValue = "`" + defaultValue + "`";
                }

                fileWriter.append("*Default Value*:\n\n" );
                fileWriter.append(" " + defaultValue + "\n\n");
                fileWriter.append("---\n");
            }


            fileWriter.append("## Sample Json Config \n");

            final ObjectMapper mapper = new ObjectMapper().configure(SerializationFeature.INDENT_OUTPUT, true);
            fileWriter.append("```json\n");
            fileWriter.append(mapper.writeValueAsString(sample));
            fileWriter.append("\n```\n");


            fileWriter.flush();
            System.out.println("Config Docs Generated");
        } catch (IOException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        finally {
            if (fileWriter != null) {
                try {
                    fileWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }




    }
}
