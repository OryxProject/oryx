/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.oryx.common.settings;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;

/**
 * Utility that outputs all configuration as key-value pairs (as in a .properties file)
 * to stdout.
 */
public final class ConfigToProperties {

  private ConfigToProperties() {}

  public static void main(String[] args) throws Exception {
    for (String line : buildPropertiesLines()) {
      System.out.println(line);
    }
  }

  static List<String> buildPropertiesLines() {
    ConfigObject config = (ConfigObject) ConfigUtils.getDefault().root().get("oryx");
    Map<String,String> keyValueMap = new TreeMap<>();
    add(config, "oryx", keyValueMap);
    List<String> propertiesLines = new ArrayList<>(keyValueMap.size());
    for (Map.Entry<String,String> e : keyValueMap.entrySet()) {
      propertiesLines.add(e.getKey() + "=" + e.getValue());
    }
    return propertiesLines;
  }

  private static void add(ConfigObject config, String prefix, Map<String,String> values) {
    for (Map.Entry<String, ConfigValue> e : config.entrySet()) {
      String nextPrefix = prefix + "." + e.getKey();
      ConfigValue value = e.getValue();
      switch (value.valueType()) {
        case OBJECT:
          add((ConfigObject) value, nextPrefix, values);
          break;
        case NULL:
          // do nothing
          break;
        default:
          values.put(nextPrefix, String.valueOf(value.unwrapped()));
      }
    }
  }

}
