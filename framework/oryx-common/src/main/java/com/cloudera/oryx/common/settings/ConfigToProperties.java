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

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.typesafe.config.ConfigObject;

/**
 * Utility that outputs all configuration as key-value pairs (as in a .properties file)
 * to stdout.
 */
public final class ConfigToProperties {

  private ConfigToProperties() {}

  public static void main(String[] args) {
    buildPropertiesLines().forEach(System.out::println);
  }

  static List<String> buildPropertiesLines() {
    ConfigObject config = (ConfigObject) ConfigUtils.getDefault().root().get("oryx");
    Map<String,String> keyValueMap = new TreeMap<>();
    add(config, "oryx", keyValueMap);
    return keyValueMap.entrySet().stream().map(e -> e.getKey() + '=' + e.getValue()).collect(Collectors.toList());
  }

  private static void add(ConfigObject config, String prefix, Map<String,String> values) {
    config.forEach((key, value) -> {
      String nextPrefix = prefix + '.' + key;
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
    });
  }

}
