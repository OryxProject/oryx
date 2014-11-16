/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Map;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for retrieving {@code Config} instances.
 */
public final class ConfigUtils {

  private static final Logger log = LoggerFactory.getLogger(ConfigUtils.class);

  private static final Config DEFAULT_CONFIG = ConfigFactory.load();

  private ConfigUtils() {}

  /**
   * Returns the default {@code Config} object for this app, based on config in the JAR file
   * or otherwise specified to the library.
   *
   * @return default configuration
   */
  public static Config getDefault() {
    return DEFAULT_CONFIG;
  }

  /**
   * @param overlay map of key-value pairs to add to default config. The map is converted
   *  to a string representation, as it were from a config file, and parsed accordingly.
   * @param underlying underlying config to overlay new settings on top of
   * @return default config but with key-value pairs added
   */
  public static Config overlayOn(Map<String,String> overlay, Config underlying) {
    StringBuilder configFileString = new StringBuilder();
    for (Map.Entry<String,String> entry : overlay.entrySet()) {
      configFileString.append(entry.getKey()).append('=').append(entry.getValue()).append('\n');
    }
    String configFile = configFileString.toString();
    log.info("Overlaid config: \n{}", configFile);
    return ConfigFactory.parseString(configFile).resolve().withFallback(underlying);
  }

  /**
   * @param config configuration to query for value
   * @param key configuration path key
   * @return value for given key, or {@code null} if none exists
   */
  public static String getOptionalString(Config config, String key) {
    return config.hasPath(key) ? config.getString(key) : null;
  }

  /**
   * Helper to set a {@link Path} value correctly for use with {@link #overlayOn(Map,Config)}.
   *
   * @param overlay key-value pairs to overlay on a {@link Config}
   * @param key key to set
   * @param path {@link Path} value
   * @throws IOException if {@link Path} can't be made canonical
   */
  public static void set(Map<String,String> overlay, String key, Path path) throws IOException {
    Path finalPath = Files.exists(path, LinkOption.NOFOLLOW_LINKS) ?
        path.toRealPath(LinkOption.NOFOLLOW_LINKS) :
        path;
    overlay.put(key, "\"" + finalPath.toUri() + "\"");
  }

  /**
   * @param config {@link Config} to serialize to a String
   * @return JSON-like representation of properties in the configuration, excluding those
   *  inherited from the local JVM environment
   */
  public static String serialize(Config config) {
    return config.root().withOnlyKey("oryx").render(ConfigRenderOptions.concise());
  }

  /**
   * @param serialized serialized form of configuration as JSON-like data
   * @return {@link Config} from the serialized config
   */
  public static Config deserialize(String serialized) {
    return ConfigFactory.parseString(serialized).resolve().withFallback(DEFAULT_CONFIG);
  }

  /**
   * @param config {@link Config} to print
   * @return pretty-printed version of config values, excluding those
   *  inherited from the local JVM environment
   */
  public static String prettyPrint(Config config) {
    ConfigRenderOptions options = ConfigRenderOptions.defaults()
        .setComments(false)
        .setOriginComments(false)
        .setFormatted(true)
        .setJson(false);
    return config.root().withOnlyKey("oryx").render(options);
  }

}
