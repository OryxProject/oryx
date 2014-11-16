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

import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import com.typesafe.config.Config;
import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

/**
 * Tests {@link ConfigUtils}.
 */
public final class ConfigUtilsTest extends OryxTest {

  @Test
  public void testDefaultConfig() {
    Config config = ConfigUtils.getDefault();
    assertEquals("yarn-client", config.getString("batch.streaming.master"));
  }

  @Test
  public void testSerialize() {
    String serialized = ConfigUtils.serialize(ConfigUtils.getDefault());
    assertTrue(serialized.contains("update-class"));
    Config deserialized = ConfigUtils.deserialize(serialized);
    assertEquals(
        ConfigUtils.getDefault().getString("serving.api.port"),
        deserialized.getString("serving.api.port"));
  }

  @Test
  public void testOptional() {
    assertNull(ConfigUtils.getOptionalString(ConfigUtils.getDefault(), "nonexistent"));
  }

  @Test
  public void testOverlayOn() {
    Map<String,String> overlay = new HashMap<>();
    overlay.put("foo", "bar");
    Config config = ConfigUtils.overlayOn(overlay, ConfigUtils.getDefault());
    assertEquals("bar", config.getString("foo"));
  }

  @Test
  public void testSetPath() throws Exception {
    Path cwd = Paths.get(".");
    Map<String,String> map = new HashMap<>();
    ConfigUtils.set(map, "cwd", cwd);
    ConfigUtils.set(map, "temp", Paths.get("/tmp"));
    assertEquals("\"" + cwd.toRealPath(LinkOption.NOFOLLOW_LINKS).toUri() + "\"", map.get("cwd"));
    assertEquals("\"file:///tmp/\"", map.get("temp"));
  }

  @Test
  public void testPrettyPrint() {
    String pretty = ConfigUtils.prettyPrint(ConfigUtils.getDefault());
    // Spot checks:
    assertTrue(pretty.contains("batch {"));
    assertTrue(pretty.contains("checkpoint-dir=null"));
  }

}
