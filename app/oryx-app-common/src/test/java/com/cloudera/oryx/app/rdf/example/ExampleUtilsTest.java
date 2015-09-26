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

package com.cloudera.oryx.app.rdf.example;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.typesafe.config.Config;
import org.junit.Test;

import com.cloudera.oryx.app.schema.CategoricalValueEncodings;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.settings.ConfigUtils;

public final class ExampleUtilsTest extends OryxTest {

  @Test
  public void testToExample() {
    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.input-schema.num-features", 5);
    overlayConfig.put("oryx.input-schema.categorical-features", "[\"4\"]");
    overlayConfig.put("oryx.input-schema.id-features", "[\"0\"]");
    overlayConfig.put("oryx.input-schema.target-feature", "\"4\"");
    Config config = ConfigUtils.overlayOn(overlayConfig, ConfigUtils.getDefault());

    InputSchema schema = new InputSchema(config);
    CategoricalValueEncodings encodings =
        new CategoricalValueEncodings(Collections.singletonMap(4, Arrays.asList("A", "B", "C")));

    Example example = ExampleUtils.dataToExample(new String[] {"foo", "1", "2.5", "-3.2", "B"}, schema, encodings);
    assertEquals(CategoricalFeature.forEncoding(1), example.getTarget());
    assertNull(example.getFeature(0));
    assertEquals(NumericFeature.forValue(1.0), example.getFeature(1));
    assertEquals(NumericFeature.forValue(2.5), example.getFeature(2));
    assertEquals(NumericFeature.forValue(-3.2), example.getFeature(3));
    assertNull(example.getFeature(4));
  }

}
