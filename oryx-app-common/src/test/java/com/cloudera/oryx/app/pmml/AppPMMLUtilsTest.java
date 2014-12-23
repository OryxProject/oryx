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

package com.cloudera.oryx.app.pmml;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.typesafe.config.Config;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DataType;
import org.dmg.pmml.FieldUsageType;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.MiningFunctionType;
import org.dmg.pmml.MiningSchema;
import org.dmg.pmml.Node;
import org.dmg.pmml.OpType;
import org.dmg.pmml.PMML;
import org.dmg.pmml.TreeModel;
import org.junit.Test;

import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;

public final class AppPMMLUtilsTest extends OryxTest {

  private static PMML buildDummyModel() {
    Node node = new Node();
    node.setRecordCount(123.0);
    TreeModel treeModel = new TreeModel(null, node, MiningFunctionType.CLASSIFICATION);
    PMML pmml = PMMLUtils.buildSkeletonPMML();
    pmml.getModels().add(treeModel);
    return pmml;
  }

  @Test
  public void testExtensionValue() {
    PMML model = buildDummyModel();
    assertNull(AppPMMLUtils.getExtensionValue(model, "foo"));
    AppPMMLUtils.addExtension(model, "foo", "bar");
    assertEquals("bar", AppPMMLUtils.getExtensionValue(model, "foo"));
  }

  @Test
  public void testExtensionContent() {
    PMML model = buildDummyModel();
    assertNull(AppPMMLUtils.getExtensionContent(model, "foo"));
    AppPMMLUtils.addExtensionContent(model, "foo", Arrays.asList("bar", "baz"));
    assertEquals(Arrays.<Object>asList("bar", "baz"),
                 AppPMMLUtils.getExtensionContent(model, "foo"));
    AppPMMLUtils.addExtensionContent(model, "foo", Collections.emptyList());
    assertEquals(Arrays.<Object>asList("bar", "baz"),
                 AppPMMLUtils.getExtensionContent(model, "foo"));
  }

  @Test
  public void testParseArray() {
    assertEquals(Arrays.asList("foo", "bar", "baz"),
                 AppPMMLUtils.parseArray(Collections.singletonList("foo bar baz")));
    assertTrue(AppPMMLUtils.parseArray(Collections.singletonList("")).isEmpty());
  }

  private static InputSchema buildTestSchema() {
    Map<String,String> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.input-schema.feature-names", "[\"foo\",\"bar\",\"baz\",\"bing\"]");
    overlayConfig.put("oryx.input-schema.id-features", "[\"baz\"]");
    overlayConfig.put("oryx.input-schema.ignored-features", "[\"foo\"]");
    overlayConfig.put("oryx.input-schema.categorical-features", "[\"bar\"]");
    overlayConfig.put("oryx.input-schema.target-feature", "\"bar\"");
    Config config = ConfigUtils.overlayOn(overlayConfig, ConfigUtils.getDefault());
    return new InputSchema(config);
  }

  @Test
  public void testBuildMiningSchema() {
    MiningSchema miningSchema = AppPMMLUtils.buildMiningSchema(buildTestSchema());

    List<MiningField> miningFields = miningSchema.getMiningFields();
    assertEquals(4, miningFields.size());

    String[] fieldNames = { "foo", "bar", "baz", "bing" };
    for (int i = 0; i < fieldNames.length; i++) {
      assertEquals(fieldNames[i], miningFields.get(i).getName().getValue());
    }

    assertEquals(FieldUsageType.SUPPLEMENTARY, miningFields.get(0).getUsageType());
    assertEquals(FieldUsageType.PREDICTED, miningFields.get(1).getUsageType());
    assertEquals(FieldUsageType.SUPPLEMENTARY, miningFields.get(2).getUsageType());
    assertEquals(FieldUsageType.ACTIVE, miningFields.get(3).getUsageType());

    assertEquals(OpType.CATEGORICAL, miningFields.get(1).getOptype());
    assertEquals(OpType.CONTINUOUS, miningFields.get(3).getOptype());
  }

  @Test
  public void testBuildDataDictionary() {
    Map<Integer,BiMap<String,Double>> distinctValueMaps = new HashMap<>();
    BiMap<String,Double> values = HashBiMap.create();
    values.put("four", 4.0);
    values.put("one", 1.0);
    values.put("five", 5.0);
    values.put("three", 3.0);
    values.put("two", 2.0);
    distinctValueMaps.put(1, values);

    DataDictionary dictionary =
        AppPMMLUtils.buildDataDictionary(buildTestSchema(), distinctValueMaps);
    assertEquals(2, dictionary.getNumberOfFields().intValue());
    DataField df0 = dictionary.getDataFields().get(0);
    assertEquals("bar", df0.getName().getValue());
    assertEquals(OpType.CATEGORICAL, df0.getOptype());
    assertEquals(DataType.STRING, df0.getDataType());
    DataField df1 = dictionary.getDataFields().get(1);
    assertEquals("bing", df1.getName().getValue());
    assertEquals(OpType.CONTINUOUS, df1.getOptype());
    assertEquals(DataType.DOUBLE, df1.getDataType());

    assertEquals(5, df0.getValues().size());
    String[] categoricalValues = { "one", "two", "three", "four", "five" };
    for (int i = 0; i < categoricalValues.length; i++) {
      assertEquals(categoricalValues[i], df0.getValues().get(i).getValue());
    }
  }

}
