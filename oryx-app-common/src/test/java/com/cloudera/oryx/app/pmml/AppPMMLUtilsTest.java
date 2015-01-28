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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DataType;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.FieldUsageType;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.MiningFunctionType;
import org.dmg.pmml.MiningSchema;
import org.dmg.pmml.Node;
import org.dmg.pmml.OpType;
import org.dmg.pmml.PMML;
import org.dmg.pmml.TreeModel;
import org.dmg.pmml.Value;
import org.junit.Test;

import com.cloudera.oryx.app.schema.CategoricalValueEncodings;
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
    Map<String,Object> overlayConfig = new HashMap<>();
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
    Map<Integer,Collection<String>> distinctValues = new HashMap<>();
    distinctValues.put(1, Arrays.asList("one", "two", "three", "four", "five"));
    CategoricalValueEncodings categoricalValueEncodings =
        new CategoricalValueEncodings(distinctValues);

    DataDictionary dictionary =
        AppPMMLUtils.buildDataDictionary(buildTestSchema(), categoricalValueEncodings);
    assertEquals(4, dictionary.getNumberOfFields().intValue());
    checkDataField(dictionary.getDataFields().get(0), "foo", null);
    checkDataField(dictionary.getDataFields().get(1), "bar", true);
    checkDataField(dictionary.getDataFields().get(2), "baz", null);
    checkDataField(dictionary.getDataFields().get(3), "bing", false);

    List<Value> dfValues = dictionary.getDataFields().get(1).getValues();
    assertEquals(5, dfValues.size());
    String[] categoricalValues = { "one", "two", "three", "four", "five" };
    for (int i = 0; i < categoricalValues.length; i++) {
      assertEquals(categoricalValues[i], dfValues.get(i).getValue());
    }
  }

  @Test
  public void testListFeaturesDD() {
    Map<Integer,Collection<String>> distinctValues = new HashMap<>();
    distinctValues.put(1, Arrays.asList("one", "two", "three", "four", "five"));
    CategoricalValueEncodings categoricalValueEncodings =
        new CategoricalValueEncodings(distinctValues);
    DataDictionary dictionary = AppPMMLUtils.buildDataDictionary(
        buildTestSchema(), categoricalValueEncodings);
    List<String> featureNames = AppPMMLUtils.getFeatureNames(dictionary);
    assertEquals(Arrays.asList("foo", "bar", "baz", "bing"), featureNames);
  }

  @Test
  public void testListFeaturesMS() {
    MiningSchema miningSchema = AppPMMLUtils.buildMiningSchema(buildTestSchema());
    List<String> featureNames = AppPMMLUtils.getFeatureNames(miningSchema);
    assertEquals(Arrays.asList("foo", "bar", "baz", "bing"), featureNames);
  }

  private static void checkDataField(DataField field, String name, Boolean categorical) {
    assertEquals(name, field.getName().getValue());
    if (categorical == null) {
      assertNull(field.getOptype());
      assertNull(field.getDataType());
    } else if (categorical) {
      assertEquals(OpType.CATEGORICAL, field.getOptype());
      assertEquals(DataType.STRING, field.getDataType());
    } else {
      assertEquals(OpType.CONTINUOUS, field.getOptype());
      assertEquals(DataType.DOUBLE, field.getDataType());
    }
  }

  @Test
  public void testBuildCategoricalEncoding() {
    DataDictionary dictionary = new DataDictionary();
    DataField fooField =
        new DataField(FieldName.create("foo"), OpType.CONTINUOUS, DataType.DOUBLE);
    dictionary.getDataFields().add(fooField);
    DataField barField =
        new DataField(FieldName.create("bar"), OpType.CATEGORICAL, DataType.STRING);
    barField.getValues().add(new Value("b"));
    barField.getValues().add(new Value("a"));
    dictionary.getDataFields().add(barField);
    CategoricalValueEncodings encodings = AppPMMLUtils.buildCategoricalValueEncodings(dictionary);
    assertEquals(2, encodings.getValueCount(1));
    assertEquals(0, encodings.getValueEncodingMap(1).get("b").intValue());
    assertEquals(1, encodings.getValueEncodingMap(1).get("a").intValue());
    assertEquals("b", encodings.getEncodingValueMap(1).get(0));
    assertEquals("a", encodings.getEncodingValueMap(1).get(1));
    assertEquals(Collections.singletonMap(1, 2), encodings.getCategoryCounts());
  }

}
