/*
 * Copyright (c) 2014, Cloudera and Intel, Inc. All Rights Reserved.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.collect.BiMap;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DataType;
import org.dmg.pmml.Extension;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.FieldUsageType;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.MiningSchema;
import org.dmg.pmml.OpType;
import org.dmg.pmml.PMML;
import org.dmg.pmml.Value;

import com.cloudera.oryx.app.schema.InputSchema;

public final class AppPMMLUtils {

  private AppPMMLUtils() {}

  public static String getExtensionValue(PMML pmml, String name) {
    for (Extension extension : pmml.getExtensions()) {
      if (name.equals(extension.getName())) {
        return extension.getValue();
      }
    }
    return null;
  }

  public static List<Object> getExtensionContent(PMML pmml, String name) {
    for (Extension extension : pmml.getExtensions()) {
      if (name.equals(extension.getName())) {
        return extension.getContent();
      }
    }
    return null;
  }

  public static void addExtension(PMML pmml, String key, Object value) {
    Extension extension = new Extension();
    extension.setName(key);
    extension.setValue(value.toString());
    pmml.getExtensions().add(extension);
  }

  public static void addExtensionContent(PMML pmml, String key, Collection<?> content) {
    if (content.isEmpty()) {
      return;
    }
    Collection<String> stringContent = new ArrayList<>(content.size());
    for (Object o : content) {
      stringContent.add(o.toString());
    }
    Extension extension = new Extension();
    extension.setName(key);
    extension.getContent().addAll(stringContent);
    pmml.getExtensions().add(extension);
  }

  /**
   * @param pmmlArrayContent the content of a node that serializes an array PMML-style
   * @return array values in order
   */
  public static List<String> parseArray(List<?> pmmlArrayContent) {
    String spaceSeparated = pmmlArrayContent.get(0).toString();
    if (spaceSeparated.isEmpty()) {
      return Collections.emptyList();
    }
    return Arrays.asList(spaceSeparated.split(" "));
  }

  /**
   * @param schema {@link InputSchema} whose information should be encoded in PMML
   * @return a {@link MiningSchema} representing the information contained in an
   *  {@link InputSchema}
   */
  public static MiningSchema buildMiningSchema(InputSchema schema) {
    List<String> featureNames = schema.getFeatureNames();
    Collection<MiningField> miningFields = new ArrayList<>();
    for (String featureName : featureNames) {
      MiningField field = new MiningField(new FieldName(featureName));
      if (schema.isNumeric(featureName)) {
        field.setOptype(OpType.CONTINUOUS);
        field.setUsageType(FieldUsageType.ACTIVE);
      } else if (schema.isCategorical(featureName)) {
        field.setOptype(OpType.CATEGORICAL);
        field.setUsageType(FieldUsageType.ACTIVE);
      } else {
        // ID, or ignored
        field.setUsageType(FieldUsageType.SUPPLEMENTARY);
      }
      if (schema.hasTarget() && schema.isTarget(featureName)) {
        field.setUsageType(FieldUsageType.PREDICTED);
      }
      miningFields.add(field);
    }
    return new MiningSchema().withMiningFields(miningFields);
  }

  public static DataDictionary buildDataDictionary(
      InputSchema schema,
      Map<Integer,BiMap<String,Double>> distinctValueMaps) {
    List<String> featureNames = schema.getFeatureNames();
    DataDictionary dictionary = new DataDictionary();

    for (int featureIndex = 0; featureIndex < featureNames.size(); featureIndex++) {
      String featureName = featureNames.get(featureIndex);
      if (schema.isActive(featureName)) {
        OpType opType;
        DataType dataType;
        if (schema.isNumeric(featureName)) {
          opType = OpType.CONTINUOUS;
          dataType = DataType.DOUBLE;
        } else if (schema.isCategorical(featureName)) {
          opType = OpType.CATEGORICAL;
          dataType = DataType.STRING;
        } else {
          throw new IllegalStateException("No type for feature " + featureName);
        }
        DataField field = new DataField(new FieldName(featureName), opType, dataType);
        if (schema.isCategorical(featureName)) {
          Collection<String> valuesOrderedByEncoding =
              new TreeMap<>(distinctValueMaps.get(featureIndex).inverse()).values();
          for (String value : valuesOrderedByEncoding) {
            field.getValues().add(new Value(value));
          }
        }
        dictionary.getDataFields().add(field);
      }
    }
    dictionary.setNumberOfFields(dictionary.getDataFields().size());
    return dictionary;
  }

}
