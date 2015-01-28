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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.base.Preconditions;
import org.dmg.pmml.Array;
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
import org.dmg.pmml.TypeDefinitionField;
import org.dmg.pmml.Value;

import com.cloudera.oryx.app.schema.CategoricalValueEncodings;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.text.TextUtils;

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
    return Arrays.asList(TextUtils.parseDelimited(spaceSeparated, ' '));
  }

  /**
   * @param values {@code double} value to make into a PMML {@link Array}
   * @return PMML {@link Array} representation
   */
  public static Array toArray(double[] values) {
    List<Double> valueList = new ArrayList<>(values.length);
    for (double value : values) {
      valueList.add(value);
    }
    String arrayValue = TextUtils.joinDelimited(valueList, ' ', true);
    return new Array(arrayValue, Array.Type.REAL).withN(valueList.size());
  }

  /**
   * @param values values to make into a PMML {@link Array}
   * @param type type of {@link Array} elements
   * @return PMML {@link Array} representation
   */
  public static Array toArray(Collection<?> values, Array.Type type) {
    String arrayValue = TextUtils.joinDelimited(values, ' ');
    return new Array(arrayValue, type).withN(values.size());
  }

  /**
   * @param schema {@link InputSchema} whose information should be encoded in PMML
   * @return a {@link MiningSchema} representing the information contained in an
   *  {@link InputSchema}
   */
  public static MiningSchema buildMiningSchema(InputSchema schema) {
    return buildMiningSchema(schema, null);
  }

  /**
   * @param schema {@link InputSchema} whose information should be encoded in PMML
   * @param importances optional feature importances. May be {@code null}, or else the size
   *  of the array must match the number of predictors in the schema, which may be
   *  less than the total number of features.
   * @return a {@link MiningSchema} representing the information contained in an
   *  {@link InputSchema}
   */
  public static MiningSchema buildMiningSchema(InputSchema schema, double[] importances) {
    Preconditions.checkArgument(
        importances == null || (importances.length == schema.getNumPredictors()));
    List<String> featureNames = schema.getFeatureNames();
    Collection<MiningField> miningFields = new ArrayList<>();
    for (int featureIndex = 0; featureIndex < featureNames.size(); featureIndex++) {
      String featureName = featureNames.get(featureIndex);
      MiningField field = new MiningField(FieldName.create(featureName));
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
        // Override to PREDICTED
        field.setUsageType(FieldUsageType.PREDICTED);
      }
      // Will be active if and only if it's a predictor
      if (field.getUsageType() == FieldUsageType.ACTIVE && importances != null) {
        int predictorIndex = schema.featureToPredictorIndex(featureIndex);
        field.setImportance(importances[predictorIndex]);
      }
      miningFields.add(field);
    }
    return new MiningSchema().withMiningFields(miningFields);
  }

  /**
   * @param miningSchema {@link MiningSchema} from a model
   * @return names of features in order
   */
  public static List<String> getFeatureNames(MiningSchema miningSchema) {
    List<String> names = new ArrayList<>();
    for (MiningField field : miningSchema.getMiningFields()) {
      names.add(field.getName().getValue());
    }
    return names;
  }

  /**
   * @param miningSchema {@link MiningSchema} from a model
   * @return index of the {@link FieldUsageType#PREDICTED} feature
   */
  public static Integer findTargetIndex(MiningSchema miningSchema) {
    List<MiningField> miningFields = miningSchema.getMiningFields();
    for (int i = 0; i < miningFields.size(); i++) {
      if (miningFields.get(i).getUsageType() == FieldUsageType.PREDICTED) {
        return i;
      }
    }
    return null;
  }

  public static DataDictionary buildDataDictionary(
      InputSchema schema,
      CategoricalValueEncodings categoricalValueEncodings) {
    List<String> featureNames = schema.getFeatureNames();
    DataDictionary dictionary = new DataDictionary();

    for (int featureIndex = 0; featureIndex < featureNames.size(); featureIndex++) {
      String featureName = featureNames.get(featureIndex);
      OpType opType;
      DataType dataType;
      if (schema.isNumeric(featureName)) {
        opType = OpType.CONTINUOUS;
        dataType = DataType.DOUBLE;
      } else if (schema.isCategorical(featureName)) {
        opType = OpType.CATEGORICAL;
        dataType = DataType.STRING;
      } else {
        // Don't know
        opType = null;
        dataType = null;
      }
      DataField field = new DataField(FieldName.create(featureName), opType, dataType);
      if (schema.isCategorical(featureName)) {
        Collection<String> valuesOrderedByEncoding =
            new TreeMap<>(categoricalValueEncodings.getEncodingValueMap(featureIndex)).values();
        for (String value : valuesOrderedByEncoding) {
          field.getValues().add(new Value(value));
        }
      }
      dictionary.getDataFields().add(field);
    }
    dictionary.setNumberOfFields(dictionary.getDataFields().size());
    return dictionary;
  }


  /**
   * @param dictionary {@link DataDictionary} from model
   * @return names of features in order
   */
  public static List<String> getFeatureNames(DataDictionary dictionary) {
    List<DataField> dataFields = dictionary.getDataFields();
    Preconditions.checkArgument(dataFields != null && !dataFields.isEmpty(),
                                "No fields in DataDictionary");
    List<String> names = new ArrayList<>(dataFields.size());
    for (TypeDefinitionField field : dataFields) {
      names.add(field.getName().getValue());
    }
    return names;
  }

  public static CategoricalValueEncodings buildCategoricalValueEncodings(
      DataDictionary dictionary) {
    Map<Integer,Collection<String>> indexToValues = new HashMap<>();
    List<DataField> dataFields = dictionary.getDataFields();
    for (int featureIndex = 0; featureIndex < dataFields.size(); featureIndex++) {
      TypeDefinitionField field = dataFields.get(featureIndex);
      Collection<Value> values = field.getValues();
      if (values != null && !values.isEmpty()) {
        Collection<String> categoricalValues = new ArrayList<>();
        for (Value value : values) {
          categoricalValues.add(value.getValue());
        }
        indexToValues.put(featureIndex, categoricalValues);
      }
    }
    return new CategoricalValueEncodings(indexToValues);
  }

}
