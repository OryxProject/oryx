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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.io.CharStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.dmg.pmml.Array;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DataType;
import org.dmg.pmml.Extension;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.MiningSchema;
import org.dmg.pmml.OpType;
import org.dmg.pmml.PMML;
import org.dmg.pmml.TypeDefinitionField;
import org.dmg.pmml.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.app.schema.CategoricalValueEncodings;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.common.text.TextUtils;

/**
 * General app tier PMML-related utility methods.
 */
public final class AppPMMLUtils {

  private static final Logger log = LoggerFactory.getLogger(AppPMMLUtils.class);

  private AppPMMLUtils() {}

  public static String getExtensionValue(PMML pmml, String name) {
    return pmml.getExtensions().stream().filter(extension -> name.equals(extension.getName())).findFirst().
        map(Extension::getValue).orElse(null);
  }

  /**
   * @param pmml PMML model to query for extensions
   * @param name name of extension to query
   * @return content of the extension, parsed as if it were a PMML {@link Array}:
   *  space-separated values, with PMML quoting rules
   */
  public static List<String> getExtensionContent(PMML pmml, String name) {
    return pmml.getExtensions().stream().filter(extension -> name.equals(extension.getName())).findFirst().
        map(extension -> {
          List<?> content = extension.getContent();
          Preconditions.checkArgument(content.size() <= 1);
          return content.isEmpty() ?
              Collections.<String>emptyList() :
              Arrays.asList(TextUtils.parsePMMLDelimited(content.get(0).toString()));
        }).orElse(null);
  }

  /**
   * @param pmml PMML model to add extension to, with no content. It may possibly duplicate
   *  existing extensions.
   * @param key extension key
   * @param value extension value
   */
  public static void addExtension(PMML pmml, String key, Object value) {
    pmml.addExtensions(new Extension().setName(key).setValue(value.toString()));
  }

  /**
   * @param pmml PMML model to add extension to, with a single {@code String} content and no value.
   *  The content is encoded as if they were being added to a PMML {@link Array} and are
   *  space-separated with PMML quoting rules
   * @param key extension key
   * @param content list of values to add as a {@code String}
   */
  public static void addExtensionContent(PMML pmml, String key, Collection<?> content) {
    if (content.isEmpty()) {
      return;
    }
    String joined = TextUtils.joinPMMLDelimited(content);
    pmml.addExtensions(new Extension().setName(key).addContent(joined));
  }

  /**
   * @param values {@code double} value to make into a PMML {@link Array}
   * @return PMML {@link Array} representation
   */
  public static Array toArray(double... values) {
    List<Double> valueList = new ArrayList<>(values.length);
    for (double value : values) {
      valueList.add(value);
    }
    String arrayValue = TextUtils.joinPMMLDelimitedNumbers(valueList);
    return new Array(Array.Type.REAL, arrayValue).setN(valueList.size());
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
    List<MiningField> miningFields = new ArrayList<>();
    for (int featureIndex = 0; featureIndex < featureNames.size(); featureIndex++) {
      String featureName = featureNames.get(featureIndex);
      MiningField field = new MiningField(FieldName.create(featureName));
      if (schema.isNumeric(featureName)) {
        field.setOpType(OpType.CONTINUOUS);
        field.setUsageType(MiningField.UsageType.ACTIVE);
      } else if (schema.isCategorical(featureName)) {
        field.setOpType(OpType.CATEGORICAL);
        field.setUsageType(MiningField.UsageType.ACTIVE);
      } else {
        // ID, or ignored
        field.setUsageType(MiningField.UsageType.SUPPLEMENTARY);
      }
      if (schema.hasTarget() && schema.isTarget(featureName)) {
        // Override to PREDICTED
        field.setUsageType(MiningField.UsageType.PREDICTED);
      }
      // Will be active if and only if it's a predictor
      if (field.getUsageType() == MiningField.UsageType.ACTIVE && importances != null) {
        int predictorIndex = schema.featureToPredictorIndex(featureIndex);
        field.setImportance(importances[predictorIndex]);
      }
      miningFields.add(field);
    }
    return new MiningSchema(miningFields);
  }

  /**
   * @param miningSchema {@link MiningSchema} from a model
   * @return names of features in order
   */
  public static List<String> getFeatureNames(MiningSchema miningSchema) {
    return miningSchema.getMiningFields().stream().map(field -> field.getName().getValue())
        .collect(Collectors.toList());
  }

  /**
   * @param miningSchema {@link MiningSchema} from a model
   * @return index of the {@link MiningField.UsageType#PREDICTED} feature
   */
  public static Integer findTargetIndex(MiningSchema miningSchema) {
    List<MiningField> miningFields = miningSchema.getMiningFields();
    for (int i = 0; i < miningFields.size(); i++) {
      if (miningFields.get(i).getUsageType() == MiningField.UsageType.PREDICTED) {
        return i;
      }
    }
    return null;
  }

  public static DataDictionary buildDataDictionary(
      InputSchema schema,
      CategoricalValueEncodings categoricalValueEncodings) {
    List<String> featureNames = schema.getFeatureNames();

    List<DataField> dataFields = new ArrayList<>();
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
        categoricalValueEncodings.getEncodingValueMap(featureIndex).entrySet().stream().
            sorted(Comparator.comparing(Map.Entry::getKey)).
            map(Map.Entry::getValue).
            forEach(value -> field.addValues(new Value(value)));
      }
      dataFields.add(field);
    }

    return new DataDictionary(dataFields).setNumberOfFields(dataFields.size());
  }


  /**
   * @param dictionary {@link DataDictionary} from model
   * @return names of features in order
   */
  public static List<String> getFeatureNames(DataDictionary dictionary) {
    List<DataField> dataFields = dictionary.getDataFields();
    Preconditions.checkArgument(dataFields != null && !dataFields.isEmpty(),
                                "No fields in DataDictionary");
    return dataFields.stream().map(field -> field.getName().getValue()).collect(Collectors.toList());
  }

  public static CategoricalValueEncodings buildCategoricalValueEncodings(
      DataDictionary dictionary) {
    Map<Integer,Collection<String>> indexToValues = new HashMap<>();
    List<DataField> dataFields = dictionary.getDataFields();
    for (int featureIndex = 0; featureIndex < dataFields.size(); featureIndex++) {
      TypeDefinitionField field = dataFields.get(featureIndex);
      Collection<Value> values = field.getValues();
      if (values != null && !values.isEmpty()) {
        Collection<String> categoricalValues = values.stream().map(Value::getValue).collect(Collectors.toList());
        indexToValues.put(featureIndex, categoricalValues);
      }
    }
    return new CategoricalValueEncodings(indexToValues);
  }

  public static PMML readPMMLFromUpdateKeyMessage(String key,
                                                  String message,
                                                  Configuration hadoopConf) throws IOException {
    String pmmlString;
    switch (key) {
      case "MODEL":
        pmmlString = message;
        break;
      case "MODEL-REF":
        // Allowing null is mostly for integration tests
        if (hadoopConf == null) {
          hadoopConf = new Configuration();
        }
        Path messagePath = new Path(message);
        FileSystem fs = FileSystem.get(messagePath.toUri(), hadoopConf);
        try (InputStreamReader in = new InputStreamReader(fs.open(messagePath), StandardCharsets.UTF_8)) {
          pmmlString = CharStreams.toString(in);
        } catch (FileNotFoundException fnfe) {
          log.warn("Unable to load model file at {}; ignoring", messagePath);
          return null;
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown key " + key);
    }
    return PMMLUtils.fromString(pmmlString);
  }

}
