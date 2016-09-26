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

package com.cloudera.oryx.app.batch.mllib;

import java.util.List;
import java.util.Map;

import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DataType;
import org.dmg.pmml.Header;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.MiningSchema;
import org.dmg.pmml.OpType;
import org.dmg.pmml.PMML;

import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.lambda.batch.AbstractBatchIT;

/**
 * Superclass of app-tier MLlib-based batch layer integration tests.
 */
public abstract class AbstractAppMLlibIT extends AbstractBatchIT {

  protected static final int GEN_INTERVAL_SEC = 10;

  protected static void checkHeader(Header header) {
    assertNotNull(header);
    assertNotNull(header.getTimestamp());
    assertEquals("Oryx", header.getApplication().getName());
  }

  protected static void checkExtensions(PMML pmml, Map<String,?> expected) {
    expected.forEach((key, value) ->
      assertEquals("Value for key " + key + " did not match",
                   value.toString(), AppPMMLUtils.getExtensionValue(pmml, key))
    );
  }

  protected static void checkDataDictionary(InputSchema schema, DataDictionary dataDictionary) {
    assertNotNull(dataDictionary);
    assertEquals("Wrong number of features",
                 schema.getNumFeatures(),
                 dataDictionary.getNumberOfFields().intValue());
    List<DataField> dataFields = dataDictionary.getDataFields();
    assertEquals(schema.getNumFeatures(), dataFields.size());
    for (DataField dataField : dataFields) {
      String featureName = dataField.getName().getValue();
      if (schema.isNumeric(featureName)) {
        assertEquals("Wrong op type for feature " + featureName,
                     OpType.CONTINUOUS,
                     dataField.getOpType());
        assertEquals("Wrong data type for feature " + featureName,
                     DataType.DOUBLE,
                     dataField.getDataType());
      } else if (schema.isCategorical(featureName)) {
        assertEquals("Wrong op type for feature " + featureName,
                     OpType.CATEGORICAL,
                     dataField.getOpType());
        assertEquals("Wrong data type for feature " + featureName,
                     DataType.STRING,
                     dataField.getDataType());
      } else {
        assertNull(dataField.getOpType());
        assertNull(dataField.getDataType());
      }
    }
  }

  protected static void checkMiningSchema(InputSchema schema, MiningSchema miningSchema) {
    assertNotNull(miningSchema);
    List<MiningField> miningFields = miningSchema.getMiningFields();
    List<String> expectedFeatureNames = schema.getFeatureNames();
    assertEquals("Wrong number of features",
                 expectedFeatureNames.size(), miningFields.size());
    for (int i = 0; i < expectedFeatureNames.size(); i++) {
      MiningField miningField = miningFields.get(i);
      String expectedFeature = expectedFeatureNames.get(i);
      String featureName = miningField.getName().getValue();
      assertEquals("Wrong feature at position " + i, expectedFeature, featureName);
      if (schema.isNumeric(expectedFeature) || schema.isCategorical(expectedFeature)) {
        assertEquals("Wrong op type for feature + " + featureName,
                     schema.isNumeric(expectedFeature) ? OpType.CONTINUOUS : OpType.CATEGORICAL,
                     miningField.getOpType());
        if (schema.isTarget(expectedFeature)) {
          assertEquals("Wrong usage type for feature " + featureName,
                       MiningField.UsageType.PREDICTED,
                       miningField.getUsageType());
        } else {
          assertEquals("Wrong usage type for feature " + featureName,
                       MiningField.UsageType.ACTIVE,
                       miningField.getUsageType());
          assertRange(miningField.getImportance(), 0.0, 1.0);
        }
      } else {
        assertEquals("Wrong usage type for feature " + featureName,
                     MiningField.UsageType.SUPPLEMENTARY,
                     miningField.getUsageType());
      }

    }
  }

}
