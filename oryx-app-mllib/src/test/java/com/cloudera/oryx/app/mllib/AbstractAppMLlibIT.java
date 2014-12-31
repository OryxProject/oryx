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

package com.cloudera.oryx.app.mllib;

import java.util.List;
import java.util.Map;

import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DataType;
import org.dmg.pmml.FieldUsageType;
import org.dmg.pmml.Header;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.MiningSchema;
import org.dmg.pmml.OpType;
import org.dmg.pmml.PMML;

import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.lambda.AbstractBatchIT;

public abstract class AbstractAppMLlibIT extends AbstractBatchIT {

  protected static void checkHeader(Header header) {
    assertNotNull(header);
    assertNotNull(header.getTimestamp());
    assertEquals("Oryx", header.getApplication().getName());
  }

  protected static void checkExtensions(PMML pmml, Map<String,?> expected) {
    for (Map.Entry<String,?> e : expected.entrySet()) {
      assertEquals(e.getValue().toString(), AppPMMLUtils.getExtensionValue(pmml, e.getKey()));
    }
  }

  protected static void checkDataDictionary(InputSchema schema, DataDictionary dataDictionary) {
    assertNotNull(dataDictionary);
    assertEquals(schema.getNumFeatures(), dataDictionary.getNumberOfFields().intValue());
    List<DataField> dataFields = dataDictionary.getDataFields();
    assertEquals(schema.getNumFeatures(), dataFields.size());
    for (DataField dataField : dataFields) {
      String featureName = dataField.getName().getValue();
      if (schema.isNumeric(featureName)) {
        assertEquals(OpType.CONTINUOUS, dataField.getOptype());
        assertEquals(DataType.DOUBLE, dataField.getDataType());
      } else if (schema.isCategorical(featureName)) {
        assertEquals(OpType.CATEGORICAL, dataField.getOptype());
        assertEquals(DataType.STRING, dataField.getDataType());
      } else {
        assertNull(dataField.getOptype());
        assertNull(dataField.getDataType());
      }
    }
  }

  protected static void checkMiningSchema(InputSchema schema, MiningSchema miningSchema) {
    assertNotNull(miningSchema);
    List<MiningField> miningFields = miningSchema.getMiningFields();
    List<String> expectedFeatureNames = schema.getFeatureNames();
    assertEquals(expectedFeatureNames.size(), miningFields.size());
    for (int i = 0; i < expectedFeatureNames.size(); i++) {
      MiningField miningField = miningFields.get(i);
      String expectedFeature = expectedFeatureNames.get(i);
      assertEquals(expectedFeature, miningField.getName().getValue());
      if (schema.isNumeric(expectedFeature)) {
        assertEquals(OpType.CONTINUOUS, miningField.getOptype());
        if (schema.isTarget(expectedFeature)) {
          assertEquals(FieldUsageType.PREDICTED, miningField.getUsageType());
        } else {
          assertEquals(FieldUsageType.ACTIVE, miningField.getUsageType());
          assertTrue(miningField.getImportance() >= 0.0 && miningField.getImportance() <= 1.0);
        }
      } else if (schema.isCategorical(expectedFeature)) {
        assertEquals(OpType.CATEGORICAL, miningField.getOptype());
        if (schema.isTarget(expectedFeature)) {
          assertEquals(FieldUsageType.PREDICTED, miningField.getUsageType());
        } else {
          assertEquals(FieldUsageType.ACTIVE, miningField.getUsageType());
          assertTrue(miningField.getImportance() >= 0.0 && miningField.getImportance() <= 1.0);
        }
      } else {
        assertEquals(FieldUsageType.SUPPLEMENTARY, miningField.getUsageType());
      }

    }
  }

}
