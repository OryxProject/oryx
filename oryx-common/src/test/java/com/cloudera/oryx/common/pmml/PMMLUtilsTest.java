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

package com.cloudera.oryx.common.pmml;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.dmg.pmml.Extension;
import org.dmg.pmml.MiningFunctionType;
import org.dmg.pmml.Model;
import org.dmg.pmml.Node;
import org.dmg.pmml.PMML;
import org.dmg.pmml.TreeModel;
import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class PMMLUtilsTest extends OryxTest {

  public static PMML buildDummyModel() {
    Node node = new Node();
    node.setRecordCount(123.0);
    TreeModel treeModel = new TreeModel(null, node, MiningFunctionType.CLASSIFICATION);
    PMML pmml = PMMLUtils.buildSkeletonPMML();
    pmml.getModels().add(treeModel);
    return pmml;
  }

  @Test
  public void testSkeleton() {
    PMML pmml = PMMLUtils.buildSkeletonPMML();
    assertEquals("Oryx", pmml.getHeader().getApplication().getName());
    assertNotNull(pmml.getHeader().getTimestamp());
  }

  @Test
  public void testReadWrite() throws Exception {
    Path tempModelFile = Files.createTempFile(getTempDir(), "model", ".pmml.gz");
    PMML model = buildDummyModel();
    PMMLUtils.write(model, tempModelFile);
    assertTrue(Files.exists(tempModelFile));
    PMML model2 = PMMLUtils.read(tempModelFile);
    List<Model> models = model2.getModels();
    assertEquals(1, models.size());
    assertTrue(models.get(0) instanceof TreeModel);
    TreeModel treeModel = (TreeModel) models.get(0);
    assertEquals(123.0, treeModel.getNode().getRecordCount().doubleValue());
    assertEquals(MiningFunctionType.CLASSIFICATION, treeModel.getFunctionName());
  }

  @Test
  public void testExtensionValue() {
    PMML model = buildDummyModel();
    assertNull(PMMLUtils.getExtensionValue(model, "foo"));
    model.getExtensions().add(new Extension().withName("foo").withValue("bar"));
    assertEquals("bar", PMMLUtils.getExtensionValue(model, "foo"));
  }

  @Test
  public void testExtensionContent() {
    PMML model = buildDummyModel();
    assertNull(PMMLUtils.getExtensionContent(model, "foo"));
    Extension extension = new Extension().withName("foo");
    extension.getContent().addAll(Arrays.asList("bar", "baz"));
    model.getExtensions().add(extension);
    assertEquals(Arrays.<Object>asList("bar", "baz"), PMMLUtils.getExtensionContent(model, "foo"));
  }

}
