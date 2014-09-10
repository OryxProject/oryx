/*
 * Copyright (c) 2014, Cloudera, Inc. and Intel Corp. All Rights Reserved.
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

package com.cloudera.oryx.ml.serving.als;

import java.util.List;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.core.MediaType;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.oryx.ml.serving.IDValue;

public class SimilarityTest extends AbstractALSServingTest {

  @Test(expected = BadRequestException.class)
  public void testNoArg() {
    target("/similarity").request().get(String.class);
  }

  @Test
  public void testSimilarItems() {
    List<IDValue> recs = target("similarity/I0/I4/I6").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(LIST_ID_VALUE_TYPE);
    Assert.assertEquals(9, recs.size());
    for (int i = 1; i < recs.size(); i++) {
      Assert.assertTrue(recs.get(i).getValue() <= recs.get(i-1).getValue());
    }
    Assert.assertEquals("I1", recs.get(1).getID());
    Assert.assertEquals(0.5571406877613947, recs.get(2).getValue(), DOUBLE_EPSILON);
  }

  @Test
  public void testHowMany() {
    testHowMany("/similarity/I0/I2/I4", 10, 9);
    testHowMany("/similarity/I0/I2/I4", 9, 9);
    testHowMany("/similarity/I0/I2/I4", 5, 5);
  }

  @Test
  public void testOffset() {
    testOffset("/similarity/I0/I2/I4", 2, 1, 2);
    testOffset("/similarity/I0/I2/I4", 3, 1, 3);
    testOffset("/similarity/I0/I2/I4", 1, 1, 1);
    testOffset("/similarity/I0/I2/I4", 3, 3, 3);
  }
}
