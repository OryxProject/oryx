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

public final class BecauseTest extends AbstractALSServingTest {

  @Test
  public void testBecauseRecommended() {
    List<IDValue> recs = target("because").path("U0/I0").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(LIST_ID_VALUE_TYPE);
    Assert.assertEquals(3, recs.size());
    for (int i = 1; i < recs.size(); i++) {
      Assert.assertTrue(recs.get(i).getValue() <= recs.get(i-1).getValue());
    }
    Assert.assertEquals("I0", recs.get(0).getID());
    Assert.assertEquals(1.0, recs.get(0).getValue(), DOUBLE_EPSILON);

  }

  @Test(expected = BadRequestException.class)
  public void testNoArg() {
    target("/because").request().get(String.class);
  }

  @Test
  public void testHowMany() {
    testHowMany("/because/U5/I4", 10, 7);
    testHowMany("/because/U5/I4", 9, 7);
    testHowMany("/because/U5/I4", 5, 5);
  }

  @Test
  public void testOffset() {
    testOffset("/because/U6/I6", 2, 1, 2);
    testOffset("/because/U6/I6", 3, 1, 3);
    testOffset("/because/U6/I6", 1, 1, 1);
    testOffset("/because/U6/I6", 3, 3, 3);
  }

}
