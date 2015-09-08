/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.oryx.app.serving.als;

import javax.servlet.ServletContextListener;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import com.typesafe.config.Config;
import org.junit.Assert;
import org.junit.Test;

import com.cloudera.oryx.common.settings.ConfigUtils;

public final class ReadOnlyTest extends AbstractALSServingTest {

  @Test
  public void testNoIngest() {
    Assert.assertEquals(Response.Status.FORBIDDEN.getStatusCode(),
                        target("/ingest").request().post(Entity.text(IngestTest.INGEST_DATA)).getStatus());
  }

  @Test
  public void testNoPreference() {
    Assert.assertEquals(Response.Status.FORBIDDEN.getStatusCode(),
                        target("/pref/U2/I2").request().post(Entity.text("aBc!")).getStatus());
  }

  @Override
  protected Class<? extends ServletContextListener> getInitListenerClass() {
    return ReadOnlyMockManagerInitListener.class;
  }

  public static final class ReadOnlyMockManagerInitListener extends MockManagerInitListener {
    @Override
    protected MockServingModelManager getModelManager() {
      return new ReadOnlyMockServingModelManager(ConfigUtils.getDefault());
    }
  }

  protected static final class ReadOnlyMockServingModelManager extends MockServingModelManager {
    public ReadOnlyMockServingModelManager(Config config) {
      super(config);
    }
    @Override
    public boolean isReadOnly() {
      return true;
    }
  }

}
