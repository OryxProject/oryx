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

package com.cloudera.oryx.lambda.serving;

import javax.servlet.ServletContextEvent;

import java.util.HashMap;
import java.util.Map;

import com.typesafe.config.Config;
import org.junit.Test;
import org.springframework.mock.web.MockServletContext;

import com.cloudera.oryx.api.serving.ServingModelManager;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.lambda.AbstractLambdaIT;

public final class ModelManagerListenerIT extends AbstractLambdaIT {

  @Test
  public void testListener() throws Exception {
    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.serving.model-manager-class", MockServingModelManager.class.getName());
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());
    String serializedConfig = ConfigUtils.serialize(config);

    MockServletContext mockContext = new MockServletContext();
    mockContext.addInitParameter(ConfigUtils.class.getName() + ".serialized", serializedConfig);

    startMessaging();

    try (ModelManagerListener<?,?,?> listener = new ModelManagerListener<>()) {
      listener.init(mockContext);
      try {
        listener.contextInitialized(new ServletContextEvent(mockContext));
        ServingModelManager<?> manager =
            (ServingModelManager<?>) mockContext.getAttribute(ModelManagerListener.MANAGER_KEY);
        assertNotNull(manager);
        assertFalse(manager.isReadOnly());
        assertNotNull(manager.getConfig());
      } finally {
        listener.contextDestroyed(new ServletContextEvent(mockContext));
      }
    }
  }

}
