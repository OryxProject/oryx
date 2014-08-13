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

package com.cloudera.oryx.lambda.serving;

import com.cloudera.oryx.common.settings.ConfigUtils;
import com.typesafe.config.ConfigException;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ApplicationPath;

@ApplicationPath("")
public class OryxApplication extends ResourceConfig {
  private static final String APPLICATION_RESOURCES = "serving.application-resources";
  private static final Logger log = LoggerFactory.getLogger(ServingLayer.class);

  public OryxApplication() {
    try {
      packages(ConfigUtils.getDefault().getString(APPLICATION_RESOURCES));
    } catch(ConfigException ex) {
      log.error("no valid "+APPLICATION_RESOURCES+" set.", ex);
    }
  }
}
