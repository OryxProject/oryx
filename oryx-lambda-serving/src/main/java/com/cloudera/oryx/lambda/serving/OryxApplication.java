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

import com.google.common.base.Preconditions;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.servlet.ServletContext;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import java.util.Set;

/**
 * The single JAX-RS app for the serving-layer.
 */
@ApplicationPath("")
public final class OryxApplication extends Application {

  private static final Logger log = LoggerFactory.getLogger(OryxApplication.class);

  @Context
  private ServletContext servletContext;
  private String packages;

  @PostConstruct
  public void init() {
    packages = servletContext.getInitParameter(OryxApplication.class.getName() + ".packages");
    Preconditions.checkNotNull(packages);
  }

  /**
   * @return  user endpoint implementations from the package named in init param
   *  {@code com.cloudera.oryx.lambda.serving.OryxApplication.packages}.
   */
  @Override
  public Set<Class<?>> getClasses() {
    log.info("Creating JAX-RS from endpoints in package(s) {}", packages);
    Reflections reflections = new Reflections(packages);
    Set<Class<?>> classes = reflections.getTypesAnnotatedWith(Path.class);
    log.info("Found JAX-RS resources: {}", classes);
    return classes;
  }

}
