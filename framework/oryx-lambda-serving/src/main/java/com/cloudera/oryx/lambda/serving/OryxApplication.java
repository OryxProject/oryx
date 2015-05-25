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

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import javax.servlet.ServletContext;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;

import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.lang.ClassUtils;

/**
 * The single JAX-RS app for the serving-layer.
 */
@ApplicationPath("")
public final class OryxApplication extends Application {

  private static final Logger log = LoggerFactory.getLogger(OryxApplication.class);

  @Context
  private ServletContext servletContext;
  private Set<Class<?>> classes;

  /**
   * @return user endpoint implementations from the package named in context init param
   *  {@code com.cloudera.oryx.lambda.serving.OryxApplication.packages}.
   */
  @Override
  public Set<Class<?>> getClasses() {
    if (classes == null) {
      classes = doGetClasses();
    }
    return classes;
  }

  private Set<Class<?>> doGetClasses() {
    String packages =
        servletContext.getInitParameter(OryxApplication.class.getName() + ".packages");
    log.info("Creating JAX-RS from endpoints in package(s) {}", packages);
    Objects.requireNonNull(packages);
    Set<Class<?>> classes = new HashSet<>();
    for (String thePackage : packages.split(",")) {
      Reflections reflections = new Reflections(thePackage);
      classes.addAll(getClassesInPackageAnnotatedBy(thePackage, reflections, Path.class));
      classes.addAll(getClassesInPackageAnnotatedBy(thePackage, reflections, Produces.class));
      classes.addAll(getClassesInPackageAnnotatedBy(thePackage, reflections, Provider.class));
    }
    // Want to configure these globally, but not depend on Jersey, even though it's
    // what will be used in practice by the provided apps.
    for (String optionalJerseyClass : new String[] {
          "org.glassfish.jersey.message.DeflateEncoder",
          "org.glassfish.jersey.message.GZipEncoder",
          "org.glassfish.jersey.server.filter.EncodingFilter"}) {
      if (ClassUtils.classExists(optionalJerseyClass)) {
        classes.add(ClassUtils.loadClass(optionalJerseyClass));
      }
    }
    log.debug("Found JAX-RS resources: {}", classes);
    return classes;
  }

  private static Collection<Class<?>> getClassesInPackageAnnotatedBy(
      String thePackage,
      Reflections reflections,
      Class<? extends Annotation> annotation) {
    Set<Class<?>> classes = reflections.getTypesAnnotatedWith(annotation);
    // Filter classes actually in subpackages
    for (Iterator<Class<?>> it = classes.iterator(); it.hasNext();) {
      String classPackage = it.next().getPackage().getName();
      if (!classPackage.equals(thePackage)) {
        it.remove();
      }
    }
    return classes;
  }

}
