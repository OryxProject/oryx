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

package com.cloudera.oryx.lambda.serving;

import java.security.Principal;

import org.apache.catalina.core.StandardContext;
import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class InMemoryRealmTest extends OryxTest {

  @Test
  public void testAuthenticate() throws Exception {
    InMemoryRealm realm = new InMemoryRealm();
    StandardContext ctx = new StandardContext();
    ctx.setName("OryxTest");
    realm.setContainer(ctx);
    realm.start();

    realm.addUser("foo", "bar");
    Principal authPrincipal = realm.authenticate("foo", "bar");
    assertNotNull(authPrincipal);
    assertEquals("foo", authPrincipal.getName());
    assertNull(realm.authenticate("foo", "baz"));
    assertNull(realm.authenticate("bar", "foo"));
    assertEquals("bar", realm.getPassword("foo"));
    assertEquals("foo", realm.getPrincipal("foo").getName());
  }

}
