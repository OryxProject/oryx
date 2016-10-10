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

package com.cloudera.oryx.app.traffic.als;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;

import org.apache.commons.math3.random.RandomGenerator;

import com.cloudera.oryx.app.traffic.Endpoint;

/**
 * ALS-specific endpoint for traffic generation.
 */
public abstract class ALSEndpoint extends Endpoint {

  ALSEndpoint(String path, double relativeProb) {
    super(path, relativeProb);
  }

  @Override
  protected final Invocation makeInvocation(WebTarget target, String[] otherArgs, RandomGenerator random) {
    String user = Integer.toString(random.nextInt(Integer.parseInt(otherArgs[0])));
    String item = Integer.toString(random.nextInt(Integer.parseInt(otherArgs[0])));
    return makeInvocation(target, user, item, "1");
  }

  abstract Invocation makeInvocation(WebTarget target, String user, String item, String strength);

  public static Endpoint[] buildALSEndpoints() {
    return new Endpoint[] {
      new ALSEndpoint("/pref", 0.5) {
        @Override
        Invocation makeInvocation(WebTarget target, String user, String item, String strength) {
          return target.path("/pref/" + user + "/" + item).request()
              .buildPost(Entity.text(strength));
        }
      },
      new ALSEndpoint("/recommend", 0.3) {
        @Override
        Invocation makeInvocation(WebTarget target, String user, String item, String strength) {
          return target.path("/recommend/" + user).request().buildGet();
        }
      },
      new ALSEndpoint("/similarity", 0.2) {
        @Override
        Invocation makeInvocation(WebTarget target, String user, String item, String strength) {
          return target.path("/similarity/" + item).request().buildGet();
        }
      },
    };
  }


}
