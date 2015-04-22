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

import java.util.regex.Pattern;

import com.cloudera.oryx.app.traffic.Endpoint;

/**
 * ALS-specific endpoint for traffic generation.
 */
public abstract class ALSEndpoint extends Endpoint {

  private static final Pattern COMMA = Pattern.compile(",");

  private ALSEndpoint(String path, double relativeProb) {
    super(path, relativeProb);
  }

  @Override
  protected final Invocation makeInvocation(WebTarget target, String input) {
    String[] tokens = COMMA.split(input);
    return makeInvocation(target, tokens[0], tokens[1], tokens[2]);
  }

  abstract Invocation makeInvocation(WebTarget target, String user, String item, String strength);

  public static Endpoint[] buildALSEndpoints() {
    return new Endpoint[] {
      new ALSEndpoint("/pref", 0.1) {
        @Override
        Invocation makeInvocation(WebTarget target, String user, String item, String strength) {
          return target.path("/pref/" + user + "/" + item).request()
              .buildPost(Entity.text(strength));
        }
      },
      new ALSEndpoint("/recommend", 0.9) {
        @Override
        Invocation makeInvocation(WebTarget target, String user, String item, String strength) {
          return target.path("/recommend/" + user).request().buildGet();
        }
      }
    };
  }


}
