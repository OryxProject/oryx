package com.cloudera.oryx.app.traffic.als;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;

import java.util.regex.Pattern;

import com.cloudera.oryx.app.traffic.Endpoint;

public abstract class ALSEndpoint extends Endpoint {

  private static final Pattern COMMA = Pattern.compile(",");

  private ALSEndpoint(String path, double relativeProb) {
    super(path, relativeProb);
  }

  @Override
  protected final Invocation makeInvocation(WebTarget target, String input) {
    String[] tokens = COMMA.split(input);
    String user = tokens[0];
    String item = tokens[1];
    String strength = tokens[2];
    return makeInvocation(target, user, item, strength);
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
