
/*
* Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
*
* Cloudera, Inc. licenses this file to you under the Apache License,
* Version 2.0 (the "License"). You may not use this file except in
* compliance with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
* CONDITIONS OF ANY KIND, either express or implied. See the License for
* the specific language governing permissions and limitations under the
* License.
*/

package com.cloudera.oryx.ml.serving;

import javax.ws.rs.core.Response;

public final class OryxServingException extends Exception {

  private final Response.Status statusCode;

  public OryxServingException(Response.Status statusCode) {
    this(statusCode, null);
  }

  public OryxServingException(Response.Status statusCode, String message) {
    super(message);
    this.statusCode = statusCode;
  }

  public Response.Status getStatusCode() {
    return statusCode;
  }
}