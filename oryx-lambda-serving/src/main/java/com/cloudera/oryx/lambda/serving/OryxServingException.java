
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

package com.cloudera.oryx.lambda.serving;

import com.google.common.base.Preconditions;

public final class OryxServingException extends Exception {

  private final String error;
  private final int statusCode;

  public OryxServingException(int statusCode) {
    this(statusCode, null);
  }

  public OryxServingException(int statusCode, String error) {
    super(statusCode + (error == null ? "" : ": " + error));
    Preconditions.checkArgument(statusCode >= 100 && statusCode < 600);
    this.statusCode = statusCode;
    this.error = error;
  }

  public String getError() {
    return error;
  }

  public int getStatusCode() {
    return statusCode;
  }
}