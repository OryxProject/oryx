/*
 * Copyright (c) 2014, Cloudera and Intel, Inc. All Rights Reserved.
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

/**
 * JSON Error for missing path params, incorrect query params, etc
 */
public final class ErrorResponse {

  private final String error;
  private final int statusCode;

  public ErrorResponse(int statusCode) {
    this(statusCode, null);
  }

  public ErrorResponse(int statusCode, String error) {
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

  @Override
  public String toString() {
    return statusCode + (error == null ? "" : ": " + error);
  }

}
