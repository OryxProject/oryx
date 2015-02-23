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

package com.cloudera.oryx.app.mllib.als;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import com.cloudera.oryx.app.mllib.AbstractAppMLlibIT;

public abstract class AbstractALSIT extends AbstractAppMLlibIT {

  static Collection<Integer> parseIDsFromContent(List<?> content) {
    Collection<Integer> result = new HashSet<>(content.size());
    for (Object s : content) {
      result.add(Integer.valueOf(s.toString()));
    }
    return result;
  }

}
