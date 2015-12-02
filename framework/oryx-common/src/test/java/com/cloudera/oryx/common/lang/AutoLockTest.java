/*
 * Copyright (c) 2015, Cloudera and Intel, Inc. All Rights Reserved.
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

package com.cloudera.oryx.common.lang;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class AutoLockTest extends OryxTest {

  @Test
  public void testDefault() throws Exception {
    AutoLock al = new AutoLock();
    assertNotNull(al.autoLock());
    assertNotNull(al.toString());
    assertNotNull(al.newCondition());
    al.lockInterruptibly();
    al.unlock();
    assertTrue(al.tryLock());
    al.unlock();
    assertTrue(al.tryLock(1, TimeUnit.SECONDS));
    al.unlock();
  }

  @Test
  public void testClose() {
    ReentrantLock lock = new ReentrantLock();
    assertFalse(lock.isHeldByCurrentThread());
    AutoLock al = new AutoLock(lock);
    assertFalse(lock.isHeldByCurrentThread());
    al.autoLock();
    assertTrue(lock.isHeldByCurrentThread());
    al.close();
    assertFalse(lock.isHeldByCurrentThread());
  }

  @Test
  public void testAutoClose() {
    ReentrantLock lock = new ReentrantLock();
    assertFalse(lock.isHeldByCurrentThread());
    AutoLock al = new AutoLock(lock);
    try (AutoLock al2 = al.autoLock()) {
      assertTrue(lock.isHeldByCurrentThread());
    }
    assertFalse(lock.isHeldByCurrentThread());
  }

}
