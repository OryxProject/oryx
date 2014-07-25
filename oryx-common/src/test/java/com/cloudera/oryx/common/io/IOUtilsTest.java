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

package com.cloudera.oryx.common.io;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

/**
 * Tests {@link IOUtils}.
 */
public final class IOUtilsTest extends OryxTest {

  private static final byte[] SOME_BYTES = { 0x01, 0x02, 0x03 };
  private static final byte[] NO_BYTES = {};

  private Path createTestDirs() throws IOException {
    // Creates structure:
    //   tempDir/
    //     subFile1
    //     .hidden
    //     subDir1/
    //       subFile2
    //       subDir2/
    Path tempDir = getTempDir();
    assertTrue(Files.exists(tempDir));
    Path subFile1 = tempDir.resolve("subFile1");
    Files.write(subFile1, SOME_BYTES);
    Path hidden = tempDir.resolve(".hidden");
    Files.write(hidden, NO_BYTES);
    assertTrue(Files.exists(subFile1));
    Path subDir1 = tempDir.resolve("subDir1");
    Files.createDirectory(subDir1);
    Path subFile2 = subDir1.resolve("subFile2");
    Files.write(subFile2, SOME_BYTES);
    assertTrue(Files.exists(subFile2));
    Path subDir2 = subDir1.resolve("subDir2");
    Files.createDirectory(subDir2);
    return tempDir;
  }

  @Test
  public void testDeleteRecursively() throws IOException {
    Path testDir = createTestDirs();
    IOUtils.deleteRecursively(testDir);
    assertFalse(Files.exists(testDir));
    assertFalse(Files.exists(testDir.resolve("subFile1")));
  }

  @Test
  public void testListFiles() throws IOException {
    Path testDir = createTestDirs();
    List<Path> files = IOUtils.listFiles(testDir, "*");
    assertEquals(2, files.size());
    assertTrue(files.contains(testDir.resolve("subFile1")));
    assertFalse(files.contains(testDir.resolve(".hidden")));
    assertTrue(files.contains(testDir.resolve("subDir1")));
  }

  @Test
  public void testListSubdirs() throws IOException {
    Path testDir = createTestDirs();
    List<Path> files = IOUtils.listFiles(testDir, "*/*");
    assertEquals(2, files.size());
    assertTrue(files.contains(testDir.resolve("subDir1").resolve("subFile2")));
    assertTrue(files.contains(testDir.resolve("subDir1").resolve("subDir2")));
  }

  @Test
  public void testListSubdirs2() throws IOException {
    Path testDir = createTestDirs();
    List<Path> files = IOUtils.listFiles(testDir, "*/subFile*");
    assertEquals(1, files.size());
    assertTrue(files.contains(testDir.resolve("subDir1").resolve("subFile2")));
  }

  @Test
  public void testOrder() throws IOException {
    Path testDir = createTestDirs();
    List<Path> files = IOUtils.listFiles(testDir, "*/*");
    assertEquals(testDir.resolve("subDir1").resolve("subDir2"), files.get(0));
    assertEquals(testDir.resolve("subDir1").resolve("subFile2"), files.get(1));
  }

  @Test
  public void testReadLines() throws IOException {
    Path tempDir = getTempDir();
    Path textFile = tempDir.resolve("file.txt");
    Files.write(textFile, Arrays.asList("foo", "bar", "baz"), StandardCharsets.UTF_8);
    Iterator<String> it = IOUtils.readLines(textFile).iterator();
    assertTrue(it.hasNext());
    assertEquals("foo", it.next());
    assertTrue(it.hasNext());
    assertEquals("bar", it.next());
    assertTrue(it.hasNext());
    assertEquals("baz", it.next());
    assertFalse(it.hasNext());
  }

}
