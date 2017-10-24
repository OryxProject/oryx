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

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * I/O-related utility methods.
 */
public final class IOUtils {

  private static final Logger log = LoggerFactory.getLogger(IOUtils.class);

  private IOUtils() {}

  /**
   * Deletes the given path, and if it is a directory, all files and subdirectories within it.
   *
   * @param rootDir directory to delete
   * @throws IOException if any error occurs while deleting files or directories
   */
  public static void deleteRecursively(Path rootDir) throws IOException {
    if (rootDir == null || !Files.exists(rootDir)) {
      return;
    }
    Files.walkFileTree(rootDir, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }
      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  /**
   * @param dir directory to list
   * @param glob glob pattern for files that should be returned, which can span subdirectories
   *  as in patterns like {@code * /*}
   * @return {@link Path}s, including both files and directories, matching the glob pattern.
   *  No path containing an element whose name starts with "." is returned.
   *  Returned paths are also ordered lexicographically.
   * @throws IOException if an error occurs while accessing the file system
   */
  public static List<Path> listFiles(Path dir, String glob) throws IOException {
    Preconditions.checkArgument(Files.isDirectory(dir), "%s is not a directory", dir);

    List<String> globLevels;
    if (glob == null || glob.isEmpty()) {
      globLevels = Collections.singletonList("*");
    } else {
      globLevels = Arrays.asList(glob.split("/"));
    }
    Preconditions.checkState(!globLevels.isEmpty());

    List<Path> paths = new ArrayList<>();
    paths.add(dir);

    for (String globLevel : globLevels) {
      List<Path> newPaths = new ArrayList<>();
      for (Path existingPath : paths) {
        if (Files.isDirectory(existingPath)) {
          try (DirectoryStream<Path> stream = Files.newDirectoryStream(existingPath, globLevel)) {
            for (Path path : stream) {
              if (!path.getFileName().toString().startsWith(".")) {
                newPaths.add(path);
              }
            }
          }
        }
      }
      paths = newPaths;
    }
    Collections.sort(paths);
    return paths;
  }

  /**
   * Closes a {@link Closeable} and logs the exception, if any. This is only suitable for closing
   * objects where a failure to close does not impact correctness -- for example, an input stream
   * that is already fully read, but not an output stream which may fail when flushing final
   * output.
   *
   * @param closeable thing to close
   */
  public static void closeQuietly(Closeable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (IOException e) {
        log.warn("Unable to close", e);
      }
    }
  }

  /**
   * Binds to a free ephemeral port, and then releases it. The returned port is quite likely
   * to be free for use after this, but is not entirely guaranteed to be.
   *
   * @return a (probably) free ephemeral port
   * @throws IOException if an error occurs while binding to a port
   */
  public static int chooseFreePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0, 0)) {
      return socket.getLocalPort();
    }
  }

}
