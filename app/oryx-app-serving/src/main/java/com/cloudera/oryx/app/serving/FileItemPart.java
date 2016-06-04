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

package com.cloudera.oryx.app.serving;

import javax.servlet.http.Part;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

import com.google.common.collect.Lists;
import org.apache.commons.fileupload.FileItem;

/**
 * Adapter from {@link FileItem} to {@link Part}, for use in tests while still necessary
 * to work around Grizzly's lack of support for Servlet 3 file uploads.
 */
final class FileItemPart implements Part {

  private final FileItem item;

  FileItemPart(FileItem item) {
    this.item = item;
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return item.getInputStream();
  }

  @Override
  public String getContentType() {
    return item.getContentType();
  }

  @Override
  public String getName() {
    return item.getFieldName();
  }

  @Override
  public String getSubmittedFileName() {
    return item.getName();
  }

  @Override
  public long getSize() {
    return item.getSize();
  }

  @Override
  public void write(String fileName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void delete() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getHeader(String name) {
    return item.getHeaders().getHeader(name);
  }

  @Override
  public Collection<String> getHeaders(String name) {
    return Lists.newArrayList(item.getHeaders().getHeaders(name));
  }

  @Override
  public Collection<String> getHeaderNames() {
    return Lists.newArrayList(item.getHeaders().getHeaderNames());
  }

}
