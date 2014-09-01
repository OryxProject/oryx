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

package com.cloudera.oryx.common.pmml;

import javax.xml.bind.JAXBException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.dmg.pmml.Application;
import org.dmg.pmml.Extension;
import org.dmg.pmml.Header;
import org.dmg.pmml.PMML;
import org.dmg.pmml.Timestamp;
import org.jpmml.model.JAXBUtil;

import com.cloudera.oryx.common.io.IOUtils;

public final class PMMLUtils {

  private PMMLUtils() {
  }

  /**
   * @return {@link PMML} with basic common header fields like Application and Timestamp, and
   *  version filled out
   */
  public static PMML buildSkeletonPMML() {
    String formattedDate =
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZZ", Locale.ENGLISH).format(new Date());
    Header header = new Header();
    header.setTimestamp(new Timestamp().withContent(formattedDate));
    header.setApplication(new Application("Oryx"));
    return new PMML(header, null, "4.2.1");
  }

  /**
   * @param pmml {@link PMML} model to write
   * @param path file to write the model to
   * @throws IOException if an error occurs while writing the model to storage
   */
  public static void write(PMML pmml, Path path) throws IOException {
    try (OutputStream out = IOUtils.writeMaybeCompressed(path, 1 << 16)) {
      write(pmml, out);
    }
  }

  /**
   * @param pmml {@link PMML} model to write
   * @param out stream to write the model to
   * @throws IOException if an error occurs while writing the model to storage
   */
  public static void write(PMML pmml, OutputStream out) throws IOException {
    try {
      JAXBUtil.marshalPMML(pmml, new StreamResult(out));
    } catch (JAXBException e) {
      throw new IOException(e);
    }
  }

  /**
   * @param path file to read PMML from
   * @return {@link PMML} model file from path
   * @throws IOException if an error occurs while reading the model from storage
   */
  public static PMML read(Path path) throws IOException {
    try (InputStream in = IOUtils.readMaybeCompressed(path)) {
      return read(in);
    }
  }

  /**
   * @param in stream to read PMML from
   * @return {@link PMML} model file from stream
   * @throws IOException if an error occurs while reading the model from the stream
   */
  public static PMML read(InputStream in) throws IOException {
    try {
      return JAXBUtil.unmarshalPMML(new StreamSource(in));
    } catch (JAXBException e) {
      throw new IOException(e);
    }
  }

  /**
   * @param pmml model
   * @return model serialized as an XML document as a string
   * @throws IOException if XML can't be serialized
   */
  public static String toString(PMML pmml) throws IOException {
    try (StringWriter out = new StringWriter()) {
      JAXBUtil.marshalPMML(pmml, new StreamResult(out));
      return out.toString();
    } catch (JAXBException e) {
      throw new IOException(e);
    }
  }

  /**
   * @param pmmlString PMML model encoded as an XML doc in a string
   * @return {@link PMML} object representing the model
   * @throws IOException if XML can't be unserialized
   */
  public static PMML fromString(String pmmlString) throws IOException {
    try {
      return JAXBUtil.unmarshalPMML(new StreamSource(new StringReader(pmmlString)));
    } catch (JAXBException e) {
      throw new IOException(e);
    }
  }

  public static String getExtensionValue(PMML pmml, String name) {
    for (Extension extension : pmml.getExtensions()) {
      if (name.equals(extension.getName())) {
        return extension.getValue();
      }
    }
    return null;
  }

  public static List<Object> getExtensionContent(PMML pmml, String name) {
    for (Extension extension : pmml.getExtensions()) {
      if (name.equals(extension.getName())) {
        return extension.getContent();
      }
    }
    return null;
  }

  public static void addExtension(PMML pmml, String key, String value) {
    Extension extension = new Extension();
    extension.setName(key);
    extension.setValue(value);
    pmml.getExtensions().add(extension);
  }

  public static void addExtensionContent(PMML pmml, String key, Collection<?> content) {
    if (content.isEmpty()) {
      return;
    }
    Collection<String> stringContent = new ArrayList<>(content.size());
    for (Object o : content) {
      stringContent.add(o.toString());
    }
    Extension extension = new Extension();
    extension.setName(key);
    extension.getContent().addAll(stringContent);
    pmml.getExtensions().add(extension);
  }

  /**
   * @param pmmlArrayContent the content of a node that serializes an array PMML-style
   * @return array values in order
   */
  public static List<String> parseArray(List<?> pmmlArrayContent) {
    String spaceSeparated = pmmlArrayContent.get(0).toString();
    if (spaceSeparated.isEmpty()) {
      return Collections.emptyList();
    }
    return Arrays.asList(spaceSeparated.split(" "));
  }

}
