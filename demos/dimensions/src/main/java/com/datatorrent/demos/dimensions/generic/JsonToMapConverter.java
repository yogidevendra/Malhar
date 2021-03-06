/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.DTThrowable;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


/**
 * Converts incoming JSON tuple to Map&lt;String,Object&gt; representation.
 * <p>
 * Data types are converted as follows:
 *
 *   object =&gt; LinkedHashMap<String,Object>
 *   array =&gt; ArrayList<Object>
 *   string =&gt; String
 *   number (no fraction) =&gt; Integer, Long or BigInteger (smallest applicable)
 *   number (fraction) =&gt; Double (configurable to use BigDecimal)
 *   true|false =&gt; Boolean
 *   null =&gt; null
 *
 * @displayName JSON to Map Parser
 * @category Stream
 * @tags parser, json, demo, converter
 *
 * @since 2.0.0
 */

@Stateless
public class JsonToMapConverter extends BaseOperator {

  private static final ObjectMapper mapper = new ObjectMapper();
  private static final ObjectReader reader = mapper.reader(new TypeReference<Map<String,Object>>() { });
  private static final Logger logger = LoggerFactory.getLogger(JsonToMapConverter.class);

  /**
   * Accepts JSON formatted byte arrays
   */
  public final transient DefaultInputPort<byte[]> input = new DefaultInputPort<byte[]>()
  {
    @Override
    public void process(byte[] message)
    {
      try {
        // Convert byte array JSON representation to HashMap
        Map<String, Object> tuple = reader.readValue(message);
        outputMap.emit(tuple);
      }
      catch (Throwable ex) {
        DTThrowable.rethrow(ex);
      }
    }
  };

  /**
   * Output JSON converted to Map<string,Object>
   */
  public final transient DefaultOutputPort<Map<String, Object>> outputMap = new DefaultOutputPort<Map<String, Object>>();

}
