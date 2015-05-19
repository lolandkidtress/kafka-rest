/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package io.confluent.kafkarest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SpoolMessage {
  private int attempt;
  private long timestamp;
  private String topic;
  private byte[] key;
  private byte[] value;

  @JsonCreator
  public SpoolMessage(@JsonProperty("attempt") int attempt,
                      @JsonProperty("timestamp") long timestamp,
                      @JsonProperty("topic") String topic,
                      @JsonProperty("key") byte[] key,
                      @JsonProperty("value") byte[] value) {
    this.attempt = attempt;
    this.timestamp = timestamp;
    this.topic = topic;
    this.key = key;
    this.value = value;
  }

  @Override
  public String toString() {
    return "SpoolMessage{" +
           "attempt=" + attempt +
           ", timestamp=" + timestamp +
           ", topic=" + topic +
           ", key=" + EntityUtils.encodeBase64Binary(key) +
           ", value=" + EntityUtils.encodeBase64Binary(value) + '\'' +
           '}';
  }
}
