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

public class SpoolShard {
  private String path;
  private long timestamp;

  @JsonCreator
  public SpoolShard(@JsonProperty("spool_path") String path,
                    @JsonProperty("resume_timestamp") long timestamp) {
    this.path = path;
    this.timestamp = timestamp;
  }

  @Override
  public String toString() {
    return "SpoolShard{" +
           "spool_path=" + path +
           ", resume_timestamp=" + timestamp + '\'' +
           '}';
  }
}
