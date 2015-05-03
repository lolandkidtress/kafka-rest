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

import com.fasterxml.jackson.annotation.JsonProperty;

import org.hibernate.validator.constraints.NotEmpty;

//import javax.validation.constraints.NotNull;

public class Broker {

  private int id;

  @NotEmpty
  private String host;

  private int port;

  public Broker(@JsonProperty("id") int id,
                @JsonProperty("host") String host,
                @JsonProperty("port") int port) {
    this.id = id;
    this.host = host;
    this.port = port;
  }

  @JsonProperty
  public int getId() {
    return id;
  }

  @JsonProperty
  public void setId(int id) {
    this.id = id;
  }

  @JsonProperty
  public String getHost() {
    return host;
  }

  @JsonProperty
  public void setHost(String host) {
    this.host = host;
  }

  @JsonProperty
  public int getPort() {
    return port;
  }

  @JsonProperty
  public void setPort(int port) {
    this.port = port;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Broker broker = (Broker) o;

    if (id != broker.id) {
      return false;
    }

    if (host != null ? !host.equals(broker.host) : broker.host != null) {
      return false;
    }

    if (port != broker.port) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = 31 * id + (host != null ? host.hashCode() : 0);
    result = 31 * result + port;
    return result;
  }
}
