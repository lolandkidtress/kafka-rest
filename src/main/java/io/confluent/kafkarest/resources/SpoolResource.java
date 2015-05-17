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
package io.confluent.kafkarest.resources;

import java.util.Collection;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import io.confluent.kafkarest.Context;
import io.confluent.kafkarest.SpoolProducer;
import io.confluent.kafkarest.entities.SpoolMessage;
import io.confluent.kafkarest.entities.SpoolShard;

@Path("/spool")
public class SpoolResource {

  private final Context ctx;

  public SpoolResource(Context ctx) {
    this.ctx = ctx;
  }

  @GET
  public Collection<SpoolShard> shards() {
    return SpoolProducer.getShards();
  }

  @GET
  @Path("/{shard}")
  public SpoolShard shard(@PathParam("shard") int shard) {
    return SpoolProducer.getShard(shard);
  }

  @POST
  @Path("/{shard}/suspend")
  public void suspend(@PathParam("shard") int shard,
                      @QueryParam("timestamp") long timestamp) {
    SpoolProducer.suspendShard(shard, timestamp);
  }

  @GET
  @Path("/{shard}/peek")
  public Collection<SpoolMessage> peek(@PathParam("shard") int shard,
                                       @QueryParam("count") Integer count) throws Exception {
    return SpoolProducer.peekErroredRecords(shard, count == null ? 10 : count);
  }

  @POST
  @Path("/{shard}/revive")
  public void revive(@PathParam("shard") int shard,
                     @QueryParam("count") Integer count) {
    SpoolProducer.reviveErroredRecords(shard, count == null ? 10 : count);
  }

  @POST
  @Path("/override")
  public void override(@QueryParam("always") boolean always) {
    SpoolProducer.overrideSpoolMode(always);
  }
}
