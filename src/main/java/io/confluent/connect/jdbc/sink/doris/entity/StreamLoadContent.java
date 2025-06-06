/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink.doris.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamLoadContent {

  @JsonProperty(value = "TxnId")
  private Long txnId;

  @JsonProperty(value = "Label")
  private String label;

  @JsonProperty(value = "Status")
  private String status;

  @JsonProperty(value = "TwoPhaseCommit")
  private String twoPhaseCommit;

  @JsonProperty(value = "ExistingJobStatus")
  private String existingJobStatus;

  @JsonProperty(value = "Message")
  private String message;

  @JsonProperty(value = "NumberTotalRows")
  private Long numberTotalRows;

  @JsonProperty(value = "NumberLoadedRows")
  private Long numberLoadedRows;

  @JsonProperty(value = "NumberFilteredRows")
  private Integer numberFilteredRows;

  @JsonProperty(value = "NumberUnselectedRows")
  private Integer numberUnselectedRows;

  @JsonProperty(value = "LoadBytes")
  private Long loadBytes;

  @JsonProperty(value = "LoadTimeMs")
  private Integer loadTimeMs;

  @JsonProperty(value = "BeginTxnTimeMs")
  private Integer beginTxnTimeMs;

  @JsonProperty(value = "StreamLoadPutTimeMs")
  private Integer streamLoadPutTimeMs;

  @JsonProperty(value = "ReadDataTimeMs")
  private Integer readDataTimeMs;

  @JsonProperty(value = "WriteDataTimeMs")
  private Integer writeDataTimeMs;

  @JsonProperty(value = "CommitAndPublishTimeMs")
  private Integer commitAndPublishTimeMs;

  @JsonProperty(value = "ErrorURL")
  private String errorURL;
}
