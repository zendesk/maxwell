package com.zendesk.maxwell.distributed;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.zendesk.maxwell.Maxwell;
import com.zendesk.maxwell.MaxwellConfig;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@StateModelInfo(initialState = "OFFLINE", states = {
    "OFFLINE", "ONLINE"
})
public class ActiveMaxwellLock extends StateModel {
  static final Logger LOGGER = LoggerFactory.getLogger(ActiveMaxwellLock.class);

  private final MaxwellConfig config;
  private Maxwell maxwell;

  public ActiveMaxwellLock(MaxwellConfig config) {
      this.config = config;
  }

  @Transition(from = "OFFLINE", to = "ONLINE")
  public void lock(Message m, NotificationContext context) throws Exception {
    LOGGER.info(context.getManager().getClusterName() + " : " + context.getManager().getInstanceName() + " applied Active");
    LOGGER.info("Start Maxwell Active Node");
    maxwell = new Maxwell(this.config);
    maxwell.run();
  }

  @Transition(from = "ONLINE", to = "OFFLINE")
  public void release(Message m, NotificationContext context) {
    LOGGER.info("Maxwell Active Node changed status");
    maxwell.terminate();
  }

}
