/*
 *
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
 *
 */
package org.apache.qpid.proton.demo;

import org.apache.qpid.proton.engine.Event;

/**
 * Events
 *
 */

public final class Events
{

    private Events() {}

    public static void dispatch(Event event, EventHandler handler) {
        switch (event.getType()) {
        case CONNECTION_INIT:
            handler.onInit(event.getConnection());
            break;
        case CONNECTION_OPEN:
            handler.onOpen(event.getConnection());
            break;
        case CONNECTION_REMOTE_OPEN:
            handler.onRemoteOpen(event.getConnection());
            break;
        case CONNECTION_CLOSE:
            handler.onClose(event.getConnection());
            break;
        case CONNECTION_REMOTE_CLOSE:
            handler.onRemoteClose(event.getConnection());
            break;
        case CONNECTION_FINAL:
            handler.onFinal(event.getConnection());
            break;
        case SESSION_INIT:
            handler.onInit(event.getSession());
            break;
        case SESSION_OPEN:
            handler.onOpen(event.getSession());
            break;
        case SESSION_REMOTE_OPEN:
            handler.onRemoteOpen(event.getSession());
            break;
        case SESSION_CLOSE:
            handler.onClose(event.getSession());
            break;
        case SESSION_REMOTE_CLOSE:
            handler.onRemoteClose(event.getSession());
            break;
        case SESSION_FINAL:
            handler.onFinal(event.getSession());
            break;
        case LINK_INIT:
            handler.onInit(event.getLink());
            break;
        case LINK_OPEN:
            handler.onOpen(event.getLink());
            break;
        case LINK_REMOTE_OPEN:
            handler.onRemoteOpen(event.getLink());
            break;
        case LINK_CLOSE:
            handler.onClose(event.getLink());
            break;
        case LINK_REMOTE_CLOSE:
            handler.onRemoteClose(event.getLink());
            break;
        case LINK_FLOW:
            handler.onFlow(event.getLink());
            break;
        case LINK_FINAL:
            handler.onFinal(event.getLink());
            break;
        case TRANSPORT:
            handler.onTransport(event.getTransport());
            break;
        case DELIVERY:
            handler.onDelivery(event.getDelivery());
            break;
        }
    }

}
