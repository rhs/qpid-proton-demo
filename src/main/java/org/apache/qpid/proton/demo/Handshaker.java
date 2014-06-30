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

import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.EndpointState;

/**
 * Handshaker
 *
 */

public class Handshaker extends AbstractEventHandler
{

    public void onRemoteOpen(Connection conn) {
        if (conn.getLocalState() == EndpointState.UNINITIALIZED) {
            conn.open();
        }
    }

    public void onRemoteOpen(Session ssn) {
        if (ssn.getLocalState() == EndpointState.UNINITIALIZED) {
            ssn.open();
        }
    }

    public void onRemoteOpen(Link link) {
        if (link.getLocalState() == EndpointState.UNINITIALIZED) {
            link.setSource(link.getRemoteSource());
            link.setTarget(link.getRemoteTarget());
            link.open();
        }
    }

    public void onRemoteClose(Connection conn) {
        if (conn.getLocalState() != EndpointState.CLOSED) {
            conn.close();
        }
    }

    public void onRemoteClose(Session ssn) {
        if (ssn.getLocalState() != EndpointState.CLOSED) {
            ssn.close();
        }
    }

    public void onRemoteClose(Link link) {
        if (link.getLocalState() != EndpointState.CLOSED) {
            link.close();
        }
    }

}
