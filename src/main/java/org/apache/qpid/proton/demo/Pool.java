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

import org.apache.qpid.proton.engine.Collector;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Receiver;

import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;

import java.util.HashMap;
import java.util.Map;

/**
 * Pool
 *
 */

public class Pool
{

    final private Collector collector;
    final private Map<String,Connection> connections;

    public Pool(Collector collector) {
        this.collector = collector;
        connections = new HashMap<String,Connection>();
    }

    public Link resolve(String remote, String local, boolean outgoing) {
        String host = remote.substring(2).split("/", 2)[0];
        Connection conn = connections.get(host);
        Link link;
        if (conn == null) {
            conn = Connection.Factory.create();
            conn.collect(collector);
            conn.setHostname(host);
            conn.open();
            connections.put(host, conn);

            Session ssn = conn.session();
            ssn.open();
            if (outgoing) {
                link = newOutgoing(ssn, remote, local);
            } else {
                link = newIncoming(ssn, remote, local);
            }
            link.open();
        } else {
            throw new RuntimeException("TODO");
        }
        return link;
    }

    public Sender outgoing(String target, String source) {
        return (Sender) resolve(target, source, true);
    }

    public Receiver incoming(String source, String target) {
        return (Receiver) resolve(source, target, false);
    }

    public Sender newOutgoing(Session ssn, String remote, String local) {
        Sender snd = ssn.sender(String.format("%s-%s", local, remote));
        Source src = new Source();
        src.setAddress(local);
        snd.setSource(src);
        Target tgt = new Target();
        tgt.setAddress(remote);
        snd.setTarget(tgt);
        return snd;
    }

    public Receiver newIncoming(Session ssn, String remote, String local) {
        Receiver rcv = ssn.receiver(String.format("%s-%s", remote, local));
        Source src = new Source();
        src.setAddress(remote);
        rcv.setSource(src);
        Target tgt = new Target();
        tgt.setAddress(remote);
        rcv.setTarget(tgt);
        return rcv;
    }

}
