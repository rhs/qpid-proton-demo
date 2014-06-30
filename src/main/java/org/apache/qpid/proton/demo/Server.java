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

import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Collector;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Sender;

import java.io.IOException;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * Server
 *
 */

public class Server extends AbstractEventHandler
{

    private class Message {

        private final byte[] bytes;

        public Message(byte[] bytes) {
            this.bytes = bytes;
        }

        public byte[] getBytes() {
            return bytes;
        }

        public String toString() {
            return new String(bytes);
        }

    }

    private class MessageStore {

        Map<String,Deque<Message>> messages = new HashMap<String,Deque<Message>>();

        void put(String address, Message message) {
            Deque<Message> queue = messages.get(address);
            if (queue == null) {
                queue = new ArrayDeque<Message>();
                messages.put(address, queue);
            }
            queue.add(message);
        }

        Message get(String address) {
            Deque<Message> queue = messages.get(address);
            if (queue == null) { return null; }
            Message msg = queue.remove();
            if (queue.isEmpty()) {
                messages.remove(address);
            }
            return msg;
        }

    }

    final private MessageStore messages = new MessageStore();
    final private Router router;
    private int tag = 0;

    public Server(Router router) {
        this.router = router;
    }

    private byte[] nextTag() {
        return String.format("%s", tag++).getBytes();
    }

    private int send(String address) {
        return send(address, null);
    }

    private int send(String address, Sender snd) {
        if (snd == null) {
            Router.Routes<Sender> routes = router.getOutgoing(address);
            snd = routes.choose();
            if (snd == null) {
                return 0;
            }
        }

        int count = 0;
        while (snd.getCredit() > 0) {
            Message msg = messages.get(address);
            if (msg == null) {
                snd.drained();
                return count;
            }
            Delivery dlv = snd.delivery(nextTag());
            byte[] bytes = msg.getBytes();
            snd.send(bytes, 0, bytes.length);
            dlv.settle();
            count++;
            System.out.println(String.format("Sent message(%s): %s", address, msg));
        }

        return count;
    }

    public void onFlow(Link link) {
        if (link instanceof Sender) {
            Sender snd = (Sender) link;
            send(router.getAddress(snd), snd);
        }
    }

    public void onDelivery(Delivery dlv) {
        Link link = dlv.getLink();
        if (link instanceof Sender) {
            dlv.settle();
        } else {
            Receiver rcv = (Receiver) link;
            if (!dlv.isPartial()) {
                byte[] bytes = new byte[dlv.pending()];
                rcv.recv(bytes, 0, bytes.length);
                String address = router.getAddress(rcv);
                Message message = new Message(bytes);
                messages.put(address, message);
                dlv.disposition(Accepted.getInstance());
                dlv.settle();
                System.out.println(String.format("Got message(%s): %s", address, message));
                send(address);
            }
        }
    }

    public static final void main(String[] args) throws IOException {
        Collector collector = Collector.Factory.create();
        Router router = new Router();
        Driver driver = new Driver(collector, new Handshaker(),
                                   new FlowController(1024), router,
                                   new Server(router));
        driver.listen("localhost", 5672);
        driver.run();
    }

}