#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import os, random
from proton import *
from socket import *
from select import select

class EventDispatcher:

    methods = {
        Event.CONNECTION_INIT: "connection_init",
        Event.CONNECTION_OPEN: "connection_open",
        Event.CONNECTION_REMOTE_OPEN: "connection_remote_open",
        Event.CONNECTION_CLOSE: "connection_close",
        Event.CONNECTION_REMOTE_CLOSE: "connection_remote_close",
        Event.CONNECTION_FINAL: "connection_final",

        Event.SESSION_INIT: "session_init",
        Event.SESSION_OPEN: "session_open",
        Event.SESSION_REMOTE_OPEN: "session_remote_open",
        Event.SESSION_CLOSE: "session_close",
        Event.SESSION_REMOTE_CLOSE: "session_remote_close",
        Event.SESSION_FINAL: "session_final",

        Event.LINK_INIT: "link_init",
        Event.LINK_OPEN: "link_open",
        Event.LINK_REMOTE_OPEN: "link_remote_open",
        Event.LINK_CLOSE: "link_close",
        Event.LINK_REMOTE_CLOSE: "link_remote_close",
        Event.LINK_FLOW: "link_flow",
        Event.LINK_FINAL: "link_final",

        Event.TRANSPORT: "transport",
        Event.DELIVERY: "delivery"
    }

    def dispatch(self, event):
        getattr(self, self.methods[event.type], self.unhandled)(event)

    def unhandled(self, event):
        pass

class Selectable:

    def __init__(self, transport, socket):
        self.transport = transport
        self.socket = socket
        self.write_done = False
        self.read_done = False

    def closed(self):
        if self.write_done and self.read_done:
            self.socket.close()
            return True
        else:
            return False

    def fileno(self):
        return self.socket.fileno()

    def reading(self):
        c = self.transport.capacity()
        if c > 0:
            return True
        elif c < 0:
            self.read_done = True

    def writing(self):
        if self.write_done: return False
        try:
            p = self.transport.pending()
            if p > 0:
                return True
            elif p < 0:
                self.write_done = True
                return False
        except TransportException, e:
            self.write_done = True
            return False

    def readable(self):
        c = self.transport.capacity()
        if c > 0:
            data = self.socket.recv(c)
            try:
                if data:
                    self.transport.push(data)
                else:
                    self.transport.close_tail()
            except TransportException, e:
                print e
                self.read_done = True
        elif c < 0:
            self.read_done = True

    def writable(self):
        try:
            p = self.transport.pending()
            if p > 0:
                data = self.transport.peek(p)
                n = self.socket.send(data)
                self.transport.pop(n)
            elif p < 0:
                self.write_done = True
        except TransportException, e:
            self.write_done = True

class Acceptor:

    def __init__(self, driver, host, port):
        self.driver = driver
        self.socket = socket()
        self.socket.setblocking(0)
        self.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.socket.bind((host, port))
        self.socket.listen(5)
        self.driver.selectables.append(self)

    def closed(self):
        return False

    def fileno(self):
        return self.socket.fileno()

    def reading(self):
        return True

    def writing(self):
        return False

    def readable(self):
        sock, addr = self.socket.accept()
        sock.setblocking(0)
        print "Incoming Connection:", addr
        if sock:
            conn = Connection()
            conn.collect(self.driver.collector)
            transport = Transport()
            transport.bind(conn)
            sasl = transport.sasl()
            sasl.mechanisms("ANONYMOUS")
            sasl.server()
            sasl.done(SASL.OK)
            sel = Selectable(transport, sock)
            self.driver.selectables.append(sel)

class Driver(EventDispatcher):

    def __init__(self, collector, *dispatchers):
        self.collector = collector
        self.dispatchers = dispatchers
        self.selectables = []
        self._abort = False
        self._exit = False

    def abort(self):
        self._abort = True

    def exit(self):
        self._exit = True

    def run(self):
        while True:
            self.process_events()
            if self._abort: return

            reading = []
            writing = []

            for s in self.selectables:
                if s.reading(): reading.append(s)
                if s.writing(): writing.append(s)
                if s.closed(): self.selectables.remove(s)

            if self._exit and not self.selectables: return

            readable, writable, _ = select(reading, writing, [], 3)

            for s in readable:
                s.readable()
            for s in writable:
                s.writable()

    def process_events(self):
        while True:
            ev = self.collector.peek()
            if ev:
                self.dispatch(ev)
                for d in self.dispatchers:
                    d.dispatch(ev)
                self.collector.pop()
            else:
                return

    def connection_open(self, event):
        conn = event.connection
        if conn.state & Endpoint.REMOTE_UNINIT:
            transport = Transport()
            sasl = transport.sasl()
            sasl.mechanisms("ANONYMOUS")
            sasl.client()
            transport.bind(conn)
            sock = socket()
            sock.setblocking(0)
            sock.connect_ex((conn.hostname, 5672))
            selectable = Selectable(transport, sock)
            self.selectables.append(selectable)

class Handshaker(EventDispatcher):

    def connection_remote_open(self, event):
        conn = event.connection
        if conn.state & Endpoint.LOCAL_UNINIT:
            conn.open()

    def session_remote_open(self, event):
        ssn = event.session
        if ssn.state & Endpoint.LOCAL_UNINIT:
            ssn.open()

    def link_remote_open(self, event):
        link = event.link
        if link.state & Endpoint.LOCAL_UNINIT:
            link.source.copy(link.remote_source)
            link.target.copy(link.remote_target)
            link.open()

    def connection_remote_close(self, event):
        conn = event.connection
        if not (conn.state & Endpoint.LOCAL_CLOSED):
            conn.close()

    def session_remote_close(self, event):
        ssn = event.session
        if not (ssn.state & Endpoint.LOCAL_CLOSED):
            ssn.close()

    def link_remote_close(self, event):
        link = event.link
        if not (link.state & Endpoint.LOCAL_CLOSED):
            link.close()

class FlowController(EventDispatcher):

    def __init__(self, window):
        self.window = window

    def top_up(self, link):
        delta = self.window - link.credit
        link.flow(delta)

    def link_open(self, event):
        if event.link.is_receiver:
            self.top_up(event.link)

    def link_remote_open(self, event):
        if event.link.is_receiver:
            self.top_up(event.link)

    def link_flow(self, event):
        if event.link.is_receiver:
            self.top_up(event.link)

    def delivery(self, event):
        if event.delivery.link.is_receiver:
            self.top_up(event.delivery.link)

class Row:

    def __init__(self):
        self.links = set()

    def add(self, link):
        self.links.add(link)

    def discard(self, link):
        self.links.discard(link)

    def choose(self):
        if self.links:
            return random.choice(list(self.links))
        else:
            return None

    def __nonzero__(self):
        return bool(self.links)


class Router(EventDispatcher):

    EMPTY = Row()

    def __init__(self):
        self._outgoing = {}
        self._incoming = {}

    def incoming(self, address):
        return self._incoming.get(address, self.EMPTY)

    def outgoing(self, address):
        return self._outgoing.get(address, self.EMPTY)

    def address(self, link):
        if link.is_sender:
            return link.source.address or link.target.address
        else:
            return link.target.address

    def table(self, link):
        if link.is_sender:
            return self._outgoing
        else:
            return self._incoming

    def add(self, link):
        address = self.address(link)
        table = self.table(link)
        row = table.get(address)
        if row is None:
            row = Row()
            table[address] = row
        row.add(link)

    def remove(self, link):
        address = self.address(link)
        table = self.table(link)
        row = table.get(address)
        if row is not None:
            row.discard(link)
            if not row:
                del table[address]

    def link_open(self, event):
        self.add(event.link)

    def link_close(self, event):
        self.remove(event.link)

    def link_final(self, event):
        self.remove(event.link)

class Pool:

    def __init__(self, collector, router=None):
        self.collector = collector
        self._connections = {}
        if router:
            self.outgoing_resolver = lambda address: router.outgoing(address).choose()
            self.incoming_resolver = lambda address: router.incoming(address).choose()
        else:
            self.outgoing_resolver = lambda address: None
            self.incoming_resolver = lambda address: None

    def resolve(self, remote, local, resolver, constructor):
        link = resolver(remote)
        if link is None:
            host = remote[2:].split("/", 1)[0]
            conn = self._connections.get(host)
            if conn is None:
                conn = Connection()
                conn.collect(self.collector)
                conn.hostname = host
                conn.open()
                self._connections[host] = conn

            ssn = conn.session()
            ssn.open()
            link = constructor(ssn, remote, local)
            link.open()
        return link

    def outgoing(self, target, source=None):
        return self.resolve(target, source, self.outgoing_resolver, self.new_outgoing)

    def incoming(self, source, target=None):
        return self.resolve(source, target, self.incoming_resolver, self.new_incoming)

    def new_outgoing(self, ssn, remote, local):
        snd = ssn.sender("%s-%s" % (local, remote))
        snd.source.address = local
        snd.target.address = remote
        return snd

    def new_incoming(self, ssn, remote, local):
        rcv = ssn.receiver("%s-%s" % (remote, local))
        rcv.source.address = remote
        rcv.target.address = local
        return rcv
