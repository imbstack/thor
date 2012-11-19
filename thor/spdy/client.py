#!/usr/bin/env python

"""
Thor SPDY Client
"""

__author__ = "Mark Nottingham <mnot@mnot.net>"
__copyright__ = """\
Copyright (c) 2008-2011 Mark Nottingham

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

from urlparse import urlsplit

import thor
from thor.events import EventEmitter, on
from thor.tcp import TcpClient
# TODO: Add in TLS support

from thor.http.error import ConnectError, UrlError
from thor.http.common import WAITING, hop_by_hop_hdrs, dummy, get_header
from common import SpdyMessageHandler, CTL_SYN_STREAM, FLAG_NONE, FLAG_FIN

req_remove_hdrs = hop_by_hop_hdrs + ['host']

# TODO: read timeout support (needs to be in push_tcp?)

class SpdyClient(object):
    "An asynchronous SPDY client."
    proxy = None
    connect_timeout = None
    tcp_client_class = TcpClient

    def __init__(self, loop=None):
        self.loop = loop or thor.loop._loop
        self._conns = {}
        self.loop.on('stop', self._close_conns)

    def exchange(self):
        return SpdyClientConn(self)

    def _attach_conn(self, host, port, handle_connect,
               handle_connect_error, connect_timeout):
        "Find an idle connection for (host, port), or create a new one."
        while True:
            try:
                tcp_conn = self._conns[(host, port)].pop()
            except (IndexError, KeyError):
                tcp_client = self.tcp_client_class(self.loop)
                tcp_client.on('connect', handle_connect)
                tcp_client.on('connect_error', handle_connect_error)
                tcp_client.connect(host, port, connect_timeout)
                break
            if tcp_conn.tcp_connected:
                if hasattr(tcp_conn, "_idler"):
                    tcp_conn._idler.delete()
                handle_connect(tcp_conn)
                break

    def _release_conn(self, tcp_conn):
        "Add an idle connection back to the pool."
        tcp_conn.removeListeners('data', 'pause', 'close')
        tcp_conn.pause(True)
        if tcp_conn.tcp_connected:
            def idle_close():
                "Remove the connection from the pool when it closes."
                try:
                    if hasattr(tcp_conn, "_idler"):
                        tcp_conn._idler.delete()
                    self._conns[
                        (tcp_conn.host, tcp_conn.port)
                    ].remove(tcp_conn)
                except (KeyError, ValueError):
                    pass
            tcp_conn.on('close', idle_close)
            if self.idle_timeout:
                tcp_conn._idler = self.loop.schedule(
                    self.idle_timeout, tcp_conn.close
                )
            if not self._conns.has_key((tcp_conn.host, tcp_conn.port)):
                self._conns[(tcp_conn.host, tcp_conn.port)] = [tcp_conn]
            else:
                self._conns[(tcp_conn.host, tcp_conn.port)].append(tcp_conn)

    def _close_conns(self):
        "Close all idle HTTP connections."
        for conn_list in self._conns.values():
            for conn in conn_list:
                try:
                    conn.close()
                except:
                    pass
        self._conns = {}
        # TODO: probably need to close in-progress conns too.

class SpdyClientConn(SpdyMessageHandler, EventEmitter):

    def __init__(self, client):
        SpdyMessageHandler.__init__(self)
        EventEmitter.__init__(self)
        self.client = client
        self.method = None
        self.uri = None
        self.req_hdrs = None
        self.req_target = None
        self.scheme = None
        self.authority = None
        self.res_version = None
        self._tcp_conn = None
        self._conn_reusable = False
        self._req_body = False
        self._req_started = False
        self._retries = 0
        self._read_timeout_ev = None
        self._output_buffer = []
        self._streams = {}
        self._highest_stream_id = -1

    def __repr__(self):
        status = [self.__class__.__module__ + "." + self.__class__.__name__]
        status.append('%s {%s}' % (self.method or "-", self.uri or "-"))
        if self._tcp_conn:
            status.append(
              self._tcp_conn.tcp_connected and 'connected' or 'disconnected')
        return "<%s at %#x>" % (", ".join(status), id(self))

    def request_start(self, method, uri, req_hdrs):
        """
        Start a request to uri using method, where
        req_hdrs is a list of (field_name, field_value) for
        the request headers.
        """
        (scheme, authority, path, query, fragment) = urlsplit(uri)
        if scheme.lower() != 'http':
            self._handle_error(UrlError, "Only HTTP URLs are supported")
        if "@" in authority:
            userinfo, authority = authority.split("@", 1)
        if ":" in authority:
            host, port = authority.rsplit(":", 1)
            try:
                port = int(port)
            except ValueError:
                self._handle_error(UrlError, "Non-integer port in URL")
                return dummy, dummy
        else:
            host, port = authority, 80
        if not self._tcp_conn:
            self.client._attach_conn(host, port, self._handle_connect,
                    self._handle_connect_error, self.client.connect_timeout)

        req_hdrs = [i for i in req_hdrs if not i[0].lower() in req_remove_hdrs]
        req_hdrs.append(('method', method))
        req_hdrs.append(('url', uri))
        req_hdrs.append(('version', 'HTTP/1.1'))
        self._highest_stream_id += 2 # TODO: check to make sure it's not too high.. what then?
        stream_id = self._highest_stream_id
        exch = SpdyClientExchange(self, stream_id, method, uri, req_hdrs)
        self._streams[stream_id] = exch
        return stream_id


    # Methods called by tcp

    def _handle_connect(self, tcp_conn):
        "The connection has succeeded."
        self._tcp_conn = tcp_conn
        tcp_conn.on('data', self.handle_input)
        tcp_conn.on('close', self._conn_closed)
        tcp_conn.on('pause', self._req_body_pause)
        self.output("") # kick the output buffer
        self._tcp_conn.pause(False)

    def _handle_connect_error(self, host, port, err):
        "The connection has failed."
        import os, types, socket
        if type(err) == types.IntType:
            err = os.strerror(err)
        elif isinstance(err, socket.error):
            err = err[1]
        else:
            err = str(err)
        self._handle_error(ConnectError, err)

    def _conn_closed(self):
        "The server closed the connection."
        if self._input_buffer:
            self.handle_input("")
        # TODO: figure out what to do with existing conns

    def _req_body_pause(self, paused):
        "The client needs the application to pause/unpause the request body."
        # FIXME: figure out how pausing should work.
        if self._req_body_pause_cb:
            self._req_body_pause_cb(paused)

    # Methods called by common.SpdyMessageHandler

    def input_start(self, stream_id, hdr_tuples):
        """
        Take the top set of headers from the input stream, parse them
        and queue the request to be processed by the application.
        """
        status = get_header(hdr_tuples, 'status')[0]
        try:
            res_code, res_phrase = status.split(None, 1)
        except ValueError:
            res_code = status.rstrip()
            res_phrase = ""
        self._streams[stream_id].emit('response_start',
                  res_code,
                  res_phrase,
                  hdr_tuples
        )

    def input_body(self, stream_id, chunk):
        "Process a response body chunk from the wire."
        self._streams[stream_id].emit('response_body', chunk)

    def input_end(self, stream_id):
        "Indicate that the response body is complete."
        self._streams[stream_id].emit('response_done', "")
        # TODO: delete stream if output side is half-closed.

    def _input_error(self, err, detail=None):
        "Indicate a parsing problem with the response body."
        if self._tcp_conn:
            self._tcp_conn.close()
            self._tcp_conn = None
        err['detail'] = detail
        self.res_done_cb(err)

    def output_start(self, stream_id, hdr_tuples):
        syn = self._ser_syn_frame(CTL_SYN_STREAM, FLAG_NONE, stream_id, hdr_tuples)
        self.output(syn)

    def output(self, chunk):
        self._output_buffer.append(chunk)
        if self._tcp_conn and self._tcp_conn.tcp_connected:
            self._tcp_conn.write("".join(self._output_buffer))
            self._output_buffer = []

    # misc

    def _handle_error(self, err, detail=None):
        "Handle a problem with the request by generating an appropriate response."
        assert self._input_state == WAITING
        if self._tcp_conn:
            self._tcp_conn.close()
            self._tcp_conn = None
        if detail:
            err['detail'] = detail
        status_code, status_phrase = err.get('status', ('504', 'Gateway Timeout'))
        hdrs = [
            ('Content-Type', 'text/plain'),
            ('Connection', 'close'),
        ]
        body = err['desc']
        if err.has_key('detail'):
            body += " (%s)" % err['detail']
        res_body_cb, res_done_cb = self.res_start_cb(
              "1.1", status_code, status_phrase, hdrs, dummy)
        res_body_cb(str(body))
        push_tcp.schedule(0, res_done_cb, err)

    def res_body_pause(self, paused):
        "Temporarily stop / restart sending the response body."
        if self._tcp_conn and self._tcp_conn.tcp_connected:
            self._tcp_conn.pause(paused)

class SpdyClientExchange(EventEmitter):

    def __init__(self, spdy_conn, stream_id, method, uri, req_hdrs):
        EventEmitter.__init__(self)
        self.spdy_conn = spdy_conn
        self.method = method
        self.uri = uri
        self.req_hdrs = req_hdrs
        self.stream_id = stream_id
        self.started = False
        self.request_start()

    def request_start(self):
        self.started = True
        self.spdy_conn.output_start(self.stream_id, self.req_hdrs)

    def request_body(self, stream_id, chunk):
        "Send part of the request body. May be called zero to many times."
        self.spdy_conn.output_body(stream_id, chunk)

    def request_done(self, stream_id, err):
        """
        Signal the end of the request, whether or not there was a body. MUST be
        called exactly once for each request.

        If err is not None, it is an error dictionary (see the error module)
        indicating that an HTTP-specific (i.e., non-application) error occurred
        while satisfying the request; this is useful for debugging.
        """
        self.spdy_conn.output_end(stream_id, "")
        # TODO: delete stream after checking that input side is half-closed

def test_client(request_uri, out, err):
    from thor.loop import stop, run

    c = SpdyClient()
    c.connect_timeout = 5
    x = c.exchange()
    stream = x.request_start("GET", request_uri, [])

    @on(x._streams[stream])
    def response_start(status, phrase, headers):
        "Print the response headers."
        print "HTTP/%s %s %s" % (x.res_version, status, phrase)
        print "\n".join(["%s:%s" % header for header in headers])
        print

    @on(x._streams[stream])
    def error(err_msg):
        if err_msg:
            err("*** ERROR: %s (%s)\n" %
                (err_msg.desc, err_msg.detail)
            )
        stop()

    x._streams[stream].on('response_body', out)

    @on(x._streams[stream])
    def response_done(trailers):
        stop()

    x._streams[stream].request_done(stream, [])
    run()

if __name__ == "__main__":
    import sys
    test_client(sys.argv[1], sys.stdout.write, sys.stderr.write)
