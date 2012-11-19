#!/usr/bin/env python

"""
Thor SPDY Server
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

import os
import sys
import logging

from thor import schedule
from thor.events import EventEmitter, on
from common import SpdyMessageHandler, CTL_SYN_REPLY, FLAG_NONE, FLAG_FIN
import thor.loop
from thor.http.common import get_header, dummy
from thor.tcp import TcpServer

# FIXME: assure that the connection isn't closed before reading the entire req body

class SpdyServer(EventEmitter):
    "An asynchronous SPDY server."

    tcp_server_class = TcpServer

    def __init__(self, host, port, loop=None):
        EventEmitter.__init__(self)
        self.tcp_server = self.tcp_server_class(host, port, loop=loop)
        self.tcp_server.on('connect', self.handle_conn)
        schedule(0, self.emit, 'start')

    def handle_conn(self, tcp_conn):
        """Process a new tcp connection, tcp_conn."""
        spdy_conn = SpdyServerConnection(tcp_conn, self)
        tcp_conn.on('data', spdy_conn.handle_input)
        tcp_conn.on('close', spdy_conn.conn_closed)
        tcp_conn.on('pause', spdy_conn.res_body_pause)
        tcp_conn.pause(False)

    def shutdown(self):
        """Stop the server"""
        self.tcp_server.shutdown()
        self.emit('stop')


class SpdyServerConnection(SpdyMessageHandler, EventEmitter):
    """A handler for a SPDY server connection."""

    def __init__(self, tcp_conn, server):
        SpdyMessageHandler.__init__(self)
        EventEmitter.__init__(self)
        self._tcp_conn = tcp_conn
        self._streams = {}
        self.server = server
        self.output_paused = False

    def req_body_pause(self, paused):
        "Indicate that the server should pause (True) or unpause (False) the request."
        if self._tcp_conn and self._tcp_conn.tcp_connected:
            self._tcp_conn.pause(paused)

    # Methods called by tcp

    def res_body_pause(self, paused):
        "Pause/unpause sending the response body."
        # TODO: Perhaps add back in res_body_pause_cb in the constructor?
        if self._res_body_pause_cb:
            self._res_body_pause_cb(paused)

    def conn_closed(self):
        "The server connection has closed."
        pass # FIXME: any cleanup necessary?
#        self.pause()
#        self.tcp_conn.handler = None
#        self.tcp_conn = None

    # Methods called by common.SpdyRequestHandler

    def output(self, chunk):
        if self._tcp_conn:
            self._tcp_conn.write(chunk)

    def input_start(self, stream_id, hdr_tuples):
        method = get_header(hdr_tuples, 'method')[0] # FIXME: error handling
        uri = get_header(hdr_tuples, 'url')[0] # FIXME: error handling
        assert not self._streams.has_key(stream_id) # FIXME
        # TODO: sanity checks / catch errors from requst_handler
        exchange = SpdyServerExchange(self, stream_id, method, uri, hdr_tuples)
        self._streams[stream_id] = exchange
        self.server.emit('exchange', exchange)
        if not self.output_paused:
            # we only start new requests if we have some output buffer
            # available.
            exchange.request_start()
        #allows_body = (content_length) or (transfer_codes != [])
        #return allows_body
        # TODO: Make this actually decide if a response body is allowed
        return True

    def input_body(self, stream_id, chunk):
        "Process a request body chunk from the wire."
        self._streams[stream_id].emit('request_body', chunk)

    def input_end(self, stream_id):
        "Indicate that the request body is complete."
        self._streams[stream_id].emit('request_done', None)
        # TODO: delete stream if output side is half-closed.

    def input_error(self, stream_id, err, detail=None):
        "Indicate a parsing problem with the request body."
        # FIXME: rework after fixing spdy_common
        err['detail'] = detail
        if self._tcp_conn:
            self._tcp_conn.close()
            self._tcp_conn = None
        self._streams[stream_id].emit('request_done', err)

    # TODO: re-evaluate if this is necessary in SPDY
    def _handle_error(self, err, detail=None):
        "Handle a problem with the request by generating an appropriate response."
        if detail:
            err['detail'] = detail
        status_code, status_phrase = err.get('status', ('400', 'Bad Request'))
        hdrs = [
            ('Content-Type', 'text/plain'),
        ]
        body = err['desc']
        if err.has_key('detail'):
            body += " (%s)" % err['detail']
        self.res_start(status_code, status_phrase, hdrs, dummy)
        self.res_body(body)
        self.res_done()

class SpdyServerExchange(EventEmitter):
    """An exchange for a SPDY server connection."""

    def __init__(self, spdy_conn, stream_id, method, uri, req_hdrs):
        EventEmitter.__init__(self)
        self.spdy_conn = spdy_conn
        self.method = method
        self.uri = uri
        self.stream_id = stream_id
        self.req_hdrs = req_hdrs
        self.started = False

    def __repr__(self):
        status = [self.__class__.__module__ + "." + self.__class__.__name__]
        status.append('%s {%s}' % (self.method or "-", self.uri or "-"))
        return "<%s at %#x>" % (", ".join(status), id(self))

    def request_start(self):
        self.started = True
        self.emit('request_start', self.method, self.uri, self.req_hdrs)

    def response_start(self, status_code, status_phrase, res_hdrs):
        "Start a response. Must only be called once per response."
        res_hdrs.append(('status', "%s %s" % (status_code, status_phrase)))
        res_hdrs.append(('version', 'HTTP/1.1'))
        # TODO: hop-by-hop headers?
        self.spdy_conn.output_start(self.stream_id, res_hdrs)

    def response_body(self, chunk):
        "Send part of the response body. May be called zero to many times."
        if chunk:
            self.spdy_conn.output_body(self.stream_id, chunk)

    def response_done(self, err):
        """
        Signal the end of the response, whether or not there was a body. MUST be
        called exactly once for each response.

        If err is not None, it is an error dictionary (see the error module)
        indicating that an HTTP-specific (i.e., non-application) error occured
        in the generation of the response; this is useful for debugging.
        """
        # TODO: Do something with err...
        self.spdy_conn.output_end(self.stream_id, "")
        # TODO: delete stream after checking that input side is half-closed

def test_handler(x):
    @on(x, 'request_start')
    def go(*args):
        print "start: %s on %s" % (str(args[1]), id(x.spdy_conn))
        x.response_start(200, "OK", [])
        x.response_body('foo!')
        x.response_done([])

    @on(x, 'request_body')
    def body(chunk):
        print "body: %s" % chunk

    @on(x, 'request_done')
    def done(trailers):
        print "done: %s" % str(trailers)

if __name__ == "__main__":
    from thor.loop import run
    sys.stderr.write("PID: %s\n" % os.getpid())
    h, p = '127.0.0.1', int(sys.argv[1])
    server = SpdyServer(h, p)
    server.on('exchange', test_handler)
    run()
