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

import asyncio as _asyncio
import collections as _collections
import json as _json
import proton as _proton
import proton.handlers as _proton_handlers
import proton.reactor as _proton_reactor
import threading as _threading
import traceback as _traceback
import uuid as _uuid

class MoonRiver:
    def __init__(self, id=None, debug=False):
        self._debug = debug

        self._receiver_functions = list()
        self._sender_queues = list()

        self._events = _proton_reactor.EventInjector()
        self._container = _proton_reactor.Container(_Handler(self))
        self._container.selectable(self._events)
        self._stop_event = _proton_reactor.ApplicationEvent("stop")

        if id is not None:
            if "%" in id:
                id = id.replace("%", str(_uuid.uuid4())[-8:], 1)

            self._container.container_id = id

        self.debug("Created container")

    @property
    def id(self):
        return self._container.container_id

    def debug(self, message, *args):
        if not self._debug:
            return

        message = message.format(*args)

        print(f"{self.id}: {message}")

    def receiver(app, address):
        class _Receiver:
            def __init__(self, function):
                self.function = function
                self.address = address

                app._receiver_functions.append(self)

            def __call__(self, message):
                self.function(message)

        return _Receiver

    def run(self):
        loop = _asyncio.new_event_loop()
        _asyncio.set_event_loop(loop)

        try:
            self._container.run()
        except KeyboardInterrupt:
            pass
        except:
            _traceback.print_exc()
        finally:
            self._events.close()

    def start(self):
        _threading.Thread(target=self.run).start()
        return _Lifecycle(self)

    def stop(self):
        self._events.trigger(self._stop_event)

class _Lifecycle:
    def __init__(self, app):
        self.app = app

    def __enter__(self):
        return self.app

    def __exit__(self, exc_type, exc_value, traceback):
        self.app.stop()

class Sender:
    def __init__(self, app, address):
        self.app = app
        self.address = address

        self._items = _collections.deque()
        self._event = None

        self.app._impl._sender_queues.append(self)

    def _bind(self, sender):
        assert self._event is None

        self._event = _proton_reactor.ApplicationEvent("queue_put", subject=sender)

    def _get(self):
        try:
            return self._items.popleft()
        except IndexError:
            return None

    def send(self, message):
        assert self._event is not None

        self._items.append(_proton.Message(message))
        self.app._impl._events.trigger(self._event)

class Delivery:
    def __init__(self, pn_delivery, pn_message):
        self._pn_delivery = pn_delivery
        self.message = pn_message.body

    def json(self):
        return _json.loads(self.message)

class _TimerHandler(_proton_reactor.Handler):
    def __init__(self, sender):
        super().__init__()

        self._sender = sender

    def on_timer_task(self, event):
        self._sender(self._sender._pn_sender)

        event.container.schedule(self._sender.period, self)

class _Handler(_proton_handlers.MessagingHandler):
    def __init__(self, app):
        super().__init__()

        self.app = app
        self.connection = None

    def on_start(self, event):
        self.app.debug("Starting")

        self.connection = event.container.connect()

        for mi_sender_queue in self.app._sender_queues:
            pn_sender = event.container.create_sender(self.connection, mi_sender_queue.address)
            pn_sender.mi_sender_queue = mi_sender_queue

            mi_sender_queue._bind(pn_sender)

            self.app.debug("Created sender for address '{}'", mi_sender_queue.address)

        for mi_receiver in self.app._receiver_functions:
            pn_receiver = event.container.create_receiver(self.connection, mi_receiver.address)
            pn_receiver.mi_receiver = mi_receiver

            self.app.debug("Created receiver for address '{}'", mi_receiver.address)

    def on_stop(self, event):
        self.app.debug("Stopping")
        self.connection.close()
        self.app._container.stop()

    def on_connection_opened(self, event):
        if hasattr(event.connection, "url"):
            self.app.debug("Connected to {}", event.connection.url)
        else:
            self.app.debug("Connected")

    def on_transport_error(self, event):
        if event.connection:
            self.app.debug("Connection error ({})", event.connection.url)

    def on_message(self, event):
        delivery = Delivery(event.delivery, event.message)
        pn_receiver = event.link

        self.app.debug("Received message from '{}'", pn_receiver.source.address)

        pn_receiver.mi_receiver(delivery)

    def on_queue_put(self, event):
        pn_sender = event.subject
        mi_sender_queue = pn_sender.mi_sender_queue

        while True or pn_sender.credit:
            message = mi_sender_queue._get()

            if message is None:
                break

            pn_sender.send(message)

            self.app.debug("Sent message to '{}'", pn_sender.target.address)
