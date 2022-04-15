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

import json as _json

from . import _amqp

class MoonRiver:
    def __init__(self, id=None, debug=False):
        self._impl = _amqp.MoonRiver(id=id, debug=debug)

    @property
    def id(self):
        return self._impl.id

    def receiver(app, address):
        return app._impl.receiver(address)

    def run(self):
        return self._impl.run()

    def start(self):
        return self._impl.start()

    def stop(self):
        self._impl.stop()

class Sender:
    def __init__(self, app, address):
        self._impl = _amqp.Sender(app, address)

    @property
    def app(self):
        return self._impl.app

    @property
    def address(self):
        return self._impl.address

    def send(self, message):
        self._impl.send(message)

class Delivery:
    def __init__(self, pn_delivery, pn_message): # XXX Trouble
        self._pn_delivery = pn_delivery
        self.message = pn_message.body

    def json(self):
        return _json.loads(self.message)
