# Copyright (C) 2014 SEE AUTHORS FILE
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""
Unittests.
"""

import os
import pika
import mock
import sqlalchemy

from contextlib import nested

from . import TestCase

from replugin import sqlworker


MQ_CONF = {
    'server': '127.0.0.1',
    'port': 5672,
    'vhost': '/',
    'user': 'guest',
    'password': 'guest',
}


class TestSQLWorker(TestCase):

    def setUp(self):
        """
        Set up some reusable mocks.
        """
        TestCase.setUp(self)

        self.channel = mock.MagicMock('pika.spec.Channel')

        self.channel.basic_consume = mock.Mock('basic_consume')
        self.channel.basic_ack = mock.Mock('basic_ack')
        self.channel.basic_publish = mock.Mock('basic_publish')

        self.basic_deliver = mock.MagicMock()
        self.basic_deliver.delivery_tag = 123

        self.properties = mock.MagicMock(
            'pika.spec.BasicProperties',
            correlation_id=123,
            reply_to='me')

        self.logger = mock.MagicMock('logging.Logger').__call__()
        self.app_logger = mock.MagicMock('logging.Logger').__call__()
        self.connection = mock.MagicMock('pika.SelectConnection')

    def tearDown(self):
        """
        After every test.
        """
        TestCase.tearDown(self)
        self.channel.reset_mock()
        self.channel.basic_consume.reset_mock()
        self.channel.basic_ack.reset_mock()
        self.channel.basic_publish.reset_mock()

        self.basic_deliver.reset_mock()
        self.properties.reset_mock()

        self.logger.reset_mock()
        self.app_logger.reset_mock()
        self.connection.reset_mock()

    def test_bad_command(self):
        """
        If a bad command is sent the worker should fail.
        """
        with nested(
                mock.patch('pika.SelectConnection'),
                mock.patch('replugin.sqlworker.SQLWorker.notify'),
                mock.patch('replugin.sqlworker.SQLWorker.send')):

            worker = sqlworker.SQLWorker(
                MQ_CONF,
                logger=self.app_logger,
                config_file='conf/example.json')

            worker._on_open(self.connection)
            worker._on_channel_open(self.channel)

            body = {
                "parameters": {
                    "command": "sql",
                    "subcommand": "this is not a thing",
                },
            }

            # Execute the call
            worker.process(
                self.channel,
                self.basic_deliver,
                self.properties,
                body,
                self.logger)

            assert self.app_logger.error.call_count == 1
            assert worker.send.call_args[0][2]['status'] == 'failed'

    def test__db_connect(self):
        """
        Test _connect works as it should.
        """
        with nested(
                mock.patch('pika.SelectConnection'),
                mock.patch('replugin.sqlworker.SQLWorker.notify'),
                mock.patch('replugin.sqlworker.SQLWorker.send')):

            worker = sqlworker.SQLWorker(
                MQ_CONF,
                logger=self.app_logger,
                config_file='conf/example.json')

            metadata, engine = worker._db_connect('memory')
            # We should get a valida MetaData and Engine instance
            assert isinstance(metadata, sqlalchemy.MetaData)
            assert isinstance(engine, sqlalchemy.engine.base.Engine)

    def test_create_table(self):
        """
        Verify create_table works when all proper information is passed.
        """
        with nested(
                mock.patch('pika.SelectConnection'),
                mock.patch('replugin.sqlworker.SQLWorker.notify'),
                mock.patch('replugin.sqlworker.SQLWorker.send')):

            worker = sqlworker.SQLWorker(
                MQ_CONF,
                logger=self.app_logger,
                config_file='conf/example.json')

            worker._on_open(self.connection)
            worker._on_channel_open(self.channel)

            body = {
                "parameters": {
                    "command": "sql",
                    "subcommand": "CreateTable",
                    "database": "testdb",
                    "name": "newtable",
                    "columns": {
                        "colname": {"type": "Integer", "primary_key": True}}
                },
            }

            # Execute the call
            worker.process(
                self.channel,
                self.basic_deliver,
                self.properties,
                body,
                self.logger)

            _, engine = worker._db_connect('testdb')

            # This should raise an exception
            assert engine.execute('SELECT * FROM newtable')

            # This should raise an exception
            self.assertRaises(
                sqlalchemy.exc.OperationalError,
                 engine.execute,
                 'SELECT * FROM doesnotexist')

            # Manual cleanup
            os.remove('test.db')
