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

    def _create_dummy_db(self, conn, table_name):
        conn.execute('CREATE TABLE ' + table_name + ' (a INTEGER, b INTEGER);')

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
        try:
            os.remove('test.db')
        except:
            pass

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

            metadata, engine, conn = worker._db_connect('memory')
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
                    "name": "test_newtable",
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

            _, engine, conn = worker._db_connect('testdb')

            # This should not raise an exception
            assert engine.execute('SELECT * FROM test_newtable')

            # This should raise an exception
            try:
                 engine.execute('SELECT * FROM doesnotexist')
                 self.fail('doesnotexist did not rais an exception')
            except (sqlalchemy.exc.OperationalError,
                    sqlalchemy.exc.ProgrammingError):
                pass

    def test_drop_table(self):
        """
        Verify drop_table works when all proper information is passed.
        """
        table_name = 'test_droptable'
        with nested(
                mock.patch('pika.SelectConnection'),
                mock.patch('replugin.sqlworker.SQLWorker.notify'),
                mock.patch('replugin.sqlworker.SQLWorker.send')):

            worker = sqlworker.SQLWorker(
                MQ_CONF,
                logger=self.app_logger,
                config_file='conf/example.json')


            _, engine, conn = worker._db_connect('testdb')
            self._create_dummy_db(conn, table_name)

            worker._on_open(self.connection)
            worker._on_channel_open(self.channel)

            body = {
                "parameters": {
                    "command": "sql",
                    "subcommand": "DropTable",
                    "database": "testdb",
                    "name": table_name,
                },
            }

            # Execute the call
            worker.process(
                self.channel,
                self.basic_deliver,
                self.properties,
                body,
                self.logger)

            # This should raise an exception since the table does not
            # exist any longer
            try:
                 engine.execute('SELECT * FROM ' + table_name)
                 self.fail('The table was not dropped')
            except (sqlalchemy.exc.OperationalError,
                    sqlalchemy.exc.ProgrammingError):
                pass

    def test_drop_table_failure(self):
        """
        Verify drop_table fails when the table does not exist.
        """
        table_name = 'test_droptable_failure'
        with nested(
                mock.patch('pika.SelectConnection'),
                mock.patch('replugin.sqlworker.SQLWorker.notify'),
                mock.patch('replugin.sqlworker.SQLWorker.send')):

            worker = sqlworker.SQLWorker(
                MQ_CONF,
                logger=self.app_logger,
                config_file='conf/example.json')


            _, engine, conn = worker._db_connect('testdb')

            worker._on_open(self.connection)
            worker._on_channel_open(self.channel)

            body = {
                "parameters": {
                    "command": "sql",
                    "subcommand": "DropTable",
                    "database": "testdb",
                    "name": table_name,
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

    def test_execute_sql_with_ddl(self):
        """
        Verify that raw ddl sql can be executed.
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
                    "subcommand": "ExecuteSQL",
                    "database": "testdb",
                    "sql": "CREATE TABLE test (a int);",
                },
            }

            # Execute the call
            worker.process(
                self.channel,
                self.basic_deliver,
                self.properties,
                body,
                self.logger)

            _, engine, conn = worker._db_connect('testdb')

            assert engine.execute('SELECT * from test')
            assert self.app_logger.error.call_count == 0
            assert worker.send.call_args[0][2]['status'] == 'completed'

    def test_execute_sql_with_select(self):
        """
        Verify that raw sql can be executed.
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
                    "subcommand": "ExecuteSQL",
                    "database": "testdb",
                    "sql": "SELECT 1",
                },
            }

            # Execute the call
            worker.process(
                self.channel,
                self.basic_deliver,
                self.properties,
                body,
                self.logger)

            _, engine, conn = worker._db_connect('testdb')

            assert self.app_logger.error.call_count == 0
            assert worker.send.call_args[0][2]['status'] == 'completed'

    def test_execute_sql_fails_with_bad_sql(self):
        """
        Verify that broken raw sql causes a failure.
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
                    "subcommand": "ExecuteSQL",
                    "database": "testdb",
                    "sql": "doesnotexist",
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

    def test_drop_table_columns(self):
        """
        Verify drop_table_column works when all proper information is passed.
        """
        table_name = 'test_drop_table_columns'
        with nested(
                mock.patch('pika.SelectConnection'),
                mock.patch('replugin.sqlworker.SQLWorker.notify'),
                mock.patch('replugin.sqlworker.SQLWorker.send'),
                mock.patch('sqlalchemy.dialects.sqlite.dialect')) as (
                    _, _, _, _impl):

            worker = sqlworker.SQLWorker(
                MQ_CONF,
                logger=self.app_logger,
                config_file='conf/example.json')

            _, engine, conn = worker._db_connect('testdb')
            self._create_dummy_db(conn, table_name)
            worker._on_open(self.connection)
            worker._on_channel_open(self.channel)

            body = {
                "parameters": {
                    "command": "sql",
                    "subcommand": "DropTableColumns",
                    "database": "testdb",
                    "name": table_name,
                    "columns": ['a'],
                },
            }

            # Execute the call
            worker.process(
                self.channel,
                self.basic_deliver,
                self.properties,
                body,
                self.logger)

            _impl.drop_column.assert_called_once()

    def test_alter_table_columns(self):
        """
        Verify alter_table_columns works when all proper information is passed.
        """
        table_name = 'test_alter_table_columns'
        with nested(
                mock.patch('pika.SelectConnection'),
                mock.patch('replugin.sqlworker.SQLWorker.notify'),
                mock.patch('replugin.sqlworker.SQLWorker.send')):

            worker = sqlworker.SQLWorker(
                MQ_CONF,
                logger=self.app_logger,
                config_file='conf/example.json')

            _, engine, conn = worker._db_connect('testdb')
            self._create_dummy_db(conn, table_name)

            worker._on_open(self.connection)
            worker._on_channel_open(self.channel)

            body = {
                "parameters": {
                    "command": "sql",
                    "subcommand": "AlterTableColumns",
                    "database": "testdb",
                    "name": table_name,
                    "columns": {
                        "c": {"type": "String", "length": 255},
                        "d": {"type": "Integer"},
                    },
                },
            }

            # Execute the call
            worker.process(
                self.channel,
                self.basic_deliver,
                self.properties,
                body,
                self.logger)

            # Sadly SQLite can not actually do this so we verify the error
            assert worker.send.call_args[0][2]['status'] == 'failed'

    def test_add_table_columns(self):
        """
        Verify add_table_column works when all proper information is passed.
        """
        table_name = 'test_add_table_columns'
        with nested(
                mock.patch('pika.SelectConnection'),
                mock.patch('replugin.sqlworker.SQLWorker.notify'),
                mock.patch('replugin.sqlworker.SQLWorker.send')):

            worker = sqlworker.SQLWorker(
                MQ_CONF,
                logger=self.app_logger,
                config_file='conf/example.json')

            _, engine, conn = worker._db_connect('testdb')
            self._create_dummy_db(conn, table_name)

            worker._on_open(self.connection)
            worker._on_channel_open(self.channel)

            body = {
                "parameters": {
                    "command": "sql",
                    "subcommand": "AddTableColumns",
                    "database": "testdb",
                    "name": table_name,
                    "columns": {
                        "c": {"type": "String", "length": 255}
                    }
                },
            }

            # Execute the call
            worker.process(
                self.channel,
                self.basic_deliver,
                self.properties,
                body,
                self.logger)

            # This should not raise an exception as it includes the new col
            assert engine.execute('INSERT INTO ' + table_name +' VALUES (1, 2, "hi");')

            # This should raise an exception since it doesn't have 3 values
            try:
                 engine.execute('INSERT INTO ' + table_name + ' VALUES (1, 2);')
                 self.fail('The values should not have worked.')
            except (sqlalchemy.exc.OperationalError,
                    sqlalchemy.exc.ProgrammingError):
                pass

    def test_add_table_columns_failure(self):
        """
        Verify add_table_column fails when a col can not be added.
        """
        table_name = 'test_add_table_columns_fail'
        with nested(
                mock.patch('pika.SelectConnection'),
                mock.patch('replugin.sqlworker.SQLWorker.notify'),
                mock.patch('replugin.sqlworker.SQLWorker.send')):

            worker = sqlworker.SQLWorker(
                MQ_CONF,
                logger=self.app_logger,
                config_file='conf/example.json')

            _, engine, conn = worker._db_connect('testdb')

            worker._on_open(self.connection)
            worker._on_channel_open(self.channel)

            body = {
                "parameters": {
                    "command": "sql",
                    "subcommand": "AddTableColumns",
                    "database": "testdb",
                    "name": table_name,
                    "columns": {
                        "c": {"type": "String", "length": 255}
                    }
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

    def test_insert(self):
        """
        Verify inserting works.
        """
        table_name = 'test_insert'
        with nested(
                mock.patch('pika.SelectConnection'),
                mock.patch('replugin.sqlworker.SQLWorker.notify'),
                mock.patch('replugin.sqlworker.SQLWorker.send')):

            worker = sqlworker.SQLWorker(
                MQ_CONF,
                logger=self.app_logger,
                config_file='conf/example.json')

            _, engine, conn = worker._db_connect('testdb')
            self._create_dummy_db(conn, table_name)

            worker._on_open(self.connection)
            worker._on_channel_open(self.channel)

            body = {
                "parameters": {
                    "command": "sql",
                    "subcommand": "Insert",
                    "database": "testdb",
                    "name": table_name,
                    "rows": [
                        {
                            "a": 10,
                            "b": 2,
                        },
                        {
                            "a": 15,
                            "b": 40,
                        }
                    ],
                },
            }

            # Execute the call
            worker.process(
                self.channel,
                self.basic_deliver,
                self.properties,
                body,
                self.logger)

            # This should not raise an exception as it includes the new col
            result = engine.execute(
                'SELECT COUNT(*) from ' + table_name + ';').fetchall()[0][0]
            # We should have 2 rows inserted
            assert result == 2

    def test_insert_fail(self):
        """
        Verify inserting fails if there isn't a table.
        """
        table_name = 'test_insert'
        with nested(
                mock.patch('pika.SelectConnection'),
                mock.patch('replugin.sqlworker.SQLWorker.notify'),
                mock.patch('replugin.sqlworker.SQLWorker.send')):

            worker = sqlworker.SQLWorker(
                MQ_CONF,
                logger=self.app_logger,
                config_file='conf/example.json')

            _, engine, conn = worker._db_connect('testdb')

            worker._on_open(self.connection)
            worker._on_channel_open(self.channel)

            body = {
                "parameters": {
                    "command": "sql",
                    "subcommand": "Insert",
                    "database": "testdb",
                    "name": table_name,
                    "rows": [
                        {
                            "a": 10,
                            "b": 2,
                        },
                        {
                            "a": 15,
                            "b": 40,
                        }
                    ],
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

    def test_delete(self):
        """
        Verify deleting works.
        """
        table_name = 'test_delete'
        with nested(
                mock.patch('pika.SelectConnection'),
                mock.patch('replugin.sqlworker.SQLWorker.notify'),
                mock.patch('replugin.sqlworker.SQLWorker.send')):

            worker = sqlworker.SQLWorker(
                MQ_CONF,
                logger=self.app_logger,
                config_file='conf/example.json')

            _, engine, conn = worker._db_connect('testdb')
            self._create_dummy_db(conn, table_name)
            conn.execute('INSERT INTO ' + table_name + ' VALUES (0, 0);')
            conn.execute('INSERT INTO ' + table_name + ' VALUES (1, 1);')

            worker._on_open(self.connection)
            worker._on_channel_open(self.channel)

            body = {
                "parameters": {
                    "command": "sql",
                    "subcommand": "Delete",
                    "database": "testdb",
                    "name": table_name,
                    "where": {
                            "a": 0,
                            "b": 0,
                    },
                },
            }

            # Execute the call
            worker.process(
                self.channel,
                self.basic_deliver,
                self.properties,
                body,
                self.logger)

            # This should not raise an exception as it includes the new col
            result = engine.execute(
                'SELECT COUNT(*) from ' + table_name + ';').fetchall()[0][0]
            # We should have 1 row left as we deleted the other row
            assert result == 1
