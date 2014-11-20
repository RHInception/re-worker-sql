# -*- coding: utf-8 -*-
# Copyright © 2014 SEE AUTHORS FILE
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
SQL worker.
"""

import sqlalchemy.types

from sqlalchemy import Table, Column, MetaData, create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

from alembic.migration import MigrationContext
from alembic.op import Operations

from reworker.worker import Worker


class SQLWorkerError(Exception):
    """
    Base exception class for SQLWorker errors.
    """
    pass


class SQLWorker(Worker):
    """
    Worker which provides basic functionality for SQL databases.
    """

    #: allowed subcommands
    subcommands = (
        'CreateTable', 'ExecuteSQL', 'AlterTableColumns',
        'AddTableColumns', 'DropTableColumns', 'DropTable',
        'Insert')
    dynamic = []

    # Subcommand methods
    def create_table(self, body, corr_id, output):
        """
        Creates a database table.

        Parameters:

        * body: The message body structure
        * corr_id: The correlation id of the message
        * output: The output object back to the user
        """
        # Get needed variables
        params = body.get('parameters', {})

        try:
            db_name = params['database']
            table_name = params['name']
            columns = params['columns']

            metadata, engine, conn = self._db_connect(db_name)
            self.app_logger.info('Attempting create the table ...')

            # This dynamically makes the database structure
            # It expects data like:
            #   {"colname": {"type": "Integer", "primary_key": True}}}
            new_table = Table(table_name, metadata)
            for k, v in columns.items():
                col_type = getattr(sqlalchemy.types, v['type'])
                del v['type']
                new_table.append_column(Column(k, col_type, **v))
            try:
                new_table.create()
            except OperationalError, oe:
                raise SQLWorkerError(
                    'Could not create the table %s: %s' % (
                        params.get('name', 'NAME_NOT_GIVEN'), oe.message))

        except KeyError, ke:
            output.error('Unable to create table %s because of missing input %s' % (
                params.get('name', 'NAME_NOT_GIVEN'), ke))
            raise SQLWorkerError('Missing input %s' % ke)

    def execute_sql(self, body, corr_id, output):
        """
        Executes raw SQL.

        Parameters:

        * body: The message body structure
        * corr_id: The correlation id of the message
        * output: The output object back to the user
        """
        # Get needed variables
        params = body.get('parameters', {})

        try:
            db_name = params['database']
            sql = params['sql']

            metadata, engine, conn = self._db_connect(db_name)
            self.app_logger.info('Attempting to execute sql ...')

            try:
                r = engine.execute(sql)
                if (r.context.isdelete or
                        r.context.isupdate or
                        r.context.isinsert):
                    return "%s rows effected" % r.rowcount
                elif r.context.isddl:
                    return "DDL executed"
                else:
                    return "SQL executed"
            except OperationalError, oe:
                raise SQLWorkerError(
                    'Could not execute the given sql: %s' % oe.message)
        except KeyError, ke:
            output.error('Unable to execute sqlbecause of missing input %s' % (
               ke))
            raise SQLWorkerError('Missing input %s' % ke)

    def drop_table(self, body, corr_id, output):
        """
        Drops a table.

        Parameters:

        * body: The message body structure
        * corr_id: The correlation id of the message
        * output: The output object back to the user
        """
        # Get needed variables
        params = body.get('parameters', {})

        try:
            db_name = params['database']
            table_name = params['name']

            metadata, engine, conn = self._db_connect(db_name)
            session = sessionmaker()(bind=engine)
            ops = Operations(MigrationContext(conn.dialect, conn, {}))

            try:
                self.app_logger.info('Attempting to drop a table ...')
                ops.drop_table(table_name)
                return "Table %s droppped" % table_name
            except OperationalError, oe:
                raise SQLWorkerError(
                    'Could not execute the given drop table %s' % oe.message)
            session.flush()
        except KeyError, ke:
            output.error('Unable to execute drop table. Missing input %s' % (
               ke))
            raise SQLWorkerError('Missing input %s' % ke)

    def drop_table_columns(self, body, corr_id, output):
        """
        Drops a tables columns.

        Parameters:

        * body: The message body structure
        * corr_id: The correlation id of the message
        * output: The output object back to the user
        """
        # Get needed variables
        params = body.get('parameters', {})

        try:
            db_name = params['database']
            table_name = params['name']
            columns = params['columns']

            metadata, engine, conn = self._db_connect(db_name)
            session = sessionmaker()(bind=engine)
            ops = Operations(MigrationContext(conn.dialect, conn, {}))

            try:
                self.app_logger.info('Attempting to drop columns ...')
                for column in columns:
                    ops.drop_column(table_name, column)
                return "DDL executed"
            except OperationalError, oe:
                raise SQLWorkerError(
                    'Could not execute the given alter %s' % oe.message)
            session.flush()
        except KeyError, ke:
            output.error('Unable to execute alter of missing input %s' % (
               ke))
            raise SQLWorkerError('Missing input %s' % ke)

    def alter_table_columns(self, body, corr_id, output):
        """
        Alters a tables columns.

        Parameters:

        * body: The message body structure
        * corr_id: The correlation id of the message
        * output: The output object back to the user
        """
        # Get needed variables
        params = body.get('parameters', {})

        try:
            db_name = params['database']
            table_name = params['name']
            columns = params['columns']

            metadata, engine, conn = self._db_connect(db_name)
            session = sessionmaker()(bind=engine)
            mctx = MigrationContext.configure(conn)

            try:
                self.app_logger.info('Attempting to alter a table ...')
                add_cols = []
                for k, v in columns.items():
                    col_type = getattr(sqlalchemy.types, v['type'])
                    del v['type']
                    mc = Column(k, col_type, **v)
                    new_kwargs = {
                        'type_': mc.type,
                        'nullable': mc.nullable,
                        'autoincrement': mc.autoincrement,
                    }
                    mctx.impl.alter_column(table_name, k, **new_kwargs)
                return "DDL executed"
            except OperationalError, oe:
                raise SQLWorkerError(
                    'Could not execute the given alter %s' % oe.message)
            except Exception, ex:
                raise ex
        except KeyError, ke:
            output.error('Unable to execute alter of missing input %s' % (
               ke))
            raise SQLWorkerError('Missing input %s' % ke)

    def add_table_columns(self, body, corr_id, output):
        """
        Adds columns to a table.

        Parameters:

        * body: The message body structure
        * corr_id: The correlation id of the message
        * output: The output object back to the user
        """
        # Get needed variables
        params = body.get('parameters', {})

        try:
            db_name = params['database']
            table_name = params['name']
            columns = params['columns']

            metadata, engine, conn = self._db_connect(db_name)
            session = sessionmaker()(bind=engine)
            mctx = MigrationContext.configure(conn)

            try:
                self.app_logger.info('Attempting to alter a table ...')
                add_cols = []
                for k, v in columns.items():
                    col_type = getattr(sqlalchemy.types, v['type'])
                    del v['type']
                    mc = Column(k, col_type, **v)
                    mctx.impl.add_column(table_name, mc)
                return "DDL executed"
            except OperationalError, oe:
                raise SQLWorkerError(
                    'Could not execute the given alter %s' % oe.message)
        except KeyError, ke:
            output.error('Unable to execute alter of missing input %s' % (
               ke))
            raise SQLWorkerError('Missing input %s' % ke)

    def insert(self, body, corr_id, output):
        """
        Adds insert a row or rows into a table.

        Parameters:

        * body: The message body structure
        * corr_id: The correlation id of the message
        * output: The output object back to the user
        """
        # Get needed variables
        params = body.get('parameters', {})

        try:
            db_name = params['database']
            table_name = params['name']
            rows = params['rows']

            metadata, engine, conn = self._db_connect(db_name)
            mctx = MigrationContext.configure(conn)

            try:
                self.app_logger.info('Attempting to insert into a table ...')
                table = Table(table_name, metadata, autoload=True)
                for row in rows:
                    row_data = {}
                    for k, v in row.items():
                        row_data[k] = v
                    i = table.insert().values(**row_data)
                    engine.execute(i)
                return "Insert statements done"
            except OperationalError, oe:
                raise SQLWorkerError(
                    'Could not execute the given insert %s' % oe.message)
        except KeyError, ke:
            output.error('Unable to execute insert of missing input %s' % (
               ke))
            raise SQLWorkerError('Missing input %s' % ke)

    def _db_connect(self, db_name):
        """
        Create connection to the database.

        Parameters:
            * db_name: The name of the databaes key in the configuration file
        """
        try:
            connection_info = self._config['databases'][db_name]
            connection_str = connection_info['uri']
            conn_kwargs = connection_info.get('kwargs', {})
            engine = create_engine(connection_str, **conn_kwargs)
            # This will fail with OperationalError if we can not conenct.
            conn = engine.connect()
            metadata = MetaData(bind=engine, reflect=True)
            return (metadata, engine, conn)
        except KeyError:
            raise SQLWorkerError(
                'No database configured with the given name. '
                'Check your database parameter.')
        except OperationalError:
            raise SQLWorkerError(
                'Could not connect to the database requested.')

    def process(self, channel, basic_deliver, properties, body, output):
        """
        Processes SQLWorker requests from the bus.

        *Keys Requires*:
            * subcommand: the subcommand to execute.
        """
        # Ack the original message
        self.ack(basic_deliver)
        corr_id = str(properties.correlation_id)
        # Notify we are starting
        self.send(
            properties.reply_to, corr_id, {'status': 'started'}, exchange='')

        try:
            try:
                subcommand = str(body['parameters']['subcommand'])
                if subcommand not in self.subcommands:
                    raise KeyError()
            except KeyError:
                raise SQLWorkerError(
                    'No valid subcommand given. Nothing to do!')

            cmd_method = None
            if subcommand == 'CreateTable':
                cmd_method = self.create_table
            elif subcommand == 'DropTable':
                cmd_method = self.drop_table
            elif subcommand == 'AlterTableColumns':
                cmd_method = self.alter_table_columns
            elif subcommand == 'AddTableColumns':
                cmd_method = self.add_table_columns
            elif subcommand == 'DropTableColumns':
                cmd_method = self.drop_table_columns
            elif subcommand == 'ExecuteSQL':
                cmd_method = self.execute_sql
            elif subcommand == 'Insert':
                cmd_method = self.insert
            else:
                self.app_logger.warn(
                    'Could not find the implementation of subcommand %s' % (
                        subcommand))
                raise SQLWorkerError('No subcommand implementation')

            result = cmd_method(body, corr_id, output)
            # Send results back
            self.send(
                properties.reply_to,
                corr_id,
                {'status': 'completed', 'data': result},
                exchange=''
            )
            # Notify on result. Not required but nice to do.
            self.notify(
                'SQLWorker Executed Successfully',
                'SQLWorker successfully executed %s. See logs.' % (
                    subcommand),
                'completed',
                corr_id)

            # Send out responses
            self.app_logger.info(
                'SQLWorker successfully executed %s for '
                'correlation_id %s. See logs.' % (
                    subcommand, corr_id))

        except SQLWorkerError, fwe:
            # If a SQLWorkerError happens send a failure log it.
            self.app_logger.error('Failure: %s' % fwe)
            self.send(
                properties.reply_to,
                corr_id,
                {'status': 'failed'},
                exchange=''
            )
            self.notify(
                'SQLWorker Failed',
                str(fwe),
                'failed',
                corr_id)
            output.error(str(fwe))


def main():  # pragma: no cover
    from reworker.worker import runner
    runner(SQLWorker)


if __name__ == '__main__':  # pragma nocover
    main()
