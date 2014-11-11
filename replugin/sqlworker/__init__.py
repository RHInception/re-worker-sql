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

from sqlalchemy import Table, Column, MetaData
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
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
    subcommands = ('CreateTable', 'ExecuteSQL')
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

            metadata, engine = self._db_connect(db_name)
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

            metadata, engine = self._db_connect(db_name)
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

    def _db_connect(self, db_name):
        """
        Create connection to the database.

        Parameters:
            * db_name: The name of the databaes key in the configuration file
        """
        try:
            metadata = MetaData()
            connection_info = self._config['databases'][db_name]
            connection_str = connection_info['uri']
            conn_kwargs = connection_info.get('kwargs', {})
            engine = create_engine(connection_str, **conn_kwargs)
            # This will fail with OperationalError if we can not conenct.
            engine.connect()
            metadata.bind = engine
            return (metadata, engine)
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

            if subcommand == 'CreateTable':
                self.app_logger.info(
                    'Executing subcommand %s for correlation_id %s' % (
                        subcommand, corr_id))
                result = self.create_table(body, corr_id, output)
            elif subcommand == 'ExecuteSQL':
                self.app_logger.info(
                    'Executing subcommand %s for correlation_id %s' % (
                        subcommand, corr_id))
                result = self.execute_sql(body, corr_id, output)
            else:
                self.app_logger.warn(
                    'Could not find the implementation of subcommand %s' % (
                        subcommand))
                raise SQLWorkerError('No subcommand implementation')

            # Send results back
            self.send(
                properties.reply_to,
                corr_id,
                {'status': 'completed', 'data': result},
                exchange='re'
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
                exchange='re'
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
