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

from sqlalchemy import Table, Column, Metadata

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
    subcommands = ('CreateTable', )
    dynamic = []

    # Subcommand methods
    def create_table(self, body, corr_id, output):
        # Get needed variables
        params = body.get('parameters', {})

        try:
            table_name = params['name']
            columns = params['columns']

            self.app_logger.info('Attempting create the table ...')

            # TODO: We need an engine here
            # This dynamically makes the database structure
            # It expects data like:
            #   {"colname": {"type": "Integer", "primary_key": True}}}
            metadata = MetaData()
            new_table = Table(table_name, metadata)
            for k, v in info.items():
                col_type = getattr(sqlalchemy.types, v['type'])
                del v['type']
                new_table.append_column(Column(k, col_type, **v))

        except KeyError, ke:
            raise SQLWorkerError('Missing input %s' % ke)

    def process(self, channel, basic_deliver, properties, body, output):
        """
        Processes SQLWorker requests from the bus.

        *Keys Requires*:
            * subcommand: the subcommand to execute.
        """
        # Ack the original message
        self.ack(basic_deliver)
        corr_id = str(properties.correlation_id)

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
            else:
                self.app_logger.warn(
                    'Could not the implementation of subcommand %s' % (
                        subcommand))
                raise SQLWorkerError('No subcommand implementation')

            # Send results back
            self.send(
                properties.reply_to,
                corr_id,
                result,
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
