"""A lightweight wrapper around MySQL Connector/Python."""
# -*- coding: utf-8 -*-

import logging
from collections import namedtuple
import time
import mysql.connector

__version__ = '0.0.1'


class JeekdbError(Exception):
    """jeekdb error"""
    pass


ExecuteResult = namedtuple('ExecuteResult', ['last_row_id'], ['row_count'])


class Connection(object):
    """wrapper around mysql.connector.MySQLConnection"""

    def __init__(self, host, port, user, password, database, connection_timeout=5,
                 max_idle_time=3 * 60, **kwargs):
        """
        init a connection, a connection will be reconnect after max_idle_time
        :param kwargs: arguments for mysql.connector
            https://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html
        """
        self.max_idle_time = float(max_idle_time)
        self._last_use_time = time.time()
        self._db = None
        self._db_args = dict(
            host=host, port=port, user=user, password=password, database=database,
            connection_timeout=connection_timeout, autocommit=True, raise_on_warnings=True,
            **kwargs)
        try:
            self.reconnect()
        except Exception as e:
            logging.error('failed to connect %s:%d %s %s', host, port, database, e, exc_info=True)
            raise e

    def __del__(self):
        self.close()

    def _exceed_max_idle_time(self):
        return time.time() - self._last_use_time > self.max_idle_time

    def close(self):
        """close connection if connected"""
        if self._db is not None:
            self._db.close()
            self._db = None

    def reconnect(self):
        """
        close the existing connection and re-open it
        """
        self.close()
        self._db = mysql.connector.connect(**self._db_args)

    def _ensure_connected(self):
        """MySQL or other MySQL proxy will close connections that are idle for some time, but the
        client library will not report this face until the next query try when it fails."""
        if self._db is None or self._exceed_max_idle_time():
            self.reconnect()
        self._last_use_time = time.time()

    def _cursor(self):
        self._ensure_connected()
        return self._db.cursor(dictionary=True)

    def _execute(self, cursor, sql, parameter_dict=None):
        """
        wrap cursor.execute(). parameter_dict is a dict.
        :param cursor:
        :param sql: SQL, e.g. INSERT INTO my_table (name, field1) VALUES (%(name)s, %(company)s)
        :param parameter_dict: dict of parameter, e.g. {'name':'John', 'company':'Baidu'}
        :type parameter_dict: dict
        :return:
        """
        if parameter_dict is None:
            parameter_dict = {}
        try:
            return cursor.execute(sql, parameter_dict)
        except mysql.connector.Error as e:
            logging.error("_execute failed, query=%s, parameter_dict=%s, error=%s",
                          sql, parameter_dict, e, exc_info=True)
            self.close()
            raise e

    def iter(self, sql, parameter_dict=None, size=20):
        """
        returns an iterator for the given query and parameters
        :param sql: SQL, e.g. INSERT INTO my_table (name, field1) VALUES (%(name)s, %(company)s)
        :param parameter_dict: dict of parameter, e.g. {'name':'John', 'company':'Baidu'}
        :param size: size for fetchmany
        :type sql: str
        :type parameter_dict: dict
        :return: iterator
        """
        if parameter_dict is None:
            parameter_dict = {}
        cursor = self._cursor()
        try:
            self._execute(cursor, sql, parameter_dict)
            while True:
                rows = cursor.fetchmany(size=size)
                if not rows:
                    break
                for row in rows:
                    yield row
        finally:
            cursor.close()

    def query(self, sql, parameter_dict=None):
        """
        return a list of dict for the given sql and parameter_dict
        :param sql: SQL, e.g. INSERT INTO my_table (name, field1) VALUES (%(name)s, %(company)s)
        :param parameter_dict: dict of parameter, e.g. {'name':'John', 'company':'Baidu'}
        :type sql: str
        :type parameter_dict: dict
        :return: list
        """
        if parameter_dict is None:
            parameter_dict = {}
        cursor = self._cursor()
        try:
            self._execute(cursor, sql, parameter_dict)
            rows = cursor.fetchall()
            return rows
        finally:
            cursor.close()

    def get_one(self, sql, parameter_dict=None):
        """
        return a singular row, if it has more than one result, raise an exception
        :param sql: SQL, e.g. INSERT INTO my_table (name, field1) VALUES (%(name)s, %(company)s)
        :param parameter_dict: dict of parameter, e.g. {'name':'John', 'company':'Baidu'}
        :type sql: str
        :type parameter_dict: dict
        :return: dict
        """
        if parameter_dict is None:
            parameter_dict = {}
        rows = self.query(sql, parameter_dict)
        if not rows:
            return None
        if len(rows) > 1:
            raise JeekdbError("multiple rows returned for get_one()")
        return rows[0]

    def execute(self, sql, parameter_dict=None):
        """
        execute the given sql, return ExecuteResult
        :param sql: SQL, e.g. INSERT INTO my_table (name, field1) VALUES (%(name)s, %(company)s)
        :param parameter_dict: dict of parameter, e.g. {'name':'John', 'company':'Baidu'}
        :type sql: str
        :type parameter_dict: dict
        :return: ExecuteResult
        """
        if parameter_dict is None:
            parameter_dict = {}
        cursor = self._cursor()
        try:
            self._execute(cursor, sql, parameter_dict)
            last_row_id = cursor.lastrowid
            row_count = cursor.rowcount
            return ExecuteResult(last_row_id=last_row_id, row_count=row_count)
        finally:
            cursor.close()

    update = delete = insert = execute
