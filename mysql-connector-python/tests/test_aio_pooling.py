# Copyright (c) 2013, 2022, Oracle and/or its affiliates. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0, as
# published by the Free Software Foundation.
#
# This program is also distributed with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an
# additional permission to link the program and your derivative works
# with the separately licensed software that they have included with
# MySQL.
#
# Without limiting anything contained in the foregoing, this file,
# which is part of MySQL Connector/Python, is also subject to the
# Universal FOSS Exception, version 1.0, a copy of which can be found at
# http://oss.oracle.com/licenses/universal-foss-exception.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

"""Unittests for mysql.connector.pooling
"""

import unittest
import uuid

from queue import Queue

try:
    from mysql.connector.aio.connection_cext import CMySQLConnection
except ImportError:
    CMySQLConnection = None

import tests

from mysql.connector import errors
from mysql.connector.aio import pooling
from mysql.connector.aio.connection import MySQLConnection
from mysql.connector.constants import ClientFlag

MYSQL_CNX_CLASS = (
    MySQLConnection
    if CMySQLConnection is None
    else (MySQLConnection, CMySQLConnection)
)


class PooledMySQLConnectionTests(tests.MySQLConnectorTests):

    def test___init__(self):
        dbconfig = tests.get_mysql_config()
        if tests.MYSQL_VERSION < (5, 7):
            dbconfig["client_flags"] = [-ClientFlag.CONNECT_ARGS]
        cnxpool = pooling.MySQLConnectionPool(pool_size=1, **dbconfig)
        self.assertRaises(TypeError, pooling.PooledMySQLConnection)
        cnx = MySQLConnection(**dbconfig)
        pcnx = pooling.PooledMySQLConnection(cnxpool, cnx)
        self.assertEqual(cnxpool, pcnx._cnx_pool)
        self.assertEqual(cnx, pcnx._cnx)

        self.assertRaises(AttributeError, pooling.PooledMySQLConnection, None, None)
        self.assertRaises(AttributeError, pooling.PooledMySQLConnection, cnxpool, None)

    def test___getattr__(self):
        dbconfig = tests.get_mysql_config()
        if tests.MYSQL_VERSION < (5, 7):
            dbconfig["client_flags"] = [-ClientFlag.CONNECT_ARGS]
        cnxpool = pooling.MySQLConnectionPool(pool_size=1, pool_name="test")
        cnx = MySQLConnection(**dbconfig)
        pcnx = pooling.PooledMySQLConnection(cnxpool, cnx)

        exp_attrs = {
            "_connection_timeout": dbconfig["connection_timeout"],
            "_database": dbconfig["database"],
            "_host": dbconfig["host"],
            "_password": dbconfig["password"],
            "_port": dbconfig["port"],
            "_unix_socket": dbconfig["unix_socket"],
        }
        for attr, value in exp_attrs.items():
            self.assertEqual(
                value,
                getattr(pcnx, attr),
                "Attribute {0} of reference connection not correct".format(attr),
            )

        self.assertEqual(pcnx.connect, cnx.connect)

    async def test_close(self):
        dbconfig = tests.get_mysql_config()
        if tests.MYSQL_VERSION < (5, 7):
            dbconfig["client_flags"] = [-ClientFlag.CONNECT_ARGS]
        cnxpool = pooling.MySQLConnectionPool(pool_size=1, **dbconfig)

        cnxpool._original_cnx = None

        def dummy_add_connection(self, cnx=None):
            self._original_cnx = cnx

        cnxpool.add_connection = dummy_add_connection.__get__(
            cnxpool, pooling.MySQLConnectionPool
        )
        if tests.MYSQL_VERSION < (5, 7):
            dbconfig["client_flags"] = [-ClientFlag.CONNECT_ARGS]
        pcnx = pooling.PooledMySQLConnection(cnxpool, MySQLConnection(**dbconfig))

        cnx = pcnx._cnx
        await pcnx.close()
        self.assertEqual(cnx, cnxpool._original_cnx)

    async def test_config(self):
        dbconfig = tests.get_mysql_config()
        cnxpool = pooling.MySQLConnectionPool(pool_size=1, **dbconfig)
        cnx = await cnxpool.get_connection()

        self.assertRaises(errors.PoolError, cnx.config, user="spam")


class MySQLConnectionPoolTests(tests.MySQLConnectorTests):

    async def test_open(self):
        dbconfig = tests.get_mysql_config()
        if tests.MYSQL_VERSION < (5, 7):
            dbconfig["client_flags"] = [-ClientFlag.CONNECT_ARGS]

        with self.assertRaises(errors.PoolError):
            pool = pooling.MySQLConnectionPool()
            await pool.open()

        with self.assertRaises(AttributeError):
            pool = pooling.MySQLConnectionPool(
                pool_name="test",
                pool_size=-1,
            )
            await pool.open()

        with self.assertRaises(AttributeError):
            pool = pooling.MySQLConnectionPool(
                pool_name="test",
                pool_size=0,
            )
            await pool.open()

        with self.assertRaises(AttributeError):
            pool = pooling.MySQLConnectionPool(
                pool_name="test",
                pool_size=(pooling.CNX_POOL_MAXSIZE + 1),
            )
            await pool.open()

        cnxpool = pooling.MySQLConnectionPool(pool_name="test")
        self.assertEqual(5, cnxpool._pool_size)
        self.assertEqual("test", cnxpool._pool_name)
        self.assertEqual({}, cnxpool._cnx_config)
        self.assertTrue(isinstance(cnxpool._cnx_queue, Queue))
        self.assertTrue(isinstance(cnxpool._config_version, uuid.UUID))
        self.assertTrue(True, cnxpool._reset_session)

        cnxpool = pooling.MySQLConnectionPool(pool_size=10, pool_name="test")
        self.assertEqual(10, cnxpool._pool_size)

        cnxpool = pooling.MySQLConnectionPool(pool_size=10, **dbconfig)
        self.assertEqual(
            dbconfig,
            cnxpool._cnx_config,
            "Connection configuration not saved correctly",
        )
        self.assertEqual(10, cnxpool._cnx_queue.qsize())
        self.assertTrue(isinstance(cnxpool._config_version, uuid.UUID))

        cnxpool = pooling.MySQLConnectionPool(
            pool_size=1, pool_name="test", pool_reset_session=False
        )
        self.assertFalse(cnxpool._reset_session)

    def test_pool_name(self):
        """Test MySQLConnectionPool.pool_name property"""
        pool_name = "ham"
        cnxpool = pooling.MySQLConnectionPool(pool_name=pool_name)
        self.assertEqual(pool_name, cnxpool.pool_name)

    def test_pool_size(self):
        """Test MySQLConnectionPool.pool_size property"""
        pool_size = 4
        cnxpool = pooling.MySQLConnectionPool(pool_name="test", pool_size=pool_size)
        self.assertEqual(pool_size, cnxpool.pool_size)

    def test_reset_session(self):
        """Test MySQLConnectionPool.reset_session property"""
        cnxpool = pooling.MySQLConnectionPool(
            pool_name="test", pool_reset_session=False
        )
        self.assertFalse(cnxpool.reset_session)
        cnxpool._reset_session = True
        self.assertTrue(cnxpool.reset_session)

    def test__set_pool_size(self):
        cnxpool = pooling.MySQLConnectionPool(pool_name="test")
        self.assertRaises(AttributeError, cnxpool._set_pool_size, -1)
        self.assertRaises(AttributeError, cnxpool._set_pool_size, 0)
        self.assertRaises(
            AttributeError,
            cnxpool._set_pool_size,
            pooling.CNX_POOL_MAXSIZE + 1,
        )

        cnxpool._set_pool_size(pooling.CNX_POOL_MAXSIZE - 1)
        self.assertEqual(pooling.CNX_POOL_MAXSIZE - 1, cnxpool._pool_size)

    def test__set_pool_name(self):
        cnxpool = pooling.MySQLConnectionPool(pool_name="test")

        self.assertRaises(AttributeError, cnxpool._set_pool_name, "pool name")
        self.assertRaises(AttributeError, cnxpool._set_pool_name, "pool%%name")
        self.assertRaises(
            AttributeError,
            cnxpool._set_pool_name,
            "long_pool_name" * pooling.CNX_POOL_MAXNAMESIZE,
        )

    async def test_add_connection(self):
        cnxpool = pooling.MySQLConnectionPool(pool_name="test")
        self.assertRaises(errors.PoolError, cnxpool.add_connection)

        dbconfig = tests.get_mysql_config()
        if tests.MYSQL_VERSION < (5, 7):
            dbconfig["client_flags"] = [-ClientFlag.CONNECT_ARGS]
        cnxpool = pooling.MySQLConnectionPool(pool_size=2, pool_name="test")
        await cnxpool.open()
        await cnxpool.set_config(**dbconfig)

        await cnxpool.add_connection()
        pcnx = pooling.PooledMySQLConnection(
            cnxpool, cnxpool._cnx_queue.get_nowait()
        )
        self.assertTrue(isinstance(pcnx._cnx, MYSQL_CNX_CLASS))
        self.assertEqual(cnxpool, pcnx._cnx_pool)
        self.assertEqual(cnxpool._config_version, pcnx._cnx._pool_config_version)

        cnx = pcnx._cnx
        await pcnx.close()
        # We should get the same connection back
        self.assertEqual(cnx, cnxpool._cnx_queue.get_nowait())
        await cnxpool.add_connection(cnx)

        # reach max connections
        await cnxpool.add_connection()
        with self.assertRaises(errors.PoolError):
            await cnxpool.add_connection()

        # fail connecting
        await cnxpool._remove_connections()
        cnxpool._cnx_config["port"] = 9999999
        cnxpool._cnx_config["unix_socket"] = "/ham/spam/foobar.socket"
        with self.assertRaises(errors.Error):
            await cnxpool.add_connection()

        with self.assertRaises(errors.PoolError):
            await cnxpool.add_connection(cnx=str)

    async def test_set_config(self):
        dbconfig = tests.get_mysql_config()
        if tests.MYSQL_VERSION < (5, 7):
            dbconfig["client_flags"] = [-ClientFlag.CONNECT_ARGS]
        cnxpool = pooling.MySQLConnectionPool(pool_name="test")
        await cnxpool.open()

        # No configuration changes
        config_version = cnxpool._config_version
        await cnxpool.set_config()
        self.assertEqual(config_version, cnxpool._config_version)
        self.assertEqual({}, cnxpool._cnx_config)

        # Valid configuration changes
        config_version = cnxpool._config_version
        await cnxpool.set_config(**dbconfig)
        self.assertEqual(dbconfig, cnxpool._cnx_config)
        self.assertNotEqual(config_version, cnxpool._config_version)

        # Invalid configuration changes
        config_version = cnxpool._config_version
        wrong_dbconfig = dbconfig.copy()
        wrong_dbconfig["spam"] = "ham"
        with self.assertRaises(errors.PoolError):
            await cnxpool.set_config(**wrong_dbconfig)
        self.assertEqual(dbconfig, cnxpool._cnx_config)
        self.assertEqual(config_version, cnxpool._config_version)

    async def test_get_connection(self):
        dbconfig = tests.get_mysql_config()
        if tests.MYSQL_VERSION < (5, 7):
            dbconfig["client_flags"] = [-ClientFlag.CONNECT_ARGS]
        cnxpool = pooling.MySQLConnectionPool(pool_size=2, pool_name="test")
        await cnxpool.open()

        async with self.assertRaises(errors.PoolError):
            await cnxpool.get_connection()

        cnxpool = pooling.MySQLConnectionPool(pool_size=1, **dbconfig)
        await cnxpool.open()

        # Get connection from pool
        pcnx = await cnxpool.get_connection()
        self.assertTrue(isinstance(pcnx, pooling.PooledMySQLConnection))
        with self.assertRaises(errors.PoolError):
            await cnxpool.get_connection()
        self.assertEqual(pcnx._cnx._pool_config_version, cnxpool._config_version)
        prev_config_version = pcnx._pool_config_version
        prev_thread_id = pcnx.connection_id
        await pcnx.close()

        # Change configuration
        config_version = cnxpool._config_version
        await cnxpool.set_config(autocommit=True)
        self.assertNotEqual(config_version, cnxpool._config_version)

        pcnx = await cnxpool.get_connection()
        self.assertNotEqual(pcnx._cnx._pool_config_version, prev_config_version)
        self.assertNotEqual(prev_thread_id, pcnx.connection_id)
        self.assertEqual(1, pcnx.autocommit)
        await pcnx.close()

        # Get connection from pool using a context manager
        with cnxpool.get_connection() as pcnx:
            self.assertTrue(isinstance(pcnx, pooling.PooledMySQLConnection))

    async def test__remove_connections(self):
        dbconfig = tests.get_mysql_config()
        if tests.MYSQL_VERSION < (5, 7):
            dbconfig["client_flags"] = [-ClientFlag.CONNECT_ARGS]
        cnxpool = pooling.MySQLConnectionPool(pool_size=2, pool_name="test", **dbconfig)
        await cnxpool.open()
        pcnx = await cnxpool.get_connection()
        self.assertEqual(1, await cnxpool._remove_connections())
        await pcnx.close()
        self.assertEqual(1, await cnxpool._remove_connections())
        self.assertEqual(0, await cnxpool._remove_connections())

        with self.assertRaises(errors.PoolError):
            await cnxpool.get_connection()
