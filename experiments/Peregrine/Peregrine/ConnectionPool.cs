// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Peregrine
{
    public class ConnectionPool : IDisposable
    {
        private readonly ConnectionInfo _connectionInfo;
        private readonly Connection[] _connections;

        public ConnectionPool(in ConnectionInfo connectionInfo, int maxPoolSize)
        {
            _connectionInfo = connectionInfo;
            _connections = new Connection[maxPoolSize];
        }

        public ValueTask<Connection> Rent()
        {
            for (var i = 0; i < _connections.Length; i++)
            {
                var item = _connections[i];

                if (item != null
                    && Interlocked.CompareExchange(ref _connections[i], value: null, item) == item)
                {
                    return new ValueTask<Connection>(item);
                }
            }

            return CreateConnection();
        }

        private async ValueTask<Connection> CreateConnection()
        {
            var session = new Connection(in _connectionInfo);

            await session.OpenAsync();

            return session;
        }

        public void Return(Connection connection)
        {
            for (var i = 0; i < _connections.Length; i++)
            {
                if (Interlocked.CompareExchange(ref _connections[i], connection, comparand: null) == null)
                {
                    return;
                }
            }

            connection.Dispose();
        }

        public void Dispose()
        {
            for (var i = 0; i < _connections.Length; i++)
            {
                _connections[i]?.Dispose();
            }
        }
    }
}
