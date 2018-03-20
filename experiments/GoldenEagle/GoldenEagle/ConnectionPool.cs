// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace GoldenEagle
{
    public class ConnectionPool : IDisposable
    {
        private readonly ConnectionInfo _connectionInfo;
        private readonly Connection[] _connections;

        public ConnectionPool(ConnectionInfo connectionInfo, int poolSize)
        {
            _connectionInfo = connectionInfo;
            _connections = new Connection[poolSize];
        }

        public int PoolSize => _connections.Length;

        public ValueTask<Connection> Rent()
        {
            for (var i = 0; i < _connections.Length; i++)
            {
                var item = _connections[i];

                if (item != null
                    && Interlocked.CompareExchange(ref _connections[i], null, item) == item)
                {
                    return new ValueTask<Connection>(item);
                }
            }

            return CreateSession();
        }

        private async ValueTask<Connection> CreateSession()
        {
            var session = new Connection(_connectionInfo);

            await session.OpenAsync();

            return session;
        }

        public void Return(Connection session)
        {
            for (var i = 0; i < _connections.Length; i++)
            {
                if (Interlocked.CompareExchange(ref _connections[i], session, null) == null)
                {
                    return;
                }
            }

            session.Dispose();
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
