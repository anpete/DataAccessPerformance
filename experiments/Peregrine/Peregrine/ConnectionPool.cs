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
        private readonly Connection[] _sessions;

        public ConnectionPool(in ConnectionInfo connectionInfo, int maxPoolSize)
        {
            _connectionInfo = connectionInfo;
            _sessions = new Connection[maxPoolSize];
        }

        public Func<Connection, Task> OnCreate { get; set; }

        public ValueTask<Connection> Rent()
        {
            for (var i = 0; i < _sessions.Length; i++)
            {
                var item = _sessions[i];

                if (item != null
                    && Interlocked.CompareExchange(ref _sessions[i], null, item) == item)
                {
                    return new ValueTask<Connection>(item);
                }
            }

            return CreateSession();
        }

        private async ValueTask<Connection> CreateSession()
        {
            var session = new Connection(in _connectionInfo);

            await session.StartAsync();

            if (OnCreate != null)
            {
                await OnCreate.Invoke(session);
            }

            return session;
        }

        public void Return(Connection session)
        {
            for (var i = 0; i < _sessions.Length; i++)
            {
                if (Interlocked.CompareExchange(ref _sessions[i], session, null) == null)
                {
                    return;
                }
            }

            session.Dispose();
        }

        public void Dispose()
        {
            for (var i = 0; i < _sessions.Length; i++)
            {
                _sessions[i]?.Dispose();
            }
        }
    }
}
