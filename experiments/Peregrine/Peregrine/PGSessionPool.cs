// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Peregrine
{
    public class PGSessionPool : IDisposable
    {
        private readonly string _host;
        private readonly int _port;
        private readonly string _database;
        private readonly string _user;
        private readonly string _password;

        private readonly Connection[] _sessions;

        public PGSessionPool(
            string host,
            int port,
            string database,
            string user,
            string password,
            int maximumRetained)
        {
            _host = host;
            _port = port;
            _database = database;
            _user = user;
            _password = password;

            _sessions = new Connection[maximumRetained];
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
            var session = new Connection(new ConnectionInfo(_host, _port, _database, _user, _password));

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
