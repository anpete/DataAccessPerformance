// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Threading;

namespace Peregrine
{
    public class CommandFactory
    {
        private readonly ConnectionPool _connectionPool;
        private int _id = -1;

        public CommandFactory(ConnectionPool connectionPool)
            => _connectionPool = connectionPool;

        public Query<T> CreateQuery<T>(string sql, Shaper<T> shaper)
            => new Query<T>(
                Interlocked.Increment(ref _id),
                sql,
                shaper,
                _connectionPool);
    }
}
