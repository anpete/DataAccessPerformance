// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Threading.Tasks;
using GoldenEagle;
using Npgsql;

namespace BenchmarkDb
{
    public sealed class GoldenEagleDriver : DriverBase, IDisposable
    {
        private ConnectionPool _connectionPool;
        private Query<Fortune> _query;

        public override Func<Task> TryGetVariation(string variationName)
        {
            switch (variationName)
            {
                case Variation.AsyncCaching:
                    return DoWorkAsyncCaching;
                default:
                    return NotSupportedVariation;
            }
        }

        public override void Initialize(string connectionString, int threadCount)
        {
            var connectionStringBuilder
                = new NpgsqlConnectionStringBuilder(connectionString);

            var connectionInfo
                = new ConnectionInfo(
                    connectionStringBuilder.Host,
                    connectionStringBuilder.Port,
                    connectionStringBuilder.Database,
                    connectionStringBuilder.Username,
                    connectionStringBuilder.Password);

            _connectionPool = new ConnectionPool(connectionInfo, threadCount);

            var commandFactory = new CommandFactory(_connectionPool);

            _query
                = commandFactory.CreateQuery(
                    "select id, message from fortune",
                    r => new Fortune
                    {
                        Id = r.ReadInt(),
                        Message = r.ReadString()
                    });
        }

        public override async Task DoWorkAsyncCaching()
        {
            while (Program.IsRunning)
            {
                var results = await _query.ToListAsync();

                CheckResults(results);

                Program.IncrementCounter();
            }
        }

        public void Dispose()
        {
            _connectionPool?.Dispose();
        }
    }
}
