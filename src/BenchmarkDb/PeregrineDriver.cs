// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Buffers.Binary;
using System.Threading.Tasks;
using Npgsql;
using Peregrine;

namespace BenchmarkDb
{
    public sealed class PeregrineDriver : DriverBase, IDisposable
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
                    ShapeFortune);
        }

        private Fortune ShapeFortune(in ReadOnlySpan<byte> span, ref int offset)
        {
            var fortune = new Fortune();

            offset += 4;
            fortune.Id = BinaryPrimitives.ReadInt32BigEndian(span.Slice(offset, 4));
            offset += 4;

            var length = BinaryPrimitives.ReadInt32BigEndian(span.Slice(offset, 4));
            offset += 4;
            fortune.Message = PG.UTF8.GetString(span.Slice(offset, length));
            offset += length;

            return fortune;
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
