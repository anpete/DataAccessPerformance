// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using Peregrine;

namespace BenchmarkDb
{
    public sealed class PeregrineDriver : DriverBase, IDisposable
    {
        private ConnectionPool _sessionPool;

        public override Func<Task> TryGetVariation(string variationName)
        {
            switch (variationName)
            {
                case Variation.Async:
                    return DoWorkAsync;
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

            _sessionPool
                = new ConnectionPool(
                    new ConnectionInfo(
                        connectionStringBuilder.Host,
                        connectionStringBuilder.Port,
                        connectionStringBuilder.Database,
                        connectionStringBuilder.Username,
                        connectionStringBuilder.Password),
                    threadCount)
                {
                    OnCreate = async s => await s.PrepareAsync(1, "select id, message from fortune")
                };
        }

        

        public override async Task DoWorkAsync()
        {
            while (Program.IsRunning)
            {
                var results = new List<Fortune>();

                var session = await _sessionPool.Rent();

                try
                {
                    //await session.ExecuteAsync(1, results, CreateFortune, BindColumn);
                }
                finally
                {
                    _sessionPool.Return(session);
                }

                CheckResults(results);

                Program.IncrementCounter();
            }
        }

        public override async Task DoWorkAsyncCaching()
        {
            var session = await _sessionPool.Rent();

            try
            {
                while (Program.IsRunning)
                {
                    var results = new List<Fortune>();

                    Fortune ShapeFortune(in ReadOnlySpan<byte> span, ref int offset)
                    {
                        var fortune = new Fortune();

                        offset += 4;
                        fortune.Id = BinaryPrimitives.ReadInt32BigEndian(span.Slice(offset, 4));
                        offset += 4;
            
                        var length = BinaryPrimitives.ReadInt32BigEndian(span.Slice(offset, 4));
                        offset += 4;
                        fortune.Message = PG.UTF8.GetString(span.Slice(offset, length));
                        offset += length;

                        results.Add(fortune);
                    
                        return fortune;
                    }

                    await session.ExecuteAsync(1, ShapeFortune);

                    CheckResults(results);

                    Program.IncrementCounter();
                }
            }
            finally
            {
                _sessionPool.Return(session);
            }
        }

        public void Dispose()
        {
            _sessionPool?.Dispose();
        }
    }
}
