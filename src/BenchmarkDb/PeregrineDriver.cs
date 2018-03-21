// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
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

        private static Fortune CreateFortune(List<Fortune> results)
        {
            var fortune = new Fortune();

            results.Add(fortune);

            return fortune;
        }

        private static void BindColumn(Fortune fortune, ValueReader valueReader, int index)
        {
            switch (index)
            {
                case 0:
                    fortune.Id = valueReader.ReadInt();
                    break;
                case 1:
                    fortune.Message = valueReader.ReadString();
                    break;
            }
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

                    //await session.ExecuteAsync(1, results, CreateFortune, BindColumn);

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
