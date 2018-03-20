// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace GoldenEagle.Tests
{
    public class PGSessionTests
    {
        private static readonly ConnectionInfo _connectionInfo
            = new ConnectionInfo(
                host: "127.0.0.1",
                port: 5432,
                database: "aspnet5-Benchmarks",
                user: "postgres",
                password: "Password1");

        [Fact]
        public async Task Execute_query_no_parameters_success()
        {
            var commandFactory = new CommandFactory(new ConnectionPool(_connectionInfo, 32));

            var query
                = commandFactory.CreateQuery(
                    "select id, message from fortune",
                    r => new Fortune
                    {
                        Id = r.ReadInt(),
                        Message = r.ReadString()
                    });

            var fortunes = await query.ToListAsync();

            Assert.Equal(12, fortunes.Count);
        }

        [Fact]
        public async Task Multi_threaded()
        {
            var commandFactory = new CommandFactory(new ConnectionPool(_connectionInfo, 32));

            var query
                = commandFactory.CreateQuery(
                    "select id, message from fortune",
                    r => new Fortune
                    {
                        Id = r.ReadInt(),
                        Message = r.ReadString()
                    });

            await Task.WhenAll(
                Enumerable
                    .Range(0, 32)
                    .Select(
                        _ => Task.Run(
                            async () =>
                                {
                                    for (var i = 0; i < 1000; i++)
                                    {
                                        var results = await query.ToListAsync();

                                        Assert.Equal(12, results.Count);
                                    }
                                })));
        }

        public class Fortune
        {
            public int Id { get; set; }
            public string Message { get; set; }
        }
    }
}
