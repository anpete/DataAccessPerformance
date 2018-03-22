// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Buffers.Binary;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Peregrine.Tests
{
    public class ConnectionTests
    {
        private static readonly ConnectionInfo _connectionInfo
            = new ConnectionInfo(
                "127.0.0.1",
                5432,
                "aspnet5-Benchmarks",
                "postgres",
                "Password1");

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

        [Fact]
        public async Task Execute_query_no_parameters_success()
        {
            var commandFactory = new CommandFactory(new ConnectionPool(_connectionInfo, 32));

            var query
                = commandFactory.CreateQuery(
                    "select id, message from fortune",
                    ShapeFortune);

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
                    ShapeFortune);

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
