// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Net.Sockets;
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

        [Fact]
        public async Task Start_timeout_on_bad_host()
        {
            using (var session = new Connection(new ConnectionInfo("1.2.3.4", 5432, "aspnet5-Benchmarks", "postgres", "Password1")))
            {
                await Assert.ThrowsAsync<SocketException>(() => session.StartAsync());
            }
        }

        [Fact]
        public async Task Start_fail_on_bad_port()
        {
            using (var session = new Connection(new ConnectionInfo("127.0.0.1", 2345, "aspnet5-Benchmarks", "postgres", "Password1")))
            {
                await Assert.ThrowsAnyAsync<SocketException>(() => session.StartAsync());
            }
        }

        [Fact]
        public async Task Start_fail_bad_user()
        {
            using (var session = new Connection(new ConnectionInfo("127.0.0.1", 5432, "aspnet5-Benchmarks", "Bad!", "Password1")))
            {
                Assert.Equal(
                    "password authentication failed for user \"Bad!\"",
                    (await Assert.ThrowsAsync<InvalidOperationException>(
                        () => session.StartAsync())).Message);
            }
        }

        [Fact]
        public async Task Start_fail_bad_password()
        {
            using (var session = new Connection(new ConnectionInfo("127.0.0.1", 5432, "aspnet5-Benchmarks", "postgres", "wrong")))
            {
                Assert.Equal(
                    "password authentication failed for user \"postgres\"",
                    (await Assert.ThrowsAsync<InvalidOperationException>(
                        () => session.StartAsync())).Message);
            }
        }

        [Fact]
        public async Task Prepare_success()
        {
            using (var session = new Connection(_connectionInfo))
            {
                await session.StartAsync();

                await session.PrepareAsync(1, "select id, message from fortune");
            }
        }

        [Fact]
        public async Task Prepare_failure_invalid_sql()
        {
            using (var session = new Connection(_connectionInfo))
            {
                await session.StartAsync();

                Assert.Equal(
                    "syntax error at or near \"boom\"",
                    (await Assert.ThrowsAsync<InvalidOperationException>(
                        () => session.PrepareAsync(1, "boom!"))).Message);
            }
        }

        [Fact]
        public async Task Execute_query_no_parameters_success()
        {
            using (var session = new Connection(_connectionInfo))
            {
                await session.StartAsync();
                await session.PrepareAsync(2, "select id, message from fortune");

                var fortunes = new List<Fortune>();

                Fortune CreateFortune(ValueReader valueReader)
                {
                    var fortune = new Fortune
                    {
                        Id = valueReader.ReadInt(),
                        Message = valueReader.ReadString()
                    };

                    fortunes.Add(fortune);

                    return fortune;
                }

                await session.ExecuteAsync(2, CreateFortune);

                Assert.Equal(12, fortunes.Count);
            }
        }

        // [Fact]
        // public async Task Execute_query_parameter_success()
        // {
        //     using (var session = new Connection(new ConnectionInfo(Host, Port, Database, User, Password)))
        //     {
        //         await session.StartAsync();
        //         await session.PrepareAsync(2, "select id, randomnumber from world where id = $1");

        //  //         World world = null;

        //  //         World CreateWorld()
        //         {
        //             world = new World();

        //  //             return world;
        //         }

        //  //         void BindColumn(World w, MemoryReader readBuffer, int index, int _)
        //         {
        //             switch (index)
        //             {
        //                 case 0:
        //                     w.Id = readBuffer.ReadInt();
        //                     break;
        //                 case 1:
        //                     w.RandomNumber = readBuffer.ReadInt();
        //                     break;
        //             }
        //         }

        //  //         await session.ExecuteAsync(2, CreateWorld, BindColumn, 45);

        //  //         Assert.NotNull(world);
        //         Assert.Equal(45, world.Id);
        //         Assert.InRange(world.RandomNumber, 1, 10000);
        //     }
        // }

        public class Fortune
        {
            public int Id { get; set; }
            public string Message { get; set; }
        }

        public class World
        {
            public int Id { get; set; }
            public int RandomNumber { get; set; }
        }
    }
}
