// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
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
        public async Task Prepare_success()
        {
            using (var session = new Connection(_connectionInfo))
            {
                await session.OpenAsync();

                await session.PrepareAsync(1, "select id, message from fortune");
            }
        }

        [Fact]
        public async Task Execute_query_no_parameters_success()
        {
            using (var session = new Connection(_connectionInfo))
            {
                await session.OpenAsync();
                await session.PrepareAsync(2, "select id, message from fortune");

                var fortunes = new List<Fortune>();

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

                    fortunes.Add(fortune);
                    
                    return fortune;
                }

                await session.ExecuteAsync(2, ShapeFortune);

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
