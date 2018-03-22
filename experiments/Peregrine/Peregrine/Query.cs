// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Peregrine
{
    public class Query<T>
    {
        private readonly ConnectionPool _connectionPool;
        private readonly MemoryPool<byte> _memoryPool = MemoryPool<byte>.Shared;

        private readonly int _id;
        private readonly string _sql;

        private readonly Shaper<T> _shaper;

        public Query(int id, string sql, Shaper<T> shaper, ConnectionPool connectionPool)
        {
            _id = id;
            _sql = sql;
            _shaper = shaper;
            _connectionPool = connectionPool;
        }

        public async Task<List<T>> ToListAsync()
        {
            var connection = await _connectionPool.Rent();
            var ownedMemory = _memoryPool.Rent(minBufferSize: 8192);

            try
            {
                await connection.EnsurePreparedAsync(_id, _sql);
                await connection.ExecuteAsync(_id, ownedMemory.Memory);

                var results = new List<T>();
                var offset = 0;

                read:

                var message = ReadMessage(ownedMemory.Memory, ref offset);

                switch (message)
                {
                    case MessageType.BindComplete:
                        goto read;

                    case MessageType.DataRow:
                    {
                        offset += sizeof(short);

                        results.Add(_shaper(ownedMemory.Memory.Span, ref offset));

                        goto read;
                    }

                    case MessageType.CommandComplete:
                        return results;

                    case MessageType.ErrorResponse:
                        throw new InvalidOperationException(ReadErrorMessage(ownedMemory.Memory, ref offset));

                    default:
                        throw new NotImplementedException(message.ToString());
                }
            }
            finally
            {
                ownedMemory.Release();
                _connectionPool.Return(connection);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static MessageType ReadMessage(in Memory<byte> memory, ref int offset)
        {
            var messageType = (MessageType)memory.Span[offset++];

            // message length
            offset += sizeof(int);

            return messageType;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static string ReadErrorMessage(in Memory<byte> memory, ref int offset)
        {
            string message = null;

            read:

            var code = (ErrorFieldTypeCode)memory.Span[offset++];

            switch (code)
            {
                case ErrorFieldTypeCode.Done:
                    break;
                case ErrorFieldTypeCode.Message:
                    message = ReadNullTerminatedString(memory, ref offset);
                    break;
                default:
                    ReadNullTerminatedString(memory, ref offset);
                    goto read;
            }

            return message;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static string ReadNullTerminatedString(in Memory<byte> memory, ref int offset)
        {
            var start = offset;
            var span = memory.Span;

            while (span[offset++] != 0
                   && offset < memory.Length)
            {
            }

            return PG.UTF8.GetString(span.Slice(start, offset - start - 1));
        }
    }
}
