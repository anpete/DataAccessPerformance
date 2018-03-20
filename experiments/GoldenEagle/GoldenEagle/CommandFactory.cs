// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace GoldenEagle
{
    public class CommandFactory
    {
        private readonly ConnectionPool _connectionPool;
        private int _id = -1;

        public CommandFactory(ConnectionPool connectionPool)
            => _connectionPool = connectionPool;

        public Query<T> CreateQuery<T>(string sql, Func<ValueReader, T> shaper)
            => new Query<T>(
                Interlocked.Increment(ref _id),
                sql,
                shaper,
                _connectionPool);
    }

    public readonly struct ValueReader
    {
        private readonly ReadBuffer _readBuffer;

        public ValueReader(ReadBuffer readBuffer)
            => _readBuffer = readBuffer;

        public int ReadInt()
        {
            _readBuffer.SkipInt();

            return _readBuffer.ReadInt();
        }

        public string ReadString()
        {
            var length = _readBuffer.ReadInt();

            return _readBuffer.ReadString(length);
        }
    }

    public class Query<T>
    {
        private readonly int _id;
        private readonly string _sql;
        private readonly Func<ValueReader, T> _shaper;
        private readonly ConnectionPool _connectionPool;

        private Task<ReadBuffer> _activeTask;
        private Connection _activeConnection;

        private int _awaiters;

        public Query(int id, string sql, Func<ValueReader, T> shaper, ConnectionPool connectionPool)
        {
            _id = id;
            _sql = sql;
            _shaper = shaper;
            _connectionPool = connectionPool;
        }

        public async Task<List<T>> ToListAsync()
        {
            var activeTask = _activeTask;

            if (activeTask != null)
            {
                Interlocked.Increment(ref _awaiters);

                var readBuffer = new ReadBuffer((await activeTask).Buffer);

                if (Interlocked.Decrement(ref _awaiters) == 0)
                {
                    _connectionPool.Return(_activeConnection);
                }

                return Read(readBuffer);
            }

            var connection = await _connectionPool.Rent();

            try
            {
                _activeConnection = connection;
                _activeTask = Read(connection);

                var readBuffer = await _activeTask;

                return Read(readBuffer);
            }
            finally
            {
                _activeTask = null;

                if (_awaiters == 0)
                {
                    _connectionPool.Return(connection);
                }
            }
        }

        private async Task<ReadBuffer> Read(Connection connection)
        {
            await connection.EnsurePreparedAsync(_id, _sql);
            await connection.ExecuteAsync(_id);

            var readBuffer = connection.ReadBuffer;

            await readBuffer.ReceiveAsync();

            return readBuffer;
        }

        private List<T> Read(ReadBuffer readBuffer)
        {
            var valueReader = new ValueReader(readBuffer);

            var results = new List<T>();

            read:

            var message = readBuffer.ReadMessage();

            switch (message)
            {
                case MessageType.BindComplete:
                    goto read;

                case MessageType.DataRow:
                {
                    readBuffer.SkipShort();

                    results.Add(_shaper(valueReader));

                    goto read;
                }

                case MessageType.CommandComplete:
                    return results;

                case MessageType.ErrorResponse:
                    throw new InvalidOperationException(readBuffer.ReadErrorMessage());

//                case MessageType.ParameterStatus:
//                    goto read;
//
//                case MessageType.RowDescription:
//                    goto read;
//
//                case MessageType.ReadyForQuery:
//                    goto read;
//
//                case MessageType.EmptyQueryResponse:
//                    goto read;

                default:
                    goto read;
                   // throw new NotImplementedException(message.ToString());
            }
        }
    }
}
