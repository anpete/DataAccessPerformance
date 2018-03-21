// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Buffers;
using System.Collections;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Peregrine
{
    public class Connection : IDisposable
    {
        private const int DefaultConnectionTimeout = 2000; // ms

        private AwaitableSocket _socket;

        private WriteBuffer _writeBuffer;

        private bool _disposed;

        private readonly ConnectionInfo _connectionInfo;

        private readonly BitArray _preparedCommandMap = new BitArray(length: 100);

        public Connection(in ConnectionInfo connectionInfo)
        {
            _connectionInfo = connectionInfo;
        }

        public Task PrepareAsync(int commandId, string query)
            => _preparedCommandMap[commandId]
                ? Task.CompletedTask
                : PrepareSlow(commandId, query);

        private async Task PrepareSlow(int commandId, string query)
        {
            await _writeBuffer
                .StartMessage('P')
                .WriteString(commandId.ToString())
                .WriteString(query)
                .WriteShort(0)
                .EndMessage()
                .StartMessage('S')
                .EndMessage()
                .FlushAsync();

            var ownedMemory = MemoryPool<byte>.Shared.Rent();

            try
            {
                _socket.SetMemory(ownedMemory.Memory);

                await _socket.ReceiveAsync();

                ParsePrepare(commandId, ownedMemory.Memory);
            }
            finally
            {
                ownedMemory.Release();
            }
        }

        private void ParsePrepare(int commandId, Memory<byte> memory)
        {
            var memoryReader = new MemoryReader(memory);

            var message = memoryReader.ReadMessage();

            switch (message)
            {
                case MessageType.ParseComplete:
                    _preparedCommandMap[commandId] = true;
                    break;

                case MessageType.ErrorResponse:
                    throw new InvalidOperationException(memoryReader.ReadErrorMessage());

                default:
                    throw new NotImplementedException(message.ToString());
            }
        }

        public async Task ExecuteAsync<TResult>(
            int commandId,
            Func<ValueReader, TResult> resultFactory)
        {
            await _writeBuffer
                .StartMessage('B')
                .WriteNull()
                .WriteString(commandId.ToString())
                .WriteShort(1)
                .WriteShort(1)
                .WriteShort(0)
                .WriteShort(1)
                .WriteShort(1)
                .EndMessage()
                .StartMessage('E')
                .WriteNull()
                .WriteInt(0)
                .EndMessage()
                .StartMessage('S')
                .EndMessage()
                .FlushAsync();

            var ownedMemory = MemoryPool<byte>.Shared.Rent(minBufferSize: 8192);

            try
            {
                _socket.SetMemory(ownedMemory.Memory);

                await _socket.ReceiveAsync();

                ParseExec(resultFactory, ownedMemory);
            }
            finally
            {
                ownedMemory.Release();
            }
        }

        private static void ParseExec<TResult>(
            Func<ValueReader, TResult> resultFactory,
            OwnedMemory<byte> ownedMemory)
        {
            var memoryReader = new MemoryReader(ownedMemory.Memory);

            var valueReader = new ValueReader(memoryReader);

            read:

            var message = memoryReader.ReadMessage();

            switch (message)
            {
                case MessageType.BindComplete:
                    goto read;

                case MessageType.DataRow:
                {
                    memoryReader.SkipShort();

                    resultFactory(valueReader);

                    goto read;
                }

                case MessageType.CommandComplete:
                    return;

                case MessageType.ErrorResponse:
                    throw new InvalidOperationException(memoryReader.ReadErrorMessage());

                default:
                    throw new NotImplementedException(message.ToString());
            }
        }

        public async Task StartAsync(int millisecondsTimeout = DefaultConnectionTimeout)
        {
            await OpenSocketAsync(millisecondsTimeout);

            _writeBuffer = new WriteBuffer(_socket);

            await WriteStartupAsync();

            var ownedMemory = MemoryPool<byte>.Shared.Rent(minBufferSize: 1024);

            try
            {
                _socket.SetMemory(ownedMemory.Memory);

                await _socket.ReceiveAsync();

                var salt = ParseStartup(ownedMemory.Memory);

                var hash = Hashing.CreateMD5(_connectionInfo.Password, _connectionInfo.User, salt);

                await _writeBuffer
                    .StartMessage('p')
                    .WriteBytes(hash)
                    .EndMessage()
                    .FlushAsync();

                _socket.SetMemory(ownedMemory.Memory);

                await _socket.ReceiveAsync();

                ParseAuthOk(ownedMemory.Memory);
            }
            finally
            {
                ownedMemory.Release();
            }
        }

        private static byte[] ParseStartup(Memory<byte> memory)
        {
            var memoryReader = new MemoryReader(memory);

            var message = memoryReader.ReadMessage();

            switch (message)
            {
                case MessageType.AuthenticationRequest:
                {
                    var authenticationRequestType
                        = (AuthenticationRequestType)memoryReader.ReadInt();

                    switch (authenticationRequestType)
                    {
                        case AuthenticationRequestType.AuthenticationMD5Password:
                        {
                            return memoryReader.ReadBytes(4); //salt
                        }

                        default:
                            throw new NotImplementedException(authenticationRequestType.ToString());
                    }
                }

                case MessageType.ErrorResponse:
                    throw new InvalidOperationException(memoryReader.ReadErrorMessage());

                default:
                    throw new InvalidOperationException($"Unexpected MessageType '{message}'");
            }
        }

        private static void ParseAuthOk(Memory<byte> memory)
        {
            var memoryReader = new MemoryReader(memory);

            var message = memoryReader.ReadMessage();

            switch (message)
            {
                case MessageType.AuthenticationRequest:
                {
                    var authenticationRequestType
                        = (AuthenticationRequestType)memoryReader.ReadInt();

                    switch (authenticationRequestType)
                    {
                        case AuthenticationRequestType.AuthenticationOk:
                        {
                            return;
                        }

                        default:
                            throw new NotImplementedException(authenticationRequestType.ToString());
                    }
                }

                case MessageType.ErrorResponse:
                    throw new InvalidOperationException(memoryReader.ReadErrorMessage());

                default:
                    throw new InvalidOperationException($"Unexpected MessageType '{message}'");
            }
        }

        private AwaitableSocket OpenSocketAsync(int millisecondsTimeout)
        {
            _socket
                = new AwaitableSocket(
                    new SocketAsyncEventArgs
                    {
                        RemoteEndPoint = new IPEndPoint(IPAddress.Parse(_connectionInfo.Host), _connectionInfo.Port)
                    });

            using (var cts = new CancellationTokenSource())
            {
                cts.CancelAfter(millisecondsTimeout);

                return _socket.ConnectAsync(cts.Token);
            }
        }

        private AwaitableSocket WriteStartupAsync()
        {
            const int protocolVersion3 = 3 << 16;

            var parameters = new (string Name, string Value)[]
            {
                ("user", _connectionInfo.User),
                ("client_encoding", "UTF8"),
                ("database", _connectionInfo.Database)
            };

            _writeBuffer
                .StartMessage()
                .WriteInt(protocolVersion3);

            for (var i = 0; i < parameters.Length; i++)
            {
                var p = parameters[i];

                _writeBuffer
                    .WriteString(p.Name)
                    .WriteString(p.Value);
            }

            return _writeBuffer
                .WriteNull()
                .EndMessage()
                .FlushAsync();
        }

        public void Terminate()
        {
            try
            {
                _writeBuffer?
                    .StartMessage('X')
                    .EndMessage()
                    .FlushAsync()
                    .GetAwaiter()
                    .GetResult();
            }
            catch (SocketException)
            {
                // Socket may have closed
            }

            _socket?.Dispose();
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                Terminate();

                _disposed = true;
            }
        }
    }
}
