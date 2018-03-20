// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
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

        public Connection(ConnectionInfo connectionInfo)
        {
            _connectionInfo = connectionInfo;
        }

        public bool IsConnected
        {
            get
            {
                ThrowIfDisposed();

                return _socket?.IsConnected == true;
            }
        }

        public Task PrepareAsync(int commandId, string query)
        {
            ThrowIfDisposed();
            ThrowIfNotConnected();

            return _preparedCommandMap[commandId]
                ? Task.CompletedTask
                : PrepareSlow(commandId, query);
        }

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

                var memoryReader = new MemoryReader(ownedMemory.Memory);

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
            finally
            {
                ownedMemory.Release();
            }
        }

        public Task ExecuteAsync<TState, TResult>(
            int commandId,
            TState initialState,
            Func<TState, TResult> resultFactory,
            Action<TResult, MemoryReader, int, int> columnBinder)
        {
            ThrowIfDisposed();
            ThrowIfNotConnected();

            _writeBuffer
                .StartMessage('B')
                .WriteNull()
                .WriteString(commandId.ToString())
                .WriteShort(1)
                .WriteShort(1)
                .WriteShort(0);

            return WriteExecFinishAndProcess(initialState, resultFactory, columnBinder);
        }

        private AwaitableSocket WriteExecFinish()
            => _writeBuffer
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

        private async Task WriteExecFinishAndProcess<TState, TResult>(
            TState initialState,
            Func<TState, TResult> resultFactory,
            Action<TResult, MemoryReader, int, int> columnBinder)
        {
            await WriteExecFinish();

            var ownedMemory = MemoryPool<byte>.Shared.Rent(minBufferSize: 8192);

            try
            {
                _socket.SetMemory(ownedMemory.Memory);

                await _socket.ReceiveAsync();

                var memoryReader = new MemoryReader(ownedMemory.Memory);

                read:

                var message = memoryReader.ReadMessage();

                switch (message)
                {
                    case MessageType.BindComplete:
                        goto read;

                    case MessageType.DataRow:
                    {
                        var result
                            = resultFactory != null
                                ? resultFactory(initialState)
                                : default;

                        var columns = memoryReader.ReadShort();

                        for (var i = 0; i < columns; i++)
                        {
                            var length = memoryReader.ReadInt();

                            columnBinder(result, memoryReader, i, length);
                        }

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
            finally
            {
                ownedMemory.Release();
            }
        }

        public Task StartAsync(int millisecondsTimeout = DefaultConnectionTimeout)
        {
            ThrowIfDisposed();

            return IsConnected
                ? Task.CompletedTask
                : StartSessionAsync(millisecondsTimeout);
        }

        private async Task StartSessionAsync(int millisecondsTimeout)
        {
            await OpenSocketAsync(millisecondsTimeout);

            _writeBuffer = new WriteBuffer(_socket);

            await WriteStartupAsync();

            var ownedMemory = MemoryPool<byte>.Shared.Rent(minBufferSize: 1024);

            try
            {
                _socket.SetMemory(ownedMemory.Memory);

                await _socket.ReceiveAsync();

                var memoryReader = new MemoryReader(ownedMemory.Memory);

                read:

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

                            case AuthenticationRequestType.AuthenticationMD5Password:
                            {
                                var salt = memoryReader.ReadBytes(4);
                                var hash = Hashing.CreateMD5(_connectionInfo.Password, _connectionInfo.User, salt);

                                await _writeBuffer
                                    .StartMessage('p')
                                    .WriteBytes(hash)
                                    .EndMessage()
                                    .FlushAsync();

                                _socket.SetMemory(ownedMemory.Memory);

                                memoryReader.Reset();

                                await _socket.ReceiveAsync();

                                goto read;
                            }

                            default:
                                throw new NotImplementedException(authenticationRequestType.ToString());
                        }
                    }

                    case MessageType.ErrorResponse:
                        throw new InvalidOperationException(memoryReader.ReadErrorMessage());

                    case MessageType.BackendKeyData:
                    case MessageType.EmptyQueryResponse:
                    case MessageType.ParameterStatus:
                    case MessageType.ReadyForQuery:
                        throw new NotImplementedException($"Unhandled MessageType '{message}'");

                    default:
                        throw new InvalidOperationException($"Unexpected MessageType '{message}'");
                }
            }
            finally
            {
                ownedMemory.Release();
            }
        }

        private async Task OpenSocketAsync(int millisecondsTimeout)
        {
            var socket
                = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
                {
                    NoDelay = true
                };

            _socket
                = new AwaitableSocket(
                    new SocketAsyncEventArgs
                    {
                        RemoteEndPoint = new IPEndPoint(IPAddress.Parse(_connectionInfo.Host), _connectionInfo.Port)
                    },
                    socket);

            using (var cts = new CancellationTokenSource())
            {
                cts.CancelAfter(millisecondsTimeout);

                await _socket.ConnectAsync(cts.Token);
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
            if (IsConnected)
            {
                try
                {
                    _writeBuffer
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
