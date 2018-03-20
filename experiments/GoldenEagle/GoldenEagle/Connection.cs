// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace GoldenEagle
{
    public sealed class Connection : IDisposable
    {
        private const int DefaultConnectionTimeout = 2000; // ms
        private const int MaxPreparedCommands = 32;

        private readonly ConnectionInfo _connectionInfo;
        private readonly BitArray _preparedCommandMap = new BitArray(MaxPreparedCommands);

        private WriteBuffer _writeBuffer;

        private AwaitableSocket _awaitableSocket;

        private bool _disposed;

        public Connection(ConnectionInfo connectionInfo) 
            => _connectionInfo = connectionInfo;

        public ReadBuffer ReadBuffer { get; private set; }

        public Task EnsurePreparedAsync(int id, string query) 
            => _preparedCommandMap[id]
            ? Task.CompletedTask
            : PrepareAsync(id, query);

        private async Task PrepareAsync(int id, string query)
        {
            await _writeBuffer
                .StartMessage('P')
                .WriteString(id.ToString())
                .WriteString(query)
                .WriteShort(0)
                .EndMessage()
                .StartMessage('S')
                .EndMessage()
                .FlushAsync();

            await ReadBuffer.ReceiveAsync();

            var message = ReadBuffer.ReadMessage();

            switch (message)
            {
                case MessageType.ParseComplete:
                    _preparedCommandMap[id] = true;
                    break;

                case MessageType.ErrorResponse:
                    throw new InvalidOperationException(ReadBuffer.ReadErrorMessage());

                default:
                    throw new NotImplementedException(message.ToString());
            }
        }

        public AwaitableSocket ExecuteAsync(int id)
            => _writeBuffer
                .StartMessage('B')
                .WriteNull()
                .WriteString(id.ToString())
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

        public async Task OpenAsync(int millisecondsTimeout = DefaultConnectionTimeout)
        {
            await OpenSocketAsync(millisecondsTimeout);

            _writeBuffer = new WriteBuffer(_awaitableSocket);
            ReadBuffer = new ReadBuffer(_awaitableSocket);

            await WriteStartupAsync();

            await ReadBuffer.ReceiveAsync();

            read:

            var message = ReadBuffer.ReadMessage();

            switch (message)
            {
                case MessageType.AuthenticationRequest:
                {
                    var authenticationRequestType
                        = (AuthenticationRequestType)ReadBuffer.ReadInt();

                    switch (authenticationRequestType)
                    {
                        case AuthenticationRequestType.AuthenticationOk:
                        {
                            return;
                        }

                        case AuthenticationRequestType.AuthenticationMD5Password:
                        {
                            var salt = ReadBuffer.ReadBytes(4);
                            var hash = Hashing.CreateMD5(_connectionInfo.Password, _connectionInfo.User, salt);

                            await _writeBuffer
                                .StartMessage('p')
                                .WriteBytes(hash)
                                .EndMessage()
                                .FlushAsync();

                            await ReadBuffer.ReceiveAsync();

                            goto read;
                        }

                        default:
                            throw new NotImplementedException(authenticationRequestType.ToString());
                    }
                }

                case MessageType.ErrorResponse:
                    throw new InvalidOperationException(ReadBuffer.ReadErrorMessage());

                case MessageType.BackendKeyData:
                case MessageType.EmptyQueryResponse:
                case MessageType.ParameterStatus:
                case MessageType.ReadyForQuery:
                    throw new NotImplementedException($"Unhandled MessageType '{message}'");

                default:
                    throw new InvalidOperationException($"Unexpected MessageType '{message}'");
            }
        }

        private AwaitableSocket OpenSocketAsync(int millisecondsTimeout)
        {
            var socket
                = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
                {
                    NoDelay = true
                };

            _awaitableSocket
                = new AwaitableSocket(
                    new SocketAsyncEventArgs
                    {
                        RemoteEndPoint = new IPEndPoint(IPAddress.Parse(_connectionInfo.Host), _connectionInfo.Port)
                    },
                    socket);

            using (var cts = new CancellationTokenSource())
            {
                cts.CancelAfter(millisecondsTimeout);

                return _awaitableSocket.ConnectAsync(cts.Token);
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

            _awaitableSocket?.Dispose();
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
