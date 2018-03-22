// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Buffers.Binary;

namespace Peregrine
{
    public class WriteBuffer
    {
        private const int DefaultBufferSize = 1024;

        private readonly AwaitableSocket _awaitableSocket;

        private readonly Memory<byte> _memory;

        private int _position;
        private int _messageOffset;

        public WriteBuffer(AwaitableSocket awaitableSocket)
        {
            _awaitableSocket = awaitableSocket;
            _memory = new Memory<byte>(new byte[DefaultBufferSize]);
            _position = 0;
            _messageOffset = 0;
        }

        public WriteBuffer StartMessage(char type)
        {
            WriteByte((byte)type);

            return StartMessage();
        }

        public WriteBuffer StartMessage()
        {
            _messageOffset = _position;
            _position += sizeof(int);

            return this;
        }

        public WriteBuffer EndMessage()
        {
            WriteInt(_messageOffset, _position - _messageOffset);

            return this;
        }

        public WriteBuffer WriteByte(byte b)
        {
            _memory.Span[_position++] = b;

            return this;
        }

        public WriteBuffer WriteBytes(byte[] bytes)
        {
            bytes.CopyTo(_memory.Span.Slice(_position));

            _position += bytes.Length;

            return this;
        }

        public WriteBuffer WriteNull() => WriteByte(0);

        public WriteBuffer WriteShort(short s)
        {
            BinaryPrimitives.WriteInt16BigEndian(_memory.Span.Slice(_position), s);

            _position += sizeof(short);

            return this;
        }

        public WriteBuffer WriteInt(int i)
        {
            WriteInt(_position, i);

            _position += sizeof(int);

            return this;
        }

        private void WriteInt(int position, int i)
        {
            BinaryPrimitives.WriteInt32BigEndian(_memory.Span.Slice(position), i);
        }

        public WriteBuffer WriteString(string s)
        {
            _position += PG.UTF8.GetBytes(s.AsSpan(), _memory.Span.Slice(_position));

            return WriteNull();
        }

        public AwaitableSocket FlushAsync()
        {
            _awaitableSocket.SetMemory(_memory.Slice(0, _position));
            _position = 0;

            return _awaitableSocket.SendAsync();
        }
    }
}
