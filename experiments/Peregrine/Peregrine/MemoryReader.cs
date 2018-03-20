// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace Peregrine
{
    public class MemoryReader
    {
        private readonly Memory<byte> _memory;

        private int _position;

        public MemoryReader(Memory<byte> memory)
        {
            _memory = memory;
            _position = 0;
        }

        public void Reset() => _position = 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal MessageType ReadMessage()
        {
            var messageType = (MessageType)ReadByte();

            // Skip length
            _position += sizeof(int);

            return messageType;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal string ReadErrorMessage()
        {
            string message = null;

            read:

            var code = (ErrorFieldTypeCode)ReadByte();

            switch (code)
            {
                case ErrorFieldTypeCode.Done:
                    break;
                case ErrorFieldTypeCode.Message:
                    message = ReadNullTerminatedString();
                    break;
                default:
                    ReadNullTerminatedString();
                    goto read;
            }

            return message;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte ReadByte()
            => _memory.Span[_position++];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte[] ReadBytes(int length)
            => _memory.Span.Slice(_position, length).ToArray();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public short ReadShort()
        {
            var result = BinaryPrimitives.ReadInt16BigEndian(_memory.Span.Slice(_position, 2));

            _position += sizeof(short);

            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ReadInt()
        {
            var result = BinaryPrimitives.ReadInt32BigEndian(_memory.Span.Slice(_position, 4));

            _position += sizeof(int);

            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string ReadNullTerminatedString()
        {
            var start = _position;
            var span = _memory.Span;

            while (span[_position++] != 0
                   && _position < _memory.Length)
            {
            }

            return PG.UTF8.GetString(span.Slice(start, _position - start - 1));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string ReadString(int length)
        {
            var result = PG.UTF8.GetString(_memory.Span.Slice(_position, length));

            _position += length;

            return result;
        }
    }
}
