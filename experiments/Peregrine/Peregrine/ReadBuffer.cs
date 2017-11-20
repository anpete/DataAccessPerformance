// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;

namespace Peregrine
{
    public class ReadBuffer
    {
        private const int DefaultBufferSize = 8192;

        private readonly AwaitableSocket _awaitableSocket;

        private readonly byte[] _buffer = new byte[DefaultBufferSize];

        private int _position;

        internal ReadBuffer(AwaitableSocket awaitableSocket)
        {
            _awaitableSocket = awaitableSocket;
        }

        public byte ReadByte()
        {
            return _buffer[_position++];
        }

        public byte[] ReadBytes(int length)
        {
            var bs = new byte[length];

            Buffer.BlockCopy(_buffer, _position, bs, 0, length);

            _position += length;

            return bs;
        }

        public short ReadShort()
        {
            var s = IPAddress.NetworkToHostOrder(BitConverter.ToInt16(_buffer, _position));

            _position += sizeof(short);

            return s;
        }

        public ushort ReadUShort()
        {
            var us = (ushort)IPAddress.NetworkToHostOrder(BitConverter.ToUInt16(_buffer, _position));

            _position += sizeof(ushort);

            return us;
        }

        public int ReadInt()
        {
            var i = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(_buffer, _position));

            _position += sizeof(int);

            return i;
        }

        public uint ReadUInt()
        {
            var ui = (uint)IPAddress.NetworkToHostOrder(BitConverter.ToUInt32(_buffer, _position));

            _position += sizeof(uint);

            return ui;
        }

        public string ReadNullTerminatedString()
        {
            var start = _position;

            while (_buffer[_position++] != 0
                   && _position < _buffer.Length)
            {
            }

            var s = PG.UTF8.GetString(_buffer, start, _position - start - 1);

            return s;
        }

        private class CachedStringKeyEqualityComparer : IEqualityComparer<CachedStringKey>
        {
            public static readonly CachedStringKeyEqualityComparer Instance = new CachedStringKeyEqualityComparer();

            private CachedStringKeyEqualityComparer()
            {
            }

            public bool Equals(CachedStringKey x, CachedStringKey y)
            {
                if (x.Length != y.Length)
                {
                    return false;
                }

                var a1 = x.Buffer;
                var a2 = y.Buffer;

                unsafe
                {
                    fixed (byte* p1 = a1, p2 = a2)
                    {
                        byte* x1 = p1, x2 = p2 + y.Position;

                        var l = a1.Length;

                        for (var i = 0; i < l / 8; i++, x1 += 8, x2 += 8)
                        {
                            if (*((long*)x1) != *((long*)x2))
                            {
                                return false;
                            }
                        }

                        if ((l & 4) != 0)
                        {
                            if (*((int*)x1) != *((int*)x2))
                            {
                                return false;
                            }

                            x1 += 4;
                            x2 += 4;
                        }

                        if ((l & 2) != 0)
                        {
                            if (*((short*)x1) != *((short*)x2))
                            {
                                return false;
                            }

                            x1 += 2;
                            x2 += 2;
                        }

                        if ((l & 1) != 0)
                        {
                            if (*x1 != *x2)
                            {
                                return false;
                            }
                        }

                        return true;
                    }
                }
            }

            public int GetHashCode(CachedStringKey obj)
            {
                unchecked
                {
                    var hash = 0;

                    for (var i = obj.Position; i < obj.Position + obj.Length; i++)
                    {
                        hash = (hash * 31) ^ obj.Buffer[i];
                    }

                    return hash;
                }
            }
        }

        private struct CachedStringKey
        {
            public readonly byte[] Buffer;
            public readonly int Position;
            public readonly int Length;

            public CachedStringKey(byte[] buffer, int position, int length)
            {
                Buffer = buffer;
                Position = position;
                Length = length;
            }

            public override bool Equals(object obj)
            {
                throw new NotImplementedException();
            }

            public override int GetHashCode()
            {
                throw new NotImplementedException();
            }
        }

        private readonly Dictionary<CachedStringKey, string> _cache
            = new Dictionary<CachedStringKey, string>(CachedStringKeyEqualityComparer.Instance);

        public string ReadString(int length)
        {
            if (!_cache.TryGetValue(new CachedStringKey(_buffer, _position, length), out var s))
            {
                s = PG.UTF8.GetString(_buffer, _position, length);

                var bs = new byte[length];

                Buffer.BlockCopy(_buffer, _position, bs, 0, length);

                _cache.Add(new CachedStringKey(bs, 0, length), s);
            }

            _position += length;

            return s;
        }

        public async Task ReceiveAsync()
        {
            _awaitableSocket.SetBuffer(_buffer, 0, _buffer.Length);

            await _awaitableSocket.ReceiveAsync();

            var bytesTransferred = _awaitableSocket.BytesTransferred;

            if (bytesTransferred == 0)
            {
                throw new EndOfStreamException();
            }

            _position = 0;
        }
    }
}
