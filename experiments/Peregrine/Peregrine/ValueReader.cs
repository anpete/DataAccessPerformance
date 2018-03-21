// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

namespace Peregrine
{
    public struct ValueReader
    {
        private readonly MemoryReader _memoryReader;

        public ValueReader(MemoryReader memoryReader)
        {
            _memoryReader = memoryReader;
        }

        public int ReadInt()
        {
            _memoryReader.SkipInt();

            return _memoryReader.ReadInt();
        }

        public string ReadString()
        {
            var length = _memoryReader.ReadInt();

            return _memoryReader.ReadString(length);
        }
    }
}
