// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;

namespace Peregrine
{
    public delegate TResult Shaper<out TResult>(in ReadOnlySpan<byte> span, ref int offset);
}
