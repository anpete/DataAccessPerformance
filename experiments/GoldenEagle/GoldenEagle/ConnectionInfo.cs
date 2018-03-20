// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

namespace GoldenEagle
{
    public class ConnectionInfo
    {
        public ConnectionInfo(string host, int port, string database, string user, string password)
        {
            Host = host;
            Port = port;
            Database = database;
            User = user;
            Password = password;
        }

        public string Host { get; }
        public int Port { get; }
        public string Database { get; }
        public string User { get; }
        public string Password { get; }
    }
}
