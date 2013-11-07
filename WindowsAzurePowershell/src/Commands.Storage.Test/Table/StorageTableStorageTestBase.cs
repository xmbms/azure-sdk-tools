//
// Copyright 2012 Microsoft Corporation
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ----------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Commands.Storage.Test.Table
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage.Table;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using Service;

    public class StorageTableStorageTestBase : StorageTestBase
    {
        public MockStorageTableManagement tableMock = null;

        [TestInitialize]
        public void initMock()
        {
            tableMock = new MockStorageTableManagement();
        }

        [TestCleanup]
        public void CleanMock()
        {
            tableMock = null;
        }

        public void AddTestTables()
        {
            tableMock.tableList.Clear();
            string tableClientUri = "https://127.0.0.1/account/";
            string testUri = "https://127.0.0.1/account/test";
            string textUri = "https://127.0.0.1/account/text";
            CloudTableClient tableClient = new CloudTableClient(new Uri(tableClientUri));
            
            tableMock.tableList.Add(new CloudTable(new Uri(testUri)));
            tableMock.tableList.Add(new CloudTable(new Uri(textUri)));
        }
    }
}
