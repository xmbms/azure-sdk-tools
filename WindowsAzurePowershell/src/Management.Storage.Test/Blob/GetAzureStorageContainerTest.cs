﻿// ----------------------------------------------------------------------------------
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

namespace Microsoft.WindowsAzure.Management.Storage.Test.Blob
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Management.Storage.Blob;
    using Microsoft.WindowsAzure.Management.Storage.Common;
    using Microsoft.WindowsAzure.Management.Storage.Model;
    using Microsoft.WindowsAzure.Management.Storage.Test.Service;
    using Microsoft.WindowsAzure.Management.Test.Tests.Utilities;
    using Microsoft.WindowsAzure.Storage.Blob;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    /// <summary>
    /// unit test for get azure storage container cmdlet
    /// </summary>
    [TestClass]
    public class GetAzureStorageContainerTest : StorageBlobTestBase
    {
        public GetAzureStorageContainerCommand command = null;

        [TestInitialize]
        public void InitCommand()
        {
            command = new GetAzureStorageContainerCommand
            {
                blobClient = blobMock,
                CommandRuntime = new MockCommandRuntime()
            };
        }

        [TestCleanup]
        public void CleanCommand()
        {
            command = null;
        }

        [TestMethod]
        public void ListContainersByNameWithEmptyNameTest()
        {
            //list all the azure container
            List<CloudBlobContainer> containerList = command.ListContainersByName(String.Empty);
            Assert.AreEqual(0, containerList.Count);

            AddTestContainers();

            containerList = command.ListContainersByName(String.Empty);
            Assert.AreEqual(5, containerList.Count);
        }

        [TestMethod]
        public void ListContainersByNameWithWildCardsTest()
        {
            AddTestContainers();

            List<CloudBlobContainer> containerList = command.ListContainersByName("te*t");
            Assert.AreEqual(2, containerList.Count);

            containerList = command.ListContainersByName("tx*t");
            Assert.AreEqual(0, containerList.Count);

            containerList = command.ListContainersByName("t?st");
            Assert.AreEqual(1, containerList.Count);
            Assert.AreEqual("test", containerList[0].Name);
        }

        [TestMethod]
        public void ListContainersByNameWithInvalidNameTest()
        {
            string invalidName = "a";
            AssertThrows<ArgumentException>(() => command.ListContainersByName(invalidName),
                String.Format(Resources.InvalidContainerName, invalidName));
            invalidName = "xx%%d";
            AssertThrows<ArgumentException>(() => command.ListContainersByName(invalidName),
                String.Format(Resources.InvalidContainerName, invalidName));
        }

        [TestMethod]
        public void ListContainersByNameWithContainerNameTest()
        {
            AddTestContainers();
            List<CloudBlobContainer> containerList = command.ListContainersByName("text");
            Assert.AreEqual(1, containerList.Count);
            Assert.AreEqual("text", containerList[0].Name);
        }

        [TestMethod]
        public void ListContainersByNameWithNotExistingContainerTest()
        {
            string notExistingName = "abcdefg";
            AssertThrows<ResourceNotFoundException>(() => command.ListContainersByName(notExistingName),
                String.Format(Resources.ContainerNotFound, notExistingName));
        }

        [TestMethod]
        public void ListContainersByPrefixTest()
        {
            AddTestContainers();

            List<CloudBlobContainer> containerList = command.ListContainersByPrefix("te");
            Assert.AreEqual(2, containerList.Count);
            
            containerList = command.ListContainersByPrefix("tes");
            Assert.AreEqual(1, containerList.Count);
            Assert.AreEqual("test", containerList[0].Name);

            ((MockCommandRuntime)command.CommandRuntime).Clean();
            containerList = command.ListContainersByPrefix("testx");
            Assert.AreEqual(0, containerList.Count);
        }

        [TestMethod]
        public void ListContainerByPrefixWithInvalidPrefixTest()
        {
            ((MockCommandRuntime)command.CommandRuntime).Clean();
            string prefix = "?";
            AssertThrows<ArgumentException>(() => command.ListContainersByPrefix(prefix), String.Format(Resources.InvalidContainerName, prefix));
            prefix = string.Empty;
            AssertThrows<ArgumentException>(() => command.ListContainersByPrefix(prefix), String.Format(Resources.InvalidContainerName, prefix));
        }

        [TestMethod]
        public void WriteContainersWithAclTest()
        {
            command.WriteContainersWithAcl(null);
            Assert.AreEqual(0, ((MockCommandRuntime)command.CommandRuntime).WrittenObjects.Count);

            ((MockCommandRuntime)command.CommandRuntime).Clean();
            command.WriteContainersWithAcl(blobMock.ContainerList);
            Assert.AreEqual(0, ((MockCommandRuntime)command.CommandRuntime).WrittenObjects.Count);

            AddTestContainers();
            ((MockCommandRuntime)command.CommandRuntime).Clean();
            command.WriteContainersWithAcl(blobMock.ContainerList);
            Assert.AreEqual(5, ((MockCommandRuntime)command.CommandRuntime).WrittenObjects.Count);
        }

        [TestMethod]
        public void ExecuteCommandGetContainerTest()
        {
            AddTestContainers();
            command.Name = "test";
            command.ExecuteCommand();
            Assert.AreEqual(1, ((MockCommandRuntime)command.CommandRuntime).WrittenObjects.Count);
        }
    }
}