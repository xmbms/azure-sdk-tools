﻿// ----------------------------------------------------------------------------------
//
// Copyright Microsoft Corporation
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

namespace Microsoft.WindowsAzure.Commands.Storage.Test.Blob.Cmdlet
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Commands.Test.Utilities.Common;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Storage.Blob.Cmdlet;
    using Storage.Common;

    /// <summary>
    /// Unit test for get azure storage container cmdlet
    /// </summary>
    [TestClass]
    public class GetAzureStorageContainerTest : StorageBlobTestBase
    {
        /// <summary>
        /// Get azure storage container command
        /// </summary>
        private GetAzureStorageContainerCommand command = null;

        [TestInitialize]
        public void InitCommand()
        {
            MockCmdRunTime.ResetPipelines();
            command = new GetAzureStorageContainerCommand(BlobMock)
            {
                CommandRuntime = MockCmdRunTime
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
            //List all the azure container
            IEnumerable<CloudBlobContainer> containerList = command.ListContainersByName(String.Empty);
            Assert.IsFalse(containerList.Any());

            AddTestContainers();

            containerList = command.ListContainersByName(String.Empty);
            Assert.AreEqual(5, containerList.Count());
        }

        [TestMethod]
        public void ListContainersByNameWithWildCardsTest()
        {
            AddTestContainers();

            IEnumerable<CloudBlobContainer> containerList = command.ListContainersByName("te*t");
            Assert.AreEqual(2, containerList.Count());

            containerList = command.ListContainersByName("tx*t");
            Assert.IsFalse(containerList.Any());

            containerList = command.ListContainersByName("t?st");
            Assert.AreEqual(1, containerList.Count());
            
            Assert.AreEqual("test", containerList.First().Name);
        }

        [TestMethod]
        public void ListContainersByNameWithInvalidNameTest()
        {
            string invalidName = "a";
            AssertThrows<ArgumentException>(() => command.ListContainersByName(invalidName).ToList(),
                String.Format(Resources.InvalidContainerName, invalidName));
            invalidName = "xx%%d";
            AssertThrows<ArgumentException>(() => command.ListContainersByName(invalidName).ToList(),
                String.Format(Resources.InvalidContainerName, invalidName));
        }

        [TestMethod]
        public void ListContainersByNameWithContainerNameTest()
        {
            AddTestContainers();
            IEnumerable<CloudBlobContainer> containerList = command.ListContainersByName("text");
            Assert.AreEqual(1, containerList.Count());
            Assert.AreEqual("text", containerList.First().Name);
        }

        [TestMethod]
        public void ListContainersByNameWithNotExistingContainerTest()
        {
            string notExistingName = "abcdefg";
            AssertThrows<ResourceNotFoundException>(() => command.ListContainersByName(notExistingName).ToList(),
                String.Format(Resources.ContainerNotFound, notExistingName));
        }

        [TestMethod]
        public void ListContainersByPrefixTest()
        {
            AddTestContainers();

            IEnumerable<CloudBlobContainer> containerList = command.ListContainersByPrefix("te");
            Assert.AreEqual(2, containerList.Count());
            
            containerList = command.ListContainersByPrefix("tes");
            Assert.AreEqual(1, containerList.Count());
            Assert.AreEqual("test", containerList.First().Name);

            ((MockCommandRuntime)command.CommandRuntime).ResetPipelines();
            containerList = command.ListContainersByPrefix("testx");
            Assert.IsFalse(containerList.Any());
        }

        [TestMethod]
        public void ListContainerByPrefixWithInvalidPrefixTest()
        {
            ((MockCommandRuntime)command.CommandRuntime).ResetPipelines();
            string prefix = "?";
            AssertThrows<ArgumentException>(() => command.ListContainersByPrefix(prefix), String.Format(Resources.InvalidContainerName, prefix));
            prefix = string.Empty;
            AssertThrows<ArgumentException>(() => command.ListContainersByPrefix(prefix), String.Format(Resources.InvalidContainerName, prefix));
        }

        [TestMethod]
        public void PackCloudBlobContainerWithAclTest()
        {
            RunAsyncCommand(command, () => command.PackCloudBlobContainerWithAcl(null));
            Assert.IsFalse(MockCmdRunTime.OutputPipeline.Any());

            RunAsyncCommand(command, () => command.PackCloudBlobContainerWithAcl(BlobMock.ContainerList));
            Assert.IsFalse(MockCmdRunTime.OutputPipeline.Any());

            AddTestContainers();
            RunAsyncCommand(command, () => command.PackCloudBlobContainerWithAcl(BlobMock.ContainerList));
            Assert.AreEqual(5, MockCmdRunTime.OutputPipeline.Count());
        }
        
        [TestMethod]
        public void ExecuteCommandGetContainerTest()
        {
            AddTestContainers();
            command.Name = "test";
            RunAsyncCommand(command, () => command.ExecuteCmdlet());
            Assert.AreEqual(1, ((MockCommandRuntime)command.CommandRuntime).OutputPipeline.Count);
        }
    }
}
