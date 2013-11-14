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

namespace Microsoft.WindowsAzure.Commands.Storage.Test.Blob
{
    using System;
    using System.Linq;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Model.ResourceModel;
    using Storage.Blob.Cmdlet;
    using Storage.Common;

    [TestClass]
    public class NewAzureStorageContainerTest : StorageBlobTestBase
    {
        public NewAzureStorageContainerCommand command = null;

        [TestInitialize]
        public void InitCommand()
        {
            command = new NewAzureStorageContainerCommand(BlobMock)
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
        public void CreateContainerWithInvalidContainerNameTest()
        {
            string name = String.Empty;
            string accesslevel = StorageNouns.ContainerAclOff;

            AssertThrows<ArgumentException>(() => command.CreateAzureContainer(name, accesslevel),
                String.Format(Resources.InvalidContainerName, name));

            name = "a";
            AssertThrows<ArgumentException>(() => command.CreateAzureContainer(name, accesslevel),
                String.Format(Resources.InvalidContainerName, name));

            name = "&*(";
            AssertThrows<ArgumentException>(() => command.CreateAzureContainer(name, accesslevel),
                String.Format(Resources.InvalidContainerName, name));
        }

        [TestMethod]
        public void CreateContainerForAlreadyExistsContainerTest()
        {
            AddTestContainers();
            string name = "text";
            string accesslevel = StorageNouns.ContainerAclOff;

            AssertThrows<ResourceAlreadyExistException>(() => command.CreateAzureContainer(name, accesslevel),
                String.Format(Resources.ContainerAlreadyExists, name));
        }

        [TestMethod]
        public void CreateContainerSuccessfullyTest()
        {
            string name = String.Empty;
            string accesslevel = StorageNouns.ContainerAclOff;

            MockCmdRunTime.ResetPipelines();
            name = "test";
            AzureStorageContainer container = command.CreateAzureContainer(name, accesslevel);
            Assert.AreEqual("test", container.Name);

            MockCmdRunTime.ResetPipelines();
            AssertThrows<ResourceAlreadyExistException>(() => command.CreateAzureContainer(name, accesslevel),
                String.Format(Resources.ContainerAlreadyExists, name));
        }

        [TestMethod]
        public void ExcuteCommandNewContainerTest()
        {
            string name = "containername";
            command.Name = name;
            RunAsyncCommand(command, () => command.ExecuteCmdlet());
            AzureStorageContainer container = (AzureStorageContainer) MockCmdRunTime.OutputPipeline.FirstOrDefault();
            Assert.AreEqual(name, container.Name);
        }
    }
}
