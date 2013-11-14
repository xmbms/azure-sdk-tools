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
    using Commands.Test.Utilities.Common;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Model.ResourceModel;
    using Storage.Cmdlet;
    using Storage.Common;

    /// <summary>
    /// unit test for SetAzureStorageContainer
    /// </summary>
    [TestClass]
    public class SetAzureStorageContainerAclTest : StorageBlobTestBase
    {
        public SetAzureStorageContainerAclCommand command = null;

        [TestInitialize]
        public void InitCommand()
        {
            command = new SetAzureStorageContainerAclCommand(BlobMock)
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
        public void SetContainerAclWithInvalidContainerNameTest()
        {
            string name = "a";
            string accessLevel = StorageNouns.ContainerAclOff;
            AssertThrowsAsync<ArgumentException>(() => command.SetContainerAcl(name, accessLevel, 0), String.Format(Resources.InvalidContainerName, name));
        }

        [TestMethod]
        public void SetContainerAclWithEmptyAccessLevel()
        {
            string name = "test";
            string accessLevel = String.Empty;
            AssertThrowsAsync<ArgumentException>(() => command.SetContainerAcl(name, accessLevel, 0), Resources.OnlyOnePermissionForContainer);
        }

        [TestMethod]
        public void SetContainerAclForNotExistContainer()
        {
            string name = "test";
            string accessLevel = StorageNouns.ContainerAclOff;
            AssertThrowsAsync<ResourceNotFoundException>(() => command.SetContainerAcl(name, accessLevel, 0), String.Format(Resources.ContainerNotFound, name));
        }

        [TestMethod]
        public void SetContainerAclSucessfullyTest()
        {
            AddTestContainers();
            command.PassThru = true;

            string name = "test";
            string accessLevel = StorageNouns.ContainerAclOff;

            ((MockCommandRuntime)command.CommandRuntime).ResetPipelines();
            RunAsyncCommand(command, () => command.SetContainerAcl(name, accessLevel, 0).Wait());
            AzureStorageContainer container = (AzureStorageContainer) MockCmdRunTime.OutputPipeline.FirstOrDefault();
            Assert.AreEqual(BlobContainerPublicAccessType.Off, container.PublicAccess);

            ((MockCommandRuntime)command.CommandRuntime).ResetPipelines();
            name = "publicoff";
            accessLevel = StorageNouns.ContainerAclBlob;
            RunAsyncCommand(command, () => command.SetContainerAcl(name, accessLevel, 0).Wait());
            container = (AzureStorageContainer) MockCmdRunTime.OutputPipeline.FirstOrDefault();
            Assert.AreEqual(BlobContainerPublicAccessType.Blob, container.PublicAccess);

            ((MockCommandRuntime)command.CommandRuntime).ResetPipelines();
            name = "publicblob";
            accessLevel = StorageNouns.ContainerAclContainer;
            RunAsyncCommand(command, () => command.SetContainerAcl(name, accessLevel, 0).Wait());
            container = (AzureStorageContainer) MockCmdRunTime.OutputPipeline.FirstOrDefault();
            Assert.AreEqual(BlobContainerPublicAccessType.Container, container.PublicAccess);

            ((MockCommandRuntime)command.CommandRuntime).ResetPipelines();
            name = "publiccontainer";
            accessLevel = StorageNouns.ContainerAclOff;
            RunAsyncCommand(command, () => command.SetContainerAcl(name, accessLevel, 0).Wait());
            container = (AzureStorageContainer) MockCmdRunTime.OutputPipeline.FirstOrDefault();
            Assert.AreEqual(BlobContainerPublicAccessType.Off, container.PublicAccess);
        }

        [TestMethod]
        public void ExecuteCommandSetContainerAclTest()
        {
            AddTestContainers();
            command.Name = "publicblob";
            command.Permission = "container";
            command.PassThru = true;
            RunAsyncCommand(command, ()=>command.ExecuteCmdlet());
            AzureStorageContainer container = (AzureStorageContainer) MockCmdRunTime.OutputPipeline.FirstOrDefault();
            Assert.AreEqual(BlobContainerPublicAccessType.Container, container.PublicAccess);
        }
    }
}
