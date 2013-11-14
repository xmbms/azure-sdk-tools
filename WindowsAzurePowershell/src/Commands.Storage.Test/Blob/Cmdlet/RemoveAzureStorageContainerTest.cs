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
    using Storage.Common;

    /// <summary>
    /// unit test for RemoveAzureStorageContainer
    /// </summary>
    [TestClass]
    public class RemoveAzureStorageContainerTest : StorageBlobTestBase
    {
        /// <summary>
        /// faked remove azure container command
        /// </summary>
        internal FakeRemoveAzureContainerCommand command = null;

        [TestInitialize]
        public void InitCommand()
        {
            command = new FakeRemoveAzureContainerCommand(BlobMock)
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
        public void RemoveContainerWithInvalidContainerNameTest()
        {
            string name = "a*b";
            AssertThrowsAsync<ArgumentException>(() => command.RemoveAzureContainer(name, 0),
                String.Format(Resources.InvalidContainerName, name));
        }

        [TestMethod]
        public void RemoveContainerForNotExistsContainerTest()
        {
            string name = "test";
            AssertThrowsAsync<ResourceNotFoundException>(() => command.RemoveAzureContainer(name, 0),
                String.Format(Resources.ContainerNotFound, name));
        }

        [TestMethod]
        public void RemoveContainerCancelledTest()
        {
            AddTestContainers();

            string name = "test";
            MockCmdRunTime.ResetPipelines();
            RunAsyncCommand(command, () => command.RemoveAzureContainer(name, 0).Wait());
            string result = (string) MockCmdRunTime.VerboseStream.FirstOrDefault();
            Assert.AreEqual(String.Format(Resources.RemoveContainerCancelled, name), result);
        }

        [TestMethod]
        public void RemoveContainerSuccessfullyTest()
        {
            AddTestContainers();

            string name = "test";

            MockCmdRunTime.ResetPipelines();
            command.confirm = true;
            RunAsyncCommand(command, () => command.RemoveAzureContainer(name, 0).Wait());
            string result = (string) MockCmdRunTime.VerboseStream.FirstOrDefault();
            Assert.AreEqual(String.Format(Resources.RemoveContainerSuccessfully, name), result);

            MockCmdRunTime.ResetPipelines();
            name = "text";
            command.Force = true;
            command.confirm = false;
            RunAsyncCommand(command, () => command.RemoveAzureContainer(name, 0).Wait());
            result = (string) MockCmdRunTime.VerboseStream.FirstOrDefault();
            Assert.AreEqual(String.Format(Resources.RemoveContainerSuccessfully, name), result);            
        }

        [TestMethod]
        public void ExecuteCommandRemoveContainer()
        {
            string name = "test";
            command.Name = name;
            RunAsyncCommand(command, () => command.ExecuteCmdlet());
            ResourceNotFoundException exception = (ResourceNotFoundException)MockCmdRunTime.ErrorStream.FirstOrDefault().Exception;
            Assert.AreEqual(String.Format(Resources.ContainerNotFound, name), exception.Message);
        }
    }
}
