﻿﻿// ----------------------------------------------------------------------------------
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
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Commands.Test.Utilities.Common;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Model.ResourceModel;
    using Storage.Common;

    [TestClass]
    public class StorageCloudBlobCmdletBaseTest : StorageBlobTestBase
    {
        /// <summary>
        /// StorageCloudBlobCmdletBase command
        /// </summary>
        public StorageCloudBlobCmdletBase command = null;

        [TestInitialize]
        public void InitCommand()
        {
            command = new StorageCloudBlobCmdletBase(BlobMock)
                {
                    Context = new AzureStorageContext(CloudStorageAccount.DevelopmentStorageAccount),
                    CommandRuntime = new MockCommandRuntime()
                };
        }

        [TestCleanup]
        public void CleanCommand()
        {
            command = null;
        }

        [TestMethod]
        public void ValidatePipelineICloudBlobWithNullTest()
        {
            AssertThrows<ArgumentException>(() => command.ValidatePipelineICloudBlob(null), String.Format(Resources.ObjectCannotBeNull, typeof(ICloudBlob).Name));
        }

        [TestMethod]
        public void ValidatePipelineICloudBlobWithInvalidBlobNameTest()
        {
            CloudBlockBlob blob = new CloudBlockBlob(new Uri("http://127.0.0.1/account/container/"));
            AssertThrows<ArgumentException>(() => command.ValidatePipelineICloudBlob(blob), String.Format(Resources.InvalidBlobName, blob.Name));
        }

        [TestMethod]
        public void ValidatePipelineICloudBlobExitsTest()
        {
            AddTestContainers();
            CloudBlockBlob blob = new CloudBlockBlob(new Uri("http://127.0.0.1/account/test/blob"));
            AssertThrows<ResourceNotFoundException>(() => command.ValidatePipelineICloudBlob(blob), String.Format(Resources.BlobNotFound, blob.Name, blob.Container.Name));
        }

        [TestMethod]
        public void ValidatePipelineICloudBlobSuccessfullyTest()
        {
            AddTestBlobs();

            CloudBlockBlob blob = new CloudBlockBlob(new Uri("http://127.0.0.1/account/container1/blob0"));
            command.ValidatePipelineICloudBlob(blob);
        }

        [TestMethod]
        public void ValidatePipelineCloudBlobContainerWithNullObjectTest()
        {
            AssertThrows<ArgumentException>(() => command.ValidatePipelineCloudBlobContainer(null), String.Format(Resources.ObjectCannotBeNull, typeof(CloudBlobContainer).Name));
        }

        [TestMethod]
        public void ValidatePipelineCloudBlobContainerWithInvalidNameTest()
        {
            string uri = "http://127.0.0.1/account/t";
            CloudBlobContainer container = new CloudBlobContainer(new Uri(uri));
            AssertThrows<ArgumentException>(() => command.ValidatePipelineCloudBlobContainer(container), String.Format(Resources.InvalidContainerName, container.Name));
        }

        [TestMethod]
        public void ValidatePipelineCloudBlobContainerWithNotExistsContainerTest()
        {
            CloudBlobContainer container = BlobMock.GetContainerReference("test");
            AssertThrows<ResourceNotFoundException>(() => command.ValidatePipelineCloudBlobContainer(container), String.Format(Resources.ContainerNotFound, container.Name));
        }

        [TestMethod]
        public void ValidatePipelineCloudBlobContainerSuccessfullyTest()
        {
            AddTestContainers();
            string testUri = "http://127.0.0.1/account/test";
            CloudBlobContainer container = new CloudBlobContainer(new Uri(testUri));
            command.ValidatePipelineCloudBlobContainer(container);
        }

        [TestMethod]
        public void GetCloudBlobClientTest()
        {
            Assert.IsNotNull(command.GetCloudBlobClient());
        }
    }
}
