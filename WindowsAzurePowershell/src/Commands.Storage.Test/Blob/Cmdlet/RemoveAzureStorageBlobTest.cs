// ----------------------------------------------------------------------------------
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
// ---------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Commands.Storage.Test.Blob.Cmdlet
{
    using System;
    using System.Linq;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Commands.Test.Utilities.Common;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Storage.Blob;
    using Storage.Common;

    [TestClass]
    public class RemoveAzureStorageBlobTest : StorageBlobTestBase
    {
        public RemoveStorageAzureBlobCommand command = null;

        [TestInitialize]
        public void InitCommand()
        {
            command = new RemoveStorageAzureBlobCommand(BlobMock)
                {
                    CommandRuntime = new MockCommandRuntime()
                };
        }

        [TestCleanup]
        public void CleanCommand()
        {
            command = null;
        }

        [TestMethod]
        public void ValidatePipelineCloudBlobContainerTest()
        {
            CloudBlobContainer container = null;
            AssertThrows<ArgumentException>(()=>command.ValidatePipelineCloudBlobContainer(container), 
                String.Format(Resources.ObjectCannotBeNull, typeof(CloudBlobContainer).Name));

            container = BlobMock.GetContainerReference("t");
            AssertThrows<ArgumentException>(() => command.ValidatePipelineCloudBlobContainer(container),
                String.Format(Resources.InvalidContainerName, "t"));
            container = BlobMock.GetContainerReference("test");
            AssertThrows<ResourceNotFoundException>(() => command.ValidatePipelineCloudBlobContainer(container),
                String.Format(Resources.ContainerNotFound, "test"));

            AddTestContainers();
            command.ValidatePipelineCloudBlobContainer(container);
            container = BlobMock.GetContainerReference("text");
            command.ValidatePipelineCloudBlobContainer(container);
        }

        [TestMethod]
        public void ValidatePipelineICloudBlobTest()
        {
            CloudBlockBlob blockBlob = null;
            AssertThrows<ArgumentException>(() => command.ValidatePipelineICloudBlob(blockBlob),
                String.Format(Resources.ObjectCannotBeNull, typeof(ICloudBlob).Name));
            string blobUri = "http://127.0.0.1/account/test/";
            blockBlob = new CloudBlockBlob(new Uri(blobUri));
            AssertThrows<ArgumentException>(() => command.ValidatePipelineICloudBlob(blockBlob),
                String.Format(Resources.InvalidBlobName, blockBlob.Name));

            AddTestBlobs();
            string container1Uri = "http://127.0.0.1/account/container1/blob";
            blockBlob = new CloudBlockBlob(new Uri(container1Uri));
            AssertThrows<ResourceNotFoundException>(() => command.ValidatePipelineICloudBlob(blockBlob),
                String.Format(Resources.BlobNotFound, blockBlob.Name, blockBlob.Container.Name));
            container1Uri = "http://127.0.0.1/account/container1/blob0";
            blockBlob = new CloudBlockBlob(new Uri(container1Uri));
            command.ValidatePipelineICloudBlob(blockBlob);
        }

        [TestMethod]
        public void RemoveAzureBlobByICloudBlobWithInvliadICloudBlob()
        {
            CloudBlockBlob blockBlob = null;
            AssertThrows<ArgumentException>(() => command.RemoveAzureBlob(blockBlob, false),
                String.Format(Resources.ObjectCannotBeNull, typeof(ICloudBlob).Name));
        }

        [TestMethod]
        public void RemoveAzureBlobByICloudBlobWithNoExistsContainer()
        {
            CloudBlobContainer container = BlobMock.GetContainerReference("test");
            CloudBlockBlob blockBlob = container.GetBlockBlobReference("blob");
            command.RemoveAzureBlob(blockBlob, true);
            AssertThrows<ResourceNotFoundException>(() => command.RemoveAzureBlob(blockBlob, false),
                String.Format(Resources.ContainerNotFound, blockBlob.Container.Name));
        }

        [TestMethod]
        public void RemoveAzureBlobByICloudBlobWithNoExistsBlobTest()
        {
            AddTestContainers();
            string blobUri = "http://127.0.0.1/account/test/blob";
            CloudBlockBlob blockBlob = new CloudBlockBlob(new Uri(blobUri));
            AssertThrows<ResourceNotFoundException>(() => command.RemoveAzureBlob(blockBlob, false),
                String.Format(Resources.BlobNotFound, blockBlob.Name, blockBlob.Container.Name));
        }

        [TestMethod]
        public void RemoveAzureBlobByICloudBlobSuccessfulyTest()
        {
            AddTestBlobs();
            string blobUri = "http://127.0.0.1/account/container0/blob0";
            CloudBlockBlob blockBlob = new CloudBlockBlob(new Uri(blobUri));
            command.RemoveAzureBlob(blockBlob, true);
            AssertThrows<ResourceNotFoundException>(() => command.RemoveAzureBlob(blockBlob, false),
                String.Format(Resources.BlobNotFound, blockBlob.Name, blockBlob.Container.Name));
            blobUri = "http://127.0.0.1/account/container1/blob0";
            blockBlob = new CloudBlockBlob(new Uri(blobUri));
            command.RemoveAzureBlob(blockBlob, true);

            AddTestBlobs();
            command.RemoveAzureBlob(blockBlob, false);
            AssertThrows<ResourceNotFoundException>(() => command.RemoveAzureBlob(blockBlob, false),
                String.Format(Resources.BlobNotFound, blockBlob.Name, blockBlob.Container.Name));
        }

        [TestMethod]
        public void RemoveAzureBlobByCloudBlobContainerWithInvalidNameTest()
        {
            CloudBlobContainer container = null;
            string blobName = string.Empty;

            AssertThrows<ArgumentException>(() => command.RemoveAzureBlob(container, blobName),
                String.Format(Resources.InvalidBlobName, blobName));

            blobName = "a";
            AssertThrows<ArgumentException>(() => command.RemoveAzureBlob(container, blobName),
                String.Format(Resources.ObjectCannotBeNull, typeof(CloudBlobContainer).Name));

            string containeruri = "http://127.0.0.1/account/t";
            container = new CloudBlobContainer(new Uri(containeruri));
            AssertThrows<ArgumentException>(() => command.RemoveAzureBlob(container, blobName),
                String.Format(Resources.InvalidContainerName, container.Name));
        }

        [TestMethod]
        public void RemoveAzureBlobByCloudBlobContainerWithNotExistsContianerTest()
        {
            string blobName = "blob";
            CloudBlobContainer container = BlobMock.GetContainerReference("test");
            AssertThrows<ResourceNotFoundException>(() => command.RemoveAzureBlob(container, blobName),
                String.Format(Resources.ContainerNotFound, container.Name));
        }

        [TestMethod]
        public void RemoveAzureBlobByCloudBlobContainerWithNotExistsBlobTest()
        {
            AddTestContainers();
            CloudBlobContainer container = BlobMock.GetContainerReference("test");
            string blobName = "test";
            AssertThrows<ResourceNotFoundException>(() => command.RemoveAzureBlob(container, blobName),
                String.Format(Resources.BlobNotFound, blobName, container.Name));
        }

        [TestMethod]
        public void RemoveAzureBlobByCloudBlobContainerSuccessfullyTest()
        {
            AddTestBlobs();
            CloudBlobContainer container = BlobMock.GetContainerReference("container1");
            string blobName = "blob0";
            command.RemoveAzureBlob(container, blobName);
            AssertThrows<ResourceNotFoundException>(() => command.RemoveAzureBlob(container, blobName),
                String.Format(Resources.BlobNotFound, blobName, "container1"));
        }

        [TestMethod]
        public void RemoveAzureBlobByNameWithInvalidNameTest()
        {
            string containerName = string.Empty;
            string blobName = string.Empty;
            AssertThrows<ArgumentException>(() => command.RemoveAzureBlob(containerName, blobName),
                String.Format(Resources.InvalidBlobName, blobName));
            blobName = "abcd";
            AssertThrows<ArgumentException>(() => command.RemoveAzureBlob(containerName, blobName),
                String.Format(Resources.InvalidContainerName, containerName));
        }

        [TestMethod]
        public void RemoveAzureBlobByNameTest()
        { 
            AddTestBlobs();
            string containerName = "container1";
            string blobName = "blob0";
            command.RemoveAzureBlob(containerName, blobName);
            AssertThrows<ResourceNotFoundException>(() => command.RemoveAzureBlob(containerName, blobName),
                String.Format(Resources.BlobNotFound, blobName, containerName));
        }

        [TestMethod]
        public void ExecuteCommandRemoveBlobTest()
        { 
            AddTestBlobs();
            string containerName = "container20";
            string blobName = "blob0";
            command.Container = containerName;
            command.Blob = blobName;
            command.ExecuteCmdlet();
            string result = (string)((MockCommandRuntime)command.CommandRuntime).VerboseStream.FirstOrDefault();
            Assert.AreEqual(String.Format(Resources.RemoveBlobSuccessfully, blobName, containerName), result);
            AssertThrows<ResourceNotFoundException>(() => command.ExecuteCmdlet(),
                String.Format(Resources.BlobNotFound, blobName, containerName));
            
        }
    }
}
