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
    using Microsoft.WindowsAzure.Management.Storage.Test.Service;
    using Microsoft.WindowsAzure.Storage.Blob;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    /// <summary>
    /// test base class for storage blob
    /// </summary>
    public class StorageBlobTestBase : StorageTestBase
    {
        public MockStorageBlobManagement blobMock = null;

        [TestInitialize]
        public void InitMock()
        {
            blobMock = new MockStorageBlobManagement();
        }

        [TestCleanup]
        public void CleanMock()
        {
            blobMock = null;
        }

        /// <summary>
        /// clean all the test data
        /// </summary>
        private void CleanTestData()
        {
            blobMock.ContainerList.Clear();
            blobMock.ContainerPermissions.Clear();
            blobMock.ContainerBlobs.Clear();
        }

        /// <summary>
        /// add test containers
        /// </summary>
        public void AddTestContainers()
        {
            CleanTestData();
            string testUri = "http://127.0.0.1/account/test";
            string textUri = "http://127.0.0.1/account/text";
            string publicOffUri = "http://127.0.0.1/account/publicoff";
            string publicBlobUri = "http://127.0.0.1/account/publicblob";
            string publicContainerUri = "http://127.0.0.1/account/publiccontainer";
            blobMock.ContainerList.Add(new CloudBlobContainer(new Uri(testUri)));
            blobMock.ContainerList.Add(new CloudBlobContainer(new Uri(textUri)));
            blobMock.ContainerList.Add(new CloudBlobContainer(new Uri(publicOffUri)));
            blobMock.ContainerList.Add(new CloudBlobContainer(new Uri(publicBlobUri)));
            blobMock.ContainerList.Add(new CloudBlobContainer(new Uri(publicContainerUri)));

            BlobContainerPermissions publicOff = new BlobContainerPermissions();
            publicOff.PublicAccess = BlobContainerPublicAccessType.Off;
            blobMock.ContainerPermissions.Add("publicoff", publicOff);
            BlobContainerPermissions publicBlob = new BlobContainerPermissions();
            publicBlob.PublicAccess = BlobContainerPublicAccessType.Blob;
            blobMock.ContainerPermissions.Add("publicblob", publicBlob);
            BlobContainerPermissions publicContainer = new BlobContainerPermissions();
            publicContainer.PublicAccess = BlobContainerPublicAccessType.Container;
            blobMock.ContainerPermissions.Add("publiccontainer", publicContainer);
        }

        /// <summary>
        /// add test blobs
        /// </summary>
        public void AddTestBlobs()
        {
            CleanTestData();
            string container0Uri = "http://127.0.0.1/account/container0";
            string container1Uri = "http://127.0.0.1/account/container1";
            string container20Uri = "http://127.0.0.1/account/container20";
            blobMock.ContainerList.Add(new CloudBlobContainer(new Uri(container0Uri)));
            AddContainerBlobs("container0", 0);
            blobMock.ContainerList.Add(new CloudBlobContainer(new Uri(container1Uri)));
            AddContainerBlobs("container1", 1);
            blobMock.ContainerList.Add(new CloudBlobContainer(new Uri(container20Uri)));
            AddContainerBlobs("container20", 20);
        }

        /// <summary>
        /// add some blobs into a container
        /// </summary>
        /// <param name="containerName"></param>
        /// <param name="count"></param>
        private void AddContainerBlobs(string containerName, int count)
        {
            List<ICloudBlob> blobList = null;
            if (blobMock.ContainerBlobs.ContainsKey(containerName))
            {
                blobList = blobMock.ContainerBlobs[containerName];
                blobList.Clear();
            }
            else
            {
                blobList = new List<ICloudBlob>();
                blobMock.ContainerBlobs.Add(containerName, blobList);
            }
            string prefix = "blob";
            string uri = string.Empty;
            string endPoint = "http://127.0.0.1/account";
            for(int i = 0; i < count; i++)
            {
                uri = string.Format("{0}/{1}/{2}{3}", endPoint, containerName, prefix, i);
                CloudBlockBlob blob = new CloudBlockBlob(new Uri(uri));
                blobList.Add(blob);
            }
        }
    }
}