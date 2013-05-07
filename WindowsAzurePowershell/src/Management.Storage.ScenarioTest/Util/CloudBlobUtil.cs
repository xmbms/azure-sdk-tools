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
// ----------------------------------------------------------------------------------

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using MS.Test.Common.MsTestLib;
using StorageTestLib;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using StorageBlob = Microsoft.WindowsAzure.Storage.Blob;
using StorageType = Microsoft.WindowsAzure.Storage.Blob.BlobType;

namespace Management.Storage.ScenarioTest.Util
{
    public class CloudBlobUtil
    {
        private CloudStorageAccount account;
        private CloudBlobClient client;
        private Random random;
        private const int PageBlobUnitSize = 512;
        private static List<string> HttpsCopyHosts;

        public string ContainerName
        {
            get;
            private set;
        }

        public string BlobName
        {
            get;
            private set;
        }

        public ICloudBlob Blob
        {
            get;
            private set;
        }

        public CloudBlobContainer Container
        {
            get;
            private set;
        }

        private CloudBlobUtil()
        { }

        /// <summary>
        /// init cloud blob util
        /// </summary>
        /// <param name="account">storage account</param>
        public CloudBlobUtil(CloudStorageAccount account)
        {
            this.account = account;
            client = account.CreateCloudBlobClient();
            random = new Random();
        }

        /// <summary>
        /// Create a random container with a random blob
        /// </summary>
        public void SetupTestContainerAndBlob()
        {
            ContainerName = Utility.GenNameString("container");
            BlobName = Utility.GenNameString("blob");
            CloudBlobContainer container = CreateContainer(ContainerName);
            Blob = CreateRandomBlob(container, BlobName);
            Container = container;
        }

        /// <summary>
        /// clean test container and blob
        /// </summary>
        public void CleanupTestContainerAndBlob()
        {
            if (String.IsNullOrEmpty(ContainerName))
            {
                return;
            }

            RemoveContainer(ContainerName);
            ContainerName = string.Empty;
            BlobName = string.Empty;
            Blob = null;
            Container = null;
        }

        /// <summary>
        /// create a container with random properties and metadata
        /// </summary>
        /// <param name="containerName">container name</param>
        /// <returns>the created container object with properties and metadata</returns>
        public CloudBlobContainer CreateContainer(string containerName = "")
        {
            if (String.IsNullOrEmpty(containerName))
            {
                containerName = Utility.GenNameString("container");
            }

            CloudBlobContainer container = client.GetContainerReference(containerName);
            container.CreateIfNotExists();

            //there is no properties to set
            container.FetchAttributes();

            int minMetaCount = 1;
            int maxMetaCount = 5;
            int minMetaValueLength = 10;
            int maxMetaValueLength = 20;
            int count = random.Next(minMetaCount, maxMetaCount);
            for (int i = 0; i < count; i++)
            {
                string metaKey = Utility.GenNameString("metatest");
                int valueLength = random.Next(minMetaValueLength, maxMetaValueLength);
                string metaValue = Utility.GenNameString("metavalue-", valueLength);
                container.Metadata.Add(metaKey, metaValue);
            }

            container.SetMetadata();

            Test.Info(string.Format("create container '{0}'", containerName));
            return container;
        }

        public CloudBlobContainer CreateContainer(string containerName, BlobContainerPublicAccessType permission)
        {
            CloudBlobContainer container = CreateContainer(containerName);
            BlobContainerPermissions containerPermission = new BlobContainerPermissions();
            containerPermission.PublicAccess = permission;
            container.SetPermissions(containerPermission);
            return container;
        }

        /// <summary>
        /// create mutiple containers
        /// </summary>
        /// <param name="containerNames">container names list</param>
        /// <returns>a list of container object</returns>
        public List<CloudBlobContainer> CreateContainer(List<string> containerNames)
        {
            List<CloudBlobContainer> containers = new List<CloudBlobContainer>();

            foreach (string name in containerNames)
            {
                containers.Add(CreateContainer(name));
            }

            containers = containers.OrderBy(container => container.Name).ToList();

            return containers;
        }

        /// <summary>
        /// remove specified container
        /// </summary>
        /// <param name="Container">Cloud blob container object</param>
        public void RemoveContainer(CloudBlobContainer Container)
        {
            RemoveContainer(Container.Name);
        }

        /// <summary>
        /// remove specified container
        /// </summary>
        /// <param name="containerName">container name</param>
        public void RemoveContainer(string containerName)
        {
            CloudBlobContainer container = client.GetContainerReference(containerName);
            container.DeleteIfExists();
            Test.Info(string.Format("remove container '{0}'", containerName));
        }

        /// <summary>
        /// remove a list containers
        /// </summary>
        /// <param name="containerNames">container names</param>
        public void RemoveContainer(List<string> containerNames)
        {
            foreach (string name in containerNames)
            {
                try
                {
                    RemoveContainer(name);
                }
                catch (Exception e)
                {
                    Test.Warn(string.Format("Can't remove container {0}. Exception: {1}", name, e.Message));
                }
            }
        }

        /// <summary>
        /// create a new page blob with random properties and metadata
        /// </summary>
        /// <param name="container">CloudBlobContainer object</param>
        /// <param name="blobName">blob name</param>
        /// <returns>ICloudBlob object</returns>
        public ICloudBlob CreatePageBlob(CloudBlobContainer container, string blobName)
        {
            CloudPageBlob pageBlob = container.GetPageBlobReference(blobName);
            int size = random.Next(1, 10) * PageBlobUnitSize;
            pageBlob.Create(size);
            byte[] buffer = new byte[size];
            string md5sum = Convert.ToBase64String(Helper.GetMD5(buffer));
            pageBlob.Properties.ContentMD5 = md5sum;
            GenerateBlobPropertiesAndMetaData(pageBlob);
            Test.Info(string.Format("create page blob '{0}' in container '{1}'", blobName, container.Name));
            return pageBlob;
        }

        /// <summary>
        /// create a block blob with random properties and metadata
        /// </summary>
        /// <param name="container">CloudBlobContainer object</param>
        /// <param name="blobName">Block blob name</param>
        /// <returns>ICloudBlob object</returns>
        public ICloudBlob CreateBlockBlob(CloudBlobContainer container, string blobName)
        {
            CloudBlockBlob blockBlob = container.GetBlockBlobReference(blobName);

            int maxBlobSize = 1024 * 1024;
            string md5sum = string.Empty;
            int blobSize = random.Next(maxBlobSize);
            byte[] buffer = new byte[blobSize];
            using (MemoryStream ms = new MemoryStream(buffer))
            {
                random.NextBytes(buffer);
                //ms.Read(buffer, 0, buffer.Length);
                blockBlob.UploadFromStream(ms);
                md5sum = Convert.ToBase64String(Helper.GetMD5(buffer));
            }

            blockBlob.Properties.ContentMD5 = md5sum;
            GenerateBlobPropertiesAndMetaData(blockBlob);
            Test.Info(string.Format("create block blob '{0}' in container '{1}'", blobName, container.Name));
            return blockBlob;
        }

        /// <summary>
        /// generate random blob properties and metadata
        /// </summary>
        /// <param name="blob">ICloudBlob object</param>
        private void GenerateBlobPropertiesAndMetaData(ICloudBlob blob)
        {
            blob.Properties.ContentEncoding = Utility.GenNameString("encoding");
            blob.Properties.ContentLanguage = Utility.GenNameString("lang");

            int minMetaCount = 1;
            int maxMetaCount = 5;
            int minMetaValueLength = 10;
            int maxMetaValueLength = 20;
            int count = random.Next(minMetaCount, maxMetaCount);

            for (int i = 0; i < count; i++)
            {
                string metaKey = Utility.GenNameString("metatest");
                int valueLength = random.Next(minMetaValueLength, maxMetaValueLength);
                string metaValue = Utility.GenNameString("metavalue-", valueLength);
                blob.Metadata.Add(metaKey, metaValue);
            }

            blob.SetProperties();
            blob.SetMetadata();
            blob.FetchAttributes();
        }

        /// <summary>
        /// Create a blob with specified blob type
        /// </summary>
        /// <param name="container">CloudBlobContainer object</param>
        /// <param name="blobName">Blob name</param>
        /// <param name="type">Blob type</param>
        /// <returns>ICloudBlob object</returns>
        public ICloudBlob CreateBlob(CloudBlobContainer container, string blobName, StorageBlob.BlobType type)
        {
            if (type == StorageBlob.BlobType.BlockBlob)
            {
                return CreateBlockBlob(container, blobName);
            }
            else
            {
                return CreatePageBlob(container, blobName);
            }
        }

        /// <summary>
        /// create a list of blobs with random properties/metadata/blob type
        /// </summary>
        /// <param name="container">CloudBlobContainer object</param>
        /// <param name="blobName">a list of blob names</param>
        /// <returns>a list of cloud page blobs</returns>
        public List<ICloudBlob> CreateRandomBlob(CloudBlobContainer container, List<string> blobNames)
        {
            List<ICloudBlob> blobs = new List<ICloudBlob>();

            foreach (string blobName in blobNames)
            {
                blobs.Add(CreateRandomBlob(container, blobName));
            }

            blobs = blobs.OrderBy(blob => blob.Name).ToList();

            return blobs;
        }

        public List<ICloudBlob> CreateRandomBlob(CloudBlobContainer container)
        {
            int count = random.Next(1, 5);
            List<string> blobNames = new List<string>();
            for (int i = 0; i < count; i++)
            {
                blobNames.Add(Utility.GenNameString("blob"));
            }

            return CreateRandomBlob(container, blobNames);
        }

        /// <summary>
        /// Create a list of blobs with random properties/metadata/blob type
        /// </summary>
        /// <param name="container">CloudBlobContainer object</param>
        /// <param name="blobName">Blob name</param>
        /// <returns>ICloudBlob object</returns>
        public ICloudBlob CreateRandomBlob(CloudBlobContainer container, string blobName)
        {
            int switchKey = 0;

            switchKey = random.Next(0, 2);

            if (switchKey == 0)
            {
                return CreatePageBlob(container, blobName);
            }
            else
            {
                return CreateBlockBlob(container, blobName);
            }
        }



        /// <summary>
        /// convert blob name into valid file name
        /// </summary>
        /// <param name="blobName">blob name</param>
        /// <returns>valid file name</returns>
        public string ConvertBlobNameToFileName(string blobName, string dir, DateTimeOffset? snapshotTime = null)
        {
            string fileName = blobName;

            //replace dirctionary
            Dictionary<string, string> replaceRules = new Dictionary<string, string>()
	            {
	                {"/", "\\"}
	            };

            foreach (KeyValuePair<string, string> rule in replaceRules)
            {
                fileName = fileName.Replace(rule.Key, rule.Value);
            }

            if (snapshotTime != null)
            {
                int index = fileName.LastIndexOf('.');

                string prefix = string.Empty;
                string postfix = string.Empty;
                string timeStamp = string.Format("{0:u}", snapshotTime.Value);
                timeStamp = timeStamp.Replace(":", string.Empty).TrimEnd(new char[] { 'Z' });

                if (index == -1)
                {
                    prefix = fileName;
                    postfix = string.Empty;
                }
                else
                {
                    prefix = fileName.Substring(0, index);
                    postfix = fileName.Substring(index);
                }

                fileName = string.Format("{0} ({1}){2}", prefix, timeStamp, postfix);
            }

            return Path.Combine(dir, fileName);
        }

        public string ConvertFileNameToBlobName(string fileName)
        {
            return fileName.Replace('\\', '/');
        }

        /// <summary>
        /// list all the existing containers
        /// </summary>
        /// <returns>a list of cloudblobcontainer object</returns>
        public List<CloudBlobContainer> GetExistingContainers()
        {
            ContainerListingDetails details = ContainerListingDetails.All;
            return client.ListContainers(string.Empty, details).ToList();
        }

        /// <summary>
        /// get the number of existing container
        /// </summary>
        /// <returns></returns>
        public int GetExistingContainerCount()
        {
            return GetExistingContainers().Count;
        }

        /// <summary>
        /// Create a snapshot for the specified ICloudBlob object
        /// </summary>
        /// <param name="blob">ICloudBlob object</param>
        public ICloudBlob SnapShot(ICloudBlob blob)
        {
            ICloudBlob snapshot = default(ICloudBlob);

            switch (blob.BlobType)
            {
                case StorageBlob.BlobType.BlockBlob:
                    snapshot = ((CloudBlockBlob)blob).CreateSnapshot();
                    break;
                case StorageBlob.BlobType.PageBlob:
                    snapshot = ((CloudPageBlob)blob).CreateSnapshot();
                    break;
                default:
                    throw new ArgumentException(string.Format("Unsupport blob type {0} when create snapshot", blob.BlobType));
            }

            Test.Info(string.Format("Create snapshot for '{0}' at {1}", blob.Name, snapshot.SnapshotTime));

            return snapshot;
        }

        public static void PackContainerCompareData(CloudBlobContainer container, Dictionary<string, object> dic)
        {
            BlobContainerPermissions permissions = container.GetPermissions();
            dic["PublicAccess"] = permissions.PublicAccess;
            dic["Permission"] = permissions;
            dic["LastModified"] = container.Properties.LastModified;
        }

        public static void PackBlobCompareData(ICloudBlob blob, Dictionary<string, object> dic)
        {
            dic["Length"] = blob.Properties.Length;
            dic["ContentType"] = blob.Properties.ContentType;
            dic["LastModified"] = blob.Properties.LastModified;
            dic["SnapshotTime"] = blob.SnapshotTime;
        }

        public static string ConvertCopySourceUri(string uri)
        {
            if (HttpsCopyHosts == null)
            {
                HttpsCopyHosts = new List<string>();
                string httpsHosts = Test.Data.Get("HttpsCopyHosts");
                string[] hosts = httpsHosts.Split();

                foreach (string host in hosts)
                {
                    if (!String.IsNullOrWhiteSpace(host))
                    {
                        HttpsCopyHosts.Add(host);
                    }
                }
            }

            //Azure always use https to copy from these hosts such windows.net
            bool useHttpsCopy = HttpsCopyHosts.Any(host => uri.IndexOf(host) != -1);

            if (useHttpsCopy)
            {
                return uri.Replace("http://", "https://");
            }
            else
            {
                return uri;
            }
        }

        public static bool WaitForCopyOperationComplete(ICloudBlob destBlob, int maxRetry = 100)
        {
            int retryCount = 0;
            int sleepInterval = 1000; //ms

            if (destBlob == null)
            {
                return false;
            }

            do
            {
                if (retryCount > 0)
                {
                    Test.Info(String.Format("{0}th check current copy state and it's {1}. Wait for copy completion", retryCount, destBlob.CopyState.Status));
                }

                Thread.Sleep(sleepInterval);
                destBlob.FetchAttributes();
                retryCount++;
            }
            while (destBlob.CopyState.Status == CopyStatus.Pending && retryCount < maxRetry);

            Test.Info(String.Format("Final Copy status is {0}", destBlob.CopyState.Status));
            return destBlob.CopyState.Status != CopyStatus.Pending;
        }

        public static ICloudBlob GetBlob(CloudBlobContainer container, string blobName, StorageType blobType)
        {
            ICloudBlob blob = null;
            if (blobType == StorageType.BlockBlob)
            {
                blob = container.GetBlockBlobReference(blobName);
            }
            else
            {
                blob = container.GetPageBlobReference(blobName);
            }
            return blob;
        }
    }
}
