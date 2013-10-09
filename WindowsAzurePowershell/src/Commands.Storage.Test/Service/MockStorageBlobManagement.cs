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
// -----------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Commands.Storage.Test.Service
{
    using System;
    using System.Collections.Generic;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Model.Contract;
    using Microsoft.WindowsAzure.Storage.Auth;

    /// <summary>
    /// Mock blob management
    /// </summary>
    public class MockStorageBlobManagement : IStorageBlobManagement
    {
        /// <summary>
        /// Blob end point
        /// </summary>
        private string BlobEndPoint = "http://127.0.0.1/account/";

        /// <summary>
        /// Container list
        /// </summary>
        private List<CloudBlobContainer> containerList = new List<CloudBlobContainer>();

        public List<CloudBlobContainer> ContainerList
        {
            get
            {
                return containerList;
            }
        }
        
        /// <summary>
        /// Container permissions list
        /// </summary>
        private Dictionary<string, BlobContainerPermissions> containerPermissions = new Dictionary<string, BlobContainerPermissions>();
        public Dictionary<string, BlobContainerPermissions> ContainerPermissions
        {
            get
            {
                return containerPermissions;
            }
        }

        /// <summary>
        /// Container blobs list
        /// </summary>
        private Dictionary<string, List<ICloudBlob>> containerBlobs = new Dictionary<string, List<ICloudBlob>>();
        public Dictionary<string, List<ICloudBlob>> ContainerBlobs
        {
            get
            {
                return containerBlobs;
            }
        }

        /// <summary>
        /// Get a list of cloudblobcontainer in azure
        /// </summary>
        /// <param name="prefix">Container prefix</param>
        /// <param name="detailsIncluded">Container listing details</param>
        /// <param name="options">Blob request option</param>
        /// <param name="operationContext">Operation context</param>
        /// <returns>An enumerable collection of cloudblobcontainer</returns>
        public IEnumerable<CloudBlobContainer> ListContainers(string prefix, ContainerListingDetails detailsIncluded, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            if (string.IsNullOrEmpty(prefix))
            {
                return ContainerList;
            }
            else
            {
                List<CloudBlobContainer> prefixContainerList = new List<CloudBlobContainer>();

                foreach (CloudBlobContainer container in ContainerList)
                {
                    if (container.Name.StartsWith(prefix))
                    {
                        prefixContainerList.Add(container);
                    }
                }

                return prefixContainerList;
            }
        }

        /// <summary>
        /// Get container permissions
        /// </summary>
        /// <param name="container">A cloudblobcontainer object</param>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="options">Blob request option</param>
        /// <param name="operationContext">Operation context</param>
        /// <returns>The container's permission</returns>
        public BlobContainerPermissions GetContainerPermissions(CloudBlobContainer container, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            BlobContainerPermissions defaultPermission = new BlobContainerPermissions();
            defaultPermission.PublicAccess = BlobContainerPublicAccessType.Off;

            if (ContainerPermissions.ContainsKey(container.Name))
            {
                return ContainerPermissions[container.Name];
            }
            else
            {
                return defaultPermission;
            }
        }

        /// <summary>
        /// Get an CloudBlobContainer instance in local
        /// </summary>
        /// <param name="name">Container name</param>
        /// <returns>A CloudBlobContainer in local memory</returns>
        public CloudBlobContainer GetContainerReference(string name)
        {
            Uri containerUri = new Uri(String.Format("{0}{1}/", BlobEndPoint, name));
            string testName = "testaccount";
            Guid guid = Guid.NewGuid();
            string testKey = Convert.ToBase64String(guid.ToByteArray());
            StorageCredentials credentials = new StorageCredentials(testName, testKey);
            return new CloudBlobContainer(containerUri, credentials);
        }

        /// <summary>
        /// Create the container if not exists
        /// </summary>
        /// <param name="container">A cloudblobcontainer object</param>
        /// <param name="options">Blob request option</param>
        /// <param name="operationContext">Operation context</param>
        /// <returns>True if the container did not already exist and was created; otherwise false.</returns>
        public bool CreateContainerIfNotExists(CloudBlobContainer container, BlobRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            CloudBlobContainer containerRef =  GetContainerReference(container.Name);

            if (DoesContainerExist(containerRef, requestOptions, operationContext))
            {
                return false;
            }
            else
            {
                containerRef = GetContainerReference(container.Name);
                ContainerList.Add(containerRef);
                return true;
            }
        }

        /// <summary>
        /// Delete container
        /// </summary>
        /// <param name="container">A cloudblobcontainer object</param>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="options">Blob request option</param>
        /// <param name="operationContext">Operation context</param>
        public void DeleteContainer(CloudBlobContainer container, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            foreach (CloudBlobContainer containerRef in ContainerList)
            {
                if (container.Name == containerRef.Name)
                {
                    ContainerList.Remove(containerRef);
                    return;
                }
            }
        }

        /// <summary>
        /// Set container permissions
        /// </summary>
        /// <param name="container">A cloudblobcontainer object</param>
        /// <param name="permissions">The container's permission</param>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="options">Blob request option</param>
        /// <param name="operationContext">Operation context</param>
        public void SetContainerPermissions(CloudBlobContainer container, BlobContainerPermissions permissions, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            String name = container.Name;

            if (ContainerPermissions.ContainsKey(name))
            {
                ContainerPermissions[name] = permissions;
            }
            else
            {
                ContainerPermissions.Add(name, permissions);
            }
        }

        /// <summary>
        /// Get blob reference with properties and meta data from server
        /// </summary>
        /// <param name="container">A cloudblobcontainer object</param>
        /// <param name="blobName">Blob name</param>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="options">Blob request options</param>
        /// <param name="operationContext">Operation context</param>
        /// <returns>Return an ICloudBlob if the specific blob exists on azure, otherwise return null</returns>
        public ICloudBlob GetBlobReferenceFromServer(CloudBlobContainer container, string blobName, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            string containerName = container.Name;

            if (ContainerBlobs.ContainsKey(containerName))
            {
                List<ICloudBlob> blobList = ContainerBlobs[containerName];
                
                foreach (ICloudBlob blob in blobList)
                {
                    if (blob.Name == blobName)
                    {
                        return blob;
                    }
                }
                
                return null;
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// List all blobs in specified containers
        /// </summary>
        /// <param name="container">A cloudblobcontainer object</param>
        /// <param name="prefix">Blob prefix</param>
        /// <param name="useFlatBlobListing">Use flat blob listing(whether treat "container/" as directory)</param>
        /// <param name="blobListingDetails">Blob listing details</param>
        /// <param name="options">Blob request option</param>
        /// <param name="operationContext">Operation context</param>
        /// <returns>An enumerable collection of icloudblob</returns>
        public IEnumerable<IListBlobItem> ListBlobs(CloudBlobContainer container, string prefix, bool useFlatBlobListing, BlobListingDetails blobListingDetails, BlobRequestOptions options, OperationContext operationContext)
        {
            string containerName = container.Name;

            if (ContainerBlobs.ContainsKey(containerName))
            {
                List<ICloudBlob> blobList = ContainerBlobs[containerName];
                
                if (string.IsNullOrEmpty(prefix))
                {
                    return blobList;
                }
                
                List<ICloudBlob> prefixBlobs = new List<ICloudBlob>();
                
                foreach (ICloudBlob blob in blobList)
                {
                    if (blob.Name.StartsWith(prefix))
                    {
                        prefixBlobs.Add(blob);
                    }
                }
                
                return prefixBlobs;
            }
            else
            {
                return new List<ICloudBlob>();
            }
        }

        /// <summary>
        /// Whether the container is exists or not
        /// </summary>
        /// <param name="container">A cloudblobcontainer object</param>
        /// <param name="options">Blob request option</param>
        /// <param name="operationContext">Operation context</param>
        /// <returns>True if the specific container exists, otherwise return false</returns>
        public bool DoesContainerExist(CloudBlobContainer container, BlobRequestOptions options, OperationContext operationContext)
        {
            if (null == container)
            {
                return false;
            }
            
            foreach (CloudBlobContainer containerRef in ContainerList)
            {
                if (containerRef.Name == container.Name)
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Whether the blob is exists or not
        /// </summary>
        /// <param name="blob">A icloudblob object</param>
        /// <param name="options">Blob request option</param>
        /// <param name="operationContext">Operation context</param>
        /// <returns>True if the specific blob exists, otherwise return false</returns>
        public bool DoesBlobExist(ICloudBlob blob, BlobRequestOptions options, OperationContext operationContext)
        {
            CloudBlobContainer container = blob.Container;

            if (!ContainerBlobs.ContainsKey(container.Name))
            {
                return false;
            }
            else
            {
                List<ICloudBlob> blobList = ContainerBlobs[container.Name];
                
                foreach (ICloudBlob blobRef in blobList)
                {
                    if (blobRef.Name == blob.Name)
                    {
                        return true;
                    }
                }

                return false;
            }
        }

        /// <summary>
        /// Delete azure blob
        /// </summary>
        /// <param name="blob">ICloudblob object</param>
        /// <param name="deleteSnapshotsOption">Delete snapshots option</param>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="operationContext">Operation context</param>
        /// <returns>An enumerable collection of icloudblob</returns>
        public void DeleteICloudBlob(ICloudBlob blob, DeleteSnapshotsOption deleteSnapshotsOption, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            CloudBlobContainer container = blob.Container;

            if (!ContainerBlobs.ContainsKey(container.Name))
            {
                return;
            }
            else
            {
                List<ICloudBlob> blobList = ContainerBlobs[container.Name];
                
                foreach (ICloudBlob blobRef in blobList)
                {
                    if (blobRef.Name == blob.Name)
                    {
                        blobList.Remove(blobRef);
                        return;
                    }
                }
            }
        }

        /// <summary>
        /// fetch container attributes
        /// </summary>
        /// <param name="container">CloudBlobContainer object</param>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="options">blob request options</param>
        /// <param name="operationContext">An object that represents the context for the current operation.</param>
        public void FetchContainerAttributes(CloudBlobContainer container, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return;
        }

        /// <summary>
        /// fetch blob attributes
        /// </summary>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="options">blob request options</param>
        /// <param name="operationContext">An object that represents the context for the current operation.</param>
        public void FetchBlobAttributes(ICloudBlob blob, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return ;
        }

        /// <summary>
        /// set blob properties
        /// </summary>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="options">blob request options</param>
        /// <param name="operationContext">An object that represents the context for the current operation.</param>
        public void SetBlobProperties(ICloudBlob blob, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return;
        }

        /// <summary>
        /// set blob meta data
        /// </summary>
        /// <param name="blob">ICloud blob object</param>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="options">blob request options</param>
        /// <param name="operationContext">An object that represents the context for the current operation.</param>
        public void SetBlobMetadata(ICloudBlob blob, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return;
        }

        /// <summary>
        /// Abort copy operation on specified blob
        /// </summary>
        /// <param name="blob">ICloudBlob object</param>
        /// <param name="copyId">Copy id</param>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="options">Blob request options</param>
        /// <param name="operationContext">Operation context</param>
        public void AbortCopy(ICloudBlob blob, string copyId, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return;
        }
    }
}
