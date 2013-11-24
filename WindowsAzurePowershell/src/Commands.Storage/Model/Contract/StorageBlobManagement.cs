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

namespace Microsoft.WindowsAzure.Commands.Storage.Model.Contract
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Storage.Common;

    /// <summary>
    /// Blob management
    /// </summary>
    public class StorageBlobManagement : IStorageBlobManagement
    {
        /// <summary>
        /// Azure storage blob client
        /// </summary>
        private CloudBlobClient blobClient;

        /// <summary>
        /// Init blob management
        /// </summary>
        /// <param name="client">a cloud blob object</param>
        public StorageBlobManagement(CloudBlobClient client)
        {
            blobClient = client;
        }

        /// <summary>
        /// Get a list of cloudblobcontainer in azure
        /// </summary>
        /// <param name="prefix">Container prefix</param>
        /// <param name="detailsIncluded">Container listing details</param>
        /// <param name="options">Blob request option</param>
        /// <param name="operationContext">Operation context</param>
        /// <returns>An enumerable collection of cloudblobcontainer</returns>
        public IEnumerable<CloudBlobContainer> ListContainers(string prefix, ContainerListingDetails detailsIncluded, BlobRequestOptions options, OperationContext operationContext)
        {
            return blobClient.ListContainers(prefix, detailsIncluded, options, operationContext);
        }

        /// <summary>
        /// Get container presssions
        /// </summary>
        /// <param name="container">A cloudblobcontainer object</param>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="options">Blob request option</param>
        /// <param name="operationContext">Operation context</param>
        /// <returns>The container's permission</returns>
        public BlobContainerPermissions GetContainerPermissions(CloudBlobContainer container, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return container.GetPermissions(accessCondition, options, operationContext);
        }

        /// <summary>
        /// Get a CloudBlobContainer instance in local
        /// </summary>
        /// <param name="name">Container name</param>
        /// <returns>A CloudBlobContainer in local memory</returns>
        public CloudBlobContainer GetContainerReference(string name)
        {
            return blobClient.GetContainerReference(name);
        }

        /// <summary>
        /// Create the container if not exists
        /// </summary>
        /// <param name="container">A cloudblobcontainer object</param>
        /// <param name="options">Blob request option</param>
        /// <param name="operationContext">Operation context</param>
        /// <returns>True if the container did not already exist and was created; otherwise false.</returns>
        public bool CreateContainerIfNotExists(CloudBlobContainer container, BlobRequestOptions requestOptions, OperationContext operationContext)
        {
            return container.CreateIfNotExists(requestOptions, operationContext);
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
            container.Delete(accessCondition, options, operationContext);
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
            container.SetPermissions(permissions, accessCondition, options, operationContext);
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
            try
            {
                ICloudBlob blob = container.GetBlobReferenceFromServer(blobName, accessCondition, options, operationContext);
                return blob;
            }
            catch(StorageException e)
            {
                if (e.IsNotFoundException())
                {
                    return null;
                }
                else
                {
                    throw;
                }
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
            return container.ListBlobs(prefix, useFlatBlobListing, blobListingDetails, options, operationContext);
        }

        /// <summary>
        /// Whether the container exists or not
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
            else
            {
                return container.Exists(options, operationContext);
            }
        }

        /// <summary>
        /// Whether the blob exists or not
        /// </summary>
        /// <param name="blob">An ICloudBlob object</param>
        /// <param name="options">Blob request option</param>
        /// <param name="operationContext">Operation context</param>
        /// <returns>True if the specific blob exists, otherwise return false</returns>
        public bool DoesBlobExist(ICloudBlob blob, BlobRequestOptions options, OperationContext operationContext)
        {
            if (null == blob)
            {
                return false;
            }
            else
            {
                return blob.Exists(options, operationContext);
            }
        }

        /// <summary>
        /// Delete azure blob
        /// </summary>
        /// <param name="blob">ICloudblob object</param>
        /// <param name="deleteSnapshotsOption">Delete snapshots option</param>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="operationContext">Operation context</param>
        /// <param name="options">Blob request option</param>
        public void DeleteICloudBlob(ICloudBlob blob, DeleteSnapshotsOption deleteSnapshotsOption, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            blob.Delete(deleteSnapshotsOption, accessCondition, options, operationContext);
        }

        /// <summary>
        /// Fetch container attributes
        /// </summary>
        /// <param name="container">CloudBlobContainer object</param>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="options">Blob request options</param>
        /// <param name="operationContext">An object that represents the context for the current operation.</param>
        public void FetchContainerAttributes(CloudBlobContainer container, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            container.FetchAttributes(accessCondition, options, operationContext);
        }

        /// <summary>
        /// Fetch blob attributes
        /// </summary>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="options">Blob request options</param>
        /// <param name="operationContext">An object that represents the context for the current operation.</param>
        public void FetchBlobAttributes(ICloudBlob blob, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            blob.FetchAttributes(accessCondition, options, operationContext);
        }

        /// <summary>
        /// Set blob properties
        /// </summary>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="options">Blob request options</param>
        /// <param name="operationContext">An object that represents the context for the current operation.</param>
        public void SetBlobProperties(ICloudBlob blob, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            blob.SetProperties(accessCondition, options, operationContext);
        }

        /// <summary>
        /// Set blob meta data
        /// </summary>
        /// <param name="blob">ICloud blob object</param>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="options">Blob request options</param>
        /// <param name="operationContext">An object that represents the context for the current operation.</param>
        public void SetBlobMetadata(ICloudBlob blob, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            blob.SetMetadata(accessCondition, options, operationContext);
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
            try
            {
                blob.AbortCopy(copyId, accessCondition, options, operationContext);
            }
            catch (StorageException e)
            {
                if (e.IsSuccessfulResponse())
                {
                    //The abort operation is successful, although get an exception
                    return;
                }
                else
                {
                    throw;
                }
            }
        }

        /// <summary>
        /// Async Get container presssions
        /// </summary>
        /// <param name="container">A cloudblobcontainer object</param>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="options">Blob request option</param>
        /// <param name="operationContext">Operation context</param>
        /// <param name="cancellationToken">User cancellation token</param>
        /// <returns>A task object which retrieve the permission of the specified container</returns>
        public Task<BlobContainerPermissions> GetContainerPermissionsAsync(CloudBlobContainer container,
            AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext,
            CancellationToken cancellationToken)
        {
            return container.GetPermissionsAsync(accessCondition, options, operationContext, cancellationToken);
        }


        public Task<bool> DoesContainerExistAsync(CloudBlobContainer container, BlobRequestOptions requestOptions, OperationContext OperationContext, CancellationToken cancellationToken)
        {
            return container.ExistsAsync(requestOptions, OperationContext, cancellationToken);
        }

        public Task<ICloudBlob> GetBlobReferenceFromServerAsync(CloudBlobContainer container, string blobName, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return container.GetBlobReferenceFromServerAsync(blobName, accessCondition, options, operationContext, cancellationToken);
        }


        public Task FetchBlobAttributesAsync(ICloudBlob blob, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return blob.FetchAttributesAsync(accessCondition, options, operationContext, cancellationToken);
        }


        public Task<bool> CreateContainerIfNotExistsAsync(CloudBlobContainer container, BlobContainerPublicAccessType accessType, BlobRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return container.CreateIfNotExistsAsync(accessType, requestOptions, operationContext, cancellationToken);
        }

        public Task DeleteContainerAsync(CloudBlobContainer container, AccessCondition accessCondition, BlobRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return container.DeleteAsync(accessCondition, requestOptions, operationContext, cancellationToken);
        }

        public Task AbortCopyAsync(ICloudBlob blob, string copyId, AccessCondition accessCondition, BlobRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return blob.AbortCopyAsync(copyId, accessCondition, requestOptions, operationContext, cancellationToken);
        }

        public Task SetContainerPermissionsAsync(CloudBlobContainer container, BlobContainerPermissions permissions, AccessCondition accessCondition, BlobRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return container.SetPermissionsAsync(permissions, accessCondition, requestOptions, operationContext, cancellationToken);
        }

        public Task DeleteICloudBlobAsync(ICloudBlob blob, DeleteSnapshotsOption deleteSnapshotsOption, AccessCondition accessCondition, BlobRequestOptions requestOptions, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return blob.DeleteAsync(deleteSnapshotsOption, accessCondition, requestOptions, operationContext, cancellationToken);
        }


        public Task SetBlobMetadataAsync(ICloudBlob blob, System.Collections.Hashtable BlobMetadata, AccessCondition accessCondition, BlobRequestOptions requestOptions, OperationContext OperationContext, CancellationToken CmdletCancellationToken)
        {
            return blob.SetMetadataAsync(accessCondition, requestOptions, OperationContext, CmdletCancellationToken);
        }

        //FIXME remove hashtable parameter
        public Task SetBlobPropertiesAsync(ICloudBlob blob, System.Collections.Hashtable BlobProperties, AccessCondition accessCondition, BlobRequestOptions requestOptions, OperationContext OperationContext, CancellationToken CmdletCancellationToken)
        {
            return blob.SetPropertiesAsync(accessCondition, requestOptions, OperationContext, CmdletCancellationToken);
        }


        public Task<bool> DoesBlobExistAsync(ICloudBlob blob, BlobRequestOptions options, OperationContext operationContext, CancellationToken cmdletCancellationToken)
        {
            return blob.ExistsAsync(options, operationContext, cmdletCancellationToken);
        }
    }
}
