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
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;

    /// <summary>
    /// Blob management interface
    /// </summary>
    public interface IStorageBlobManagement
    {
        /// <summary>
        /// Get a list of cloudblobcontainer in azure
        /// </summary>
        /// <param name="prefix">Container prefix</param>
        /// <param name="detailsIncluded">Container listing details</param>
        /// <param name="options">Blob request option</param>
        /// <param name="operationContext">Operation context</param>
        /// <returns>An enumerable collection of cloudblobcontainer</returns>
        IEnumerable<CloudBlobContainer> ListContainers(string prefix, ContainerListingDetails detailsIncluded, BlobRequestOptions options, OperationContext operationContext);

        /// <summary>
        /// Get container presssions
        /// </summary>
        /// <param name="container">A cloudblobcontainer object</param>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="options">Blob request option</param>
        /// <param name="operationContext">Operation context</param>
        /// <returns>The container's permission</returns>
        BlobContainerPermissions GetContainerPermissions(CloudBlobContainer container, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext);

        /// <summary>
        /// Async get container presssions
        /// </summary>
        /// <param name="container">A cloudblobcontainer object</param>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="options">Blob request option</param>
        /// <param name="operationContext">Operation context</param>
        /// <param name="cancellationToken">User cancellation token</param>
        /// <returns>A task object which retrieve the permission of the specified container</returns>
        Task<BlobContainerPermissions> GetContainerPermissionsAsync(CloudBlobContainer container, AccessCondition accessCondition,
            BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken);

        /// <summary>
        /// Set container permissions
        /// </summary>
        /// <param name="container">A cloudblobcontainer object</param>
        /// <param name="permissions">The container's permission</param>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="options">Blob request option</param>
        /// <param name="operationContext">Operation context</param>
        void SetContainerPermissions(CloudBlobContainer container, BlobContainerPermissions permissions, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext);

        /// <summary>
        /// Get a CloudBlobContainer instance in local
        /// </summary>
        /// <param name="name">Container name</param>
        /// <returns>A CloudBlobContainer in local memory</returns>
        CloudBlobContainer GetContainerReference(String name);

        /// <summary>
        /// Get blob reference with properties and meta data from server
        /// </summary>
        /// <param name="container">A cloudblobcontainer object</param>
        /// <param name="blobName">Blob name</param>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="options">Blob request options</param>
        /// <param name="operationContext">Operation context</param>
        /// <returns>Return an ICloudBlob if the specific blob exists on azure, otherwise return null</returns>
        ICloudBlob GetBlobReferenceFromServer(CloudBlobContainer container, string blobName, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext);

        /// <summary>
        /// Whether the container is exists or not
        /// </summary>
        /// <param name="container">A cloudblobcontainer object</param>
        /// <param name="options">Blob request option</param>
        /// <param name="operationContext">Operation context</param>
        /// <returns>True if the specific container exists, otherwise return false</returns>
        bool DoesContainerExist(CloudBlobContainer container, BlobRequestOptions options, OperationContext operationContext);

        /// <summary>
        /// Whether the blob exists or not
        /// </summary>
        /// <param name="blob">A icloudblob object</param>
        /// <param name="options">Blob request option</param>
        /// <param name="operationContext">Operation context</param>
        /// <returns>True if the specific blob exists, otherwise return false</returns>
        bool DoesBlobExist(ICloudBlob blob, BlobRequestOptions options, OperationContext operationContext);

        /// <summary>
        /// Create the container if not exists
        /// </summary>
        /// <param name="container">A cloudblobcontainer object</param>
        /// <param name="options">Blob request option</param>
        /// <param name="operationContext">Operation context</param>
        /// <returns>True if the container did not already exist and was created; otherwise false.</returns>
        bool CreateContainerIfNotExists(CloudBlobContainer container, BlobRequestOptions requestOptions, OperationContext operationContext);

        /// <summary>
        /// Delete container
        /// </summary>
        /// <param name="container">A cloudblobcontainer object</param>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="options">Blob request option</param>
        /// <param name="operationContext">Operation context</param>
        void DeleteContainer(CloudBlobContainer container, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext);

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
        IEnumerable<IListBlobItem> ListBlobs(CloudBlobContainer container, string prefix, bool useFlatBlobListing, BlobListingDetails blobListingDetails, BlobRequestOptions options, OperationContext operationContext);

        /// <summary>
        /// Delete azure blob
        /// </summary>
        /// <param name="blob">ICloudblob object</param>
        /// <param name="deleteSnapshotsOption">Delete snapshots option</param>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="operationContext">Operation context</param>
        /// <param name="options">Blob request option</param>
        void DeleteICloudBlob(ICloudBlob blob, DeleteSnapshotsOption deleteSnapshotsOption, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext);

        /// <summary>
        /// Fetch container attributes
        /// </summary>
        /// <param name="container">CloudBlobContainer object</param>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="options">Blob request options</param>
        /// <param name="operationContext">An object that represents the context for the current operation.</param>
        void FetchContainerAttributes(CloudBlobContainer container, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext);

        /// <summary>
        /// Fetch blob attributes
        /// </summary>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="options">Blob request options</param>
        /// <param name="operationContext">An object that represents the context for the current operation.</param>
        void FetchBlobAttributes(ICloudBlob blob, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext);

        /// <summary>
        /// Set blob properties
        /// </summary>
        /// <param name="blob">ICloud blob object</param>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="options">Blob request options</param>
        /// <param name="operationContext">An object that represents the context for the current operation.</param>
        void SetBlobProperties(ICloudBlob blob, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext);

        /// <summary>
        /// Set blob meta data
        /// </summary>
        /// <param name="blob">ICloud blob object</param>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="options">Blob request options</param>
        /// <param name="operationContext">An object that represents the context for the current operation.</param>
        void SetBlobMetadata(ICloudBlob blob, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext);

        /// <summary>
        /// Abort copy operation on specified blob
        /// </summary>
        /// <param name="blob">ICloudBlob object</param>
        /// <param name="copyId">Copy id</param>
        /// <param name="accessCondition">Access condition</param>
        /// <param name="options">Blob request options</param>
        /// <param name="operationContext">Operation context</param>
        void AbortCopy(ICloudBlob blob, string copyId, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext);

        Task<bool> DoesContainerExistAsync(CloudBlobContainer container, BlobRequestOptions requestOptions, OperationContext operationContext, CancellationToken cmdletCancellationToken);

        Task<ICloudBlob> GetBlobReferenceFromServerAsync(CloudBlobContainer container, string blobName, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cmdletCancellationToken);

        Task FetchBlobAttributesAsync(ICloudBlob blob, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cmdletCancellationToken);

        Task<bool> CreateContainerIfNotExistsAsync(CloudBlobContainer container, BlobContainerPublicAccessType accessType, BlobRequestOptions requestOptions, OperationContext operationContext, CancellationToken cmdletCancellationToken);

        Task DeleteContainerAsync(CloudBlobContainer container, AccessCondition accessCondition, BlobRequestOptions requestOptions, OperationContext operationContext, CancellationToken cmdletCancellationToken);

        Task AbortCopyAsync(ICloudBlob blob, string abortCopyId, AccessCondition accessCondition, BlobRequestOptions abortRequestOption, OperationContext operationContext, CancellationToken cmdletCancellationToken);

        Task SetContainerPermissionsAsync(CloudBlobContainer container, BlobContainerPermissions permissions, AccessCondition accessCondition, BlobRequestOptions requestOptions, OperationContext operationContext, CancellationToken cmdletCancellationToken);

        Task DeleteICloudBlobAsync(ICloudBlob blob, DeleteSnapshotsOption deleteSnapshotsOption, AccessCondition accessCondition, BlobRequestOptions requestOptions, OperationContext operationContext, CancellationToken cmdletCancellationToken);

        Task SetBlobMetadataAsync(ICloudBlob blob, System.Collections.Hashtable BlobMetadata, AccessCondition accessCondition, BlobRequestOptions requestOptions, OperationContext operationContext, CancellationToken cmdletCancellationToken);

        Task SetBlobPropertiesAsync(ICloudBlob blob, System.Collections.Hashtable BlobProperties, AccessCondition accessCondition, BlobRequestOptions requestOptions, OperationContext operationContext, CancellationToken cmdletCancellationToken);
    }
}
