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
// ---------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Management.Storage.Common
{
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    /// <summary>
    /// blob management interface
    /// </summary>
    public interface IBlobManagement
    {
        /// <summary>
        /// get a list of cloudblobcontainer in azure
        /// </summary>
        /// <param name="prefix"></param>
        /// <param name="detailsIncluded"></param>
        /// <param name="options"></param>
        /// <param name="operationContext"></param>
        /// <returns></returns>
        IEnumerable<CloudBlobContainer> ListContainers(string prefix, ContainerListingDetails detailsIncluded, BlobRequestOptions options, OperationContext operationContext);

        /// <summary>
        /// get container presssions
        /// </summary>
        /// <param name="container"></param>
        /// <param name="accessCondition"></param>
        /// <param name="options"></param>
        /// <param name="operationContext"></param>
        /// <returns></returns>
        BlobContainerPermissions GetContainerPermissions(CloudBlobContainer container, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext);

        /// <summary>
        /// set container permissions
        /// </summary>
        /// <param name="container"></param>
        /// <param name="permissions"></param>
        /// <param name="accessCondition"></param>
        /// <param name="options"></param>
        /// <param name="operationContext"></param>
        void SetContainerPermissions(CloudBlobContainer container, BlobContainerPermissions permissions, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext);

        /// <summary>
        /// get an CloudBlobContainer instance in local
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        CloudBlobContainer GetContainerReference(String name);

        /// <summary>
        /// get blob reference with properties and meta data from server
        /// </summary>
        /// <param name="container"></param>
        /// <param name="blobName"></param>
        /// <param name="accessCondition"></param>
        /// <param name="options"></param>
        /// <param name="operationContext"></param>
        /// <returns>return an ICloudBlob if the specific blob is existing on azure, otherwise return null</returns>
        ICloudBlob GetBlobReferenceFromServer(CloudBlobContainer container, string blobName, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext);

        /// <summary>
        /// whether the container is exists or not
        /// </summary>
        /// <param name="container"></param>
        /// <param name="options"></param>
        /// <param name="operationContext"></param>
        /// <returns></returns>
        bool IsContainerExists(CloudBlobContainer container, BlobRequestOptions options, OperationContext operationContext);

        /// <summary>
        /// whether the blob is exists or not
        /// </summary>
        /// <param name="blob"></param>
        /// <param name="options"></param>
        /// <param name="operationContext"></param>
        /// <returns></returns>
        bool IsBlobExists(ICloudBlob blob, BlobRequestOptions options, OperationContext operationContext);

        /// <summary>
        /// create the container if not exists
        /// </summary>
        /// <param name="container"></param>
        /// <param name="requestOptions"></param>
        /// <param name="operationContext"></param>
        /// <returns>true if the container did not already exist and was created; otherwise false.</returns>
        bool CreateContainerIfNotExists(CloudBlobContainer container, BlobRequestOptions requestOptions, OperationContext operationContext);

        /// <summary>
        /// delete container
        /// </summary>
        /// <param name="container"></param>
        /// <param name="accessCondition"></param>
        /// <param name="options"></param>
        /// <param name="operationContext"></param>
        void DeleteContainer(CloudBlobContainer container, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext);

        /// <summary>
        /// list all blobs in sepecific containers
        /// </summary>
        /// <param name="container"></param>
        /// <param name="prefix"></param>
        /// <param name="useFlatBlobListing"></param>
        /// <param name="blobListingDetails"></param>
        /// <param name="options"></param>
        /// <param name="operationContext"></param>
        /// <returns></returns>
        IEnumerable<IListBlobItem> ListBlobs(CloudBlobContainer container, string prefix, bool useFlatBlobListing, BlobListingDetails blobListingDetails, BlobRequestOptions options, OperationContext operationContext);

        /// <summary>
        /// delete azure blob
        /// </summary>
        /// <param name="blob"></param>
        /// <param name="deleteSnapshotsOption"></param>
        /// <param name="accessCondition"></param>
        /// <param name="options"></param>
        /// <param name="operationContext"></param>
        void DeleteICloudBlob(ICloudBlob blob, DeleteSnapshotsOption deleteSnapshotsOption, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext);
    }
}