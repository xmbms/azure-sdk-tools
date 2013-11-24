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

namespace Microsoft.WindowsAzure.Commands.Storage.Blob.Cmdlet
{
    using System;
    using System.Collections.Generic;
    using System.Management.Automation;
    using System.Security.Permissions;
    using System.Threading.Tasks;
    using Common;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Model.Contract;
    using Model.ResourceModel;

    /// <summary>
    /// list azure blobs in specified azure container
    /// </summary>
    [Cmdlet(VerbsCommon.Get, StorageNouns.Blob, DefaultParameterSetName = NameParameterSet),
        OutputType(typeof(AzureStorageBlob))]
    public class GetAzureStorageBlobCommand : StorageCloudBlobCmdletBase
    {
        /// <summary>
        /// default parameter set name
        /// </summary>
        private const string NameParameterSet = "BlobName";

        /// <summary>
        /// prefix parameter set name
        /// </summary>
        private const string PrefixParameterSet = "BlobPrefix";

        [Parameter(Position = 0, HelpMessage = "Blob name", ParameterSetName = NameParameterSet)]
        public string Blob 
        {
            get
            {
                return blobName;
            }
            set
            {
                blobName = value;
            }
        }
        private string blobName = String.Empty;

        [Parameter(HelpMessage = "Blob Prefix", ParameterSetName = PrefixParameterSet)]
        public string Prefix 
        {
            get
            {
                return blobPrefix;
            }
            set
            {
                blobPrefix = value;
            }
        }
        private string blobPrefix = String.Empty;

        [Alias("N", "Name")]
        [Parameter(Position = 1, Mandatory = true, HelpMessage = "Container name",
            ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty]
        public string Container
        {
            get
            {
                return containerName;
            }
            set
            {
                containerName = value;
            }
        }
        private string containerName = String.Empty;

        /// <summary>
        /// Initializes a new instance of the GetAzureStorageBlobCommand class.
        /// </summary>
        public GetAzureStorageBlobCommand()
            : this(null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the GetAzureStorageBlobCommand class.
        /// </summary>
        /// <param name="channel">IStorageBlobManagement channel</param>
        public GetAzureStorageBlobCommand(IStorageBlobManagement channel)
        {
            Channel = channel;
        }

        /// <summary>
        /// get the CloudBlobContianer object by name if container exists
        /// </summary>
        /// <param name="containerName">container name</param>
        /// <returns>return CloudBlobContianer object if specified container exists, otherwise throw an exception</returns>
        internal async Task<CloudBlobContainer> GetCloudBlobContainerByName(string containerName, bool skipCheckExists = false)
        {
            if (!NameUtil.IsValidContainerName(containerName))
            {
                throw new ArgumentException(String.Format(Resources.InvalidContainerName, containerName));
            }

            BlobRequestOptions requestOptions = null;
            CloudBlobContainer container = Channel.GetContainerReference(containerName);

            if (!skipCheckExists && container.ServiceClient.Credentials.IsSharedKey
                && !await Channel.DoesContainerExistAsync(container, requestOptions, OperationContext, CmdletCancellationToken))
            {
                throw new ArgumentException(String.Format(Resources.ContainerNotFound, containerName));
            }

            return container;
        }

        /// <summary>
        /// list blobs by blob name and container name
        /// </summary>
        /// <param name="containerName">container name</param>
        /// <param name="blobName">blob name pattern</param>
        /// <returns>An enumerable collection of IListBlobItem</returns>
        internal async Task ListBlobsByName(string containerName, string blobName,
            AzureStorageContext context, long taskId)
        {
            CloudBlobContainer container = null;
            BlobRequestOptions requestOptions = null;
            AccessCondition accessCondition = null;

            bool useFlatBlobListing = true;
            string prefix = string.Empty;
            //OutputStream.LockStream(taskId);
            BlobListingDetails details = BlobListingDetails.Snapshots | BlobListingDetails.Metadata | BlobListingDetails.Copy;

            if (String.IsNullOrEmpty(blobName) || WildcardPattern.ContainsWildcardCharacters(blobName))
            {
                container = await GetCloudBlobContainerByName(containerName);
                prefix = NameUtil.GetNonWildcardPrefix(blobName);
                IEnumerable<IListBlobItem> blobs = Channel.ListBlobs(container, prefix, useFlatBlobListing, details, requestOptions, OperationContext);
                WildcardOptions options = WildcardOptions.IgnoreCase | WildcardOptions.Compiled;
                WildcardPattern wildcard = null;

                if (!String.IsNullOrEmpty(blobName))
                {
                    wildcard = new WildcardPattern(blobName, options);
                }

                foreach (IListBlobItem blobItem in blobs)
                {
                    ICloudBlob blob = blobItem as ICloudBlob;

                    if (blob == null)
                    {
                        continue;
                    }

                    if (wildcard == null || wildcard.IsMatch(blob.Name))
                    {
                        WriteBlobsWithContext(taskId, context, blob);
                    }
                }
            }
            else
            {
                container = await GetCloudBlobContainerByName(containerName, true);

                if (!NameUtil.IsValidBlobName(blobName))
                {
                    throw new ArgumentException(String.Format(Resources.InvalidBlobName, blobName));
                }

                ICloudBlob blob = await Channel.GetBlobReferenceFromServerAsync(container, blobName, accessCondition,
                    requestOptions, OperationContext, CmdletCancellationToken);

                if (null == blob)
                {
                    throw new ResourceNotFoundException(String.Format(Resources.BlobNotFound, blobName, containerName));
                }
                else
                {
                    WriteBlobsWithContext(taskId, context, blob);
                }
            }

            //OutputStream.UnLockStream(taskId);
        }

        /// <summary>
        /// list blobs by blob prefix and container name
        /// </summary>
        /// <param name="containerName">container name</param>
        /// <param name="prefix">blob preifx</param>
        /// <returns>An enumerable collection of IListBlobItem</returns>
        internal async Task ListBlobsByPrefix(string containerName, string prefix,
            AzureStorageContext context, long taskId)
        {
            //OutputStream.LockStream(taskId);

            CloudBlobContainer container = await GetCloudBlobContainerByName(containerName);

            BlobRequestOptions requestOptions = null;
            bool useFlatBlobListing = true;
            BlobListingDetails details = BlobListingDetails.Snapshots | BlobListingDetails.Metadata | BlobListingDetails.Copy;

            IEnumerable<IListBlobItem> blobs = Channel.ListBlobs(container, prefix, useFlatBlobListing,
                details, requestOptions, OperationContext);

            foreach (IListBlobItem blobItem in blobs)
            {
                ICloudBlob blob = blobItem as ICloudBlob;

                if (blob == null)
                {
                    continue;
                }

                WriteBlobsWithContext(taskId, context, blob);
            }

            //OutputStream.UnLockStream(taskId);
        }

        /// <summary>
        /// write blobs with storage context
        /// </summary>
        /// <param name="blobList">An enumerable collection of IListBlobItem</param>
        internal void WriteBlobsWithContext(long taskId, AzureStorageContext context, ICloudBlob blob)
        {
            AzureStorageBlob azureBlob = new AzureStorageBlob(blob);
            azureBlob.Context = context;
            OutputStream.WriteObject(taskId, azureBlob);
        }

        /// <summary>
        /// execute command
        /// </summary>
        [PermissionSet(SecurityAction.Demand, Name = "FullTrust")]
        public override void ExecuteCmdlet()
        {

            Task task = null;
            long taskId = GetAvailableTaskId();

            if (PrefixParameterSet == ParameterSetName)
            {
                task = ListBlobsByPrefix(containerName, blobPrefix, Context, taskId);
            }
            else
            {
                task = ListBlobsByName(containerName, blobName, Context, taskId);
            }

            RunConcurrentTask(task, taskId);
        }
    }
}