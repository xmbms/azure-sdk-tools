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

namespace Microsoft.WindowsAzure.Commands.Storage.Blob
{
    using System;
    using System.Collections.Generic;
    using System.Management.Automation;
    using System.Security.Permissions;
    using Common;
    using Storage.Model.ResourceModel;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Model.Contract;
    using System.Threading.Tasks;

    [Cmdlet(VerbsCommon.Remove, StorageNouns.Blob, DefaultParameterSetName = NameParameterSet, SupportsShouldProcess = true, ConfirmImpact = ConfirmImpact.High),
        OutputType(typeof(Boolean))]
    public class RemoveStorageAzureBlobCommand : StorageCloudBlobCmdletBase
    {
        /// <summary>
        /// Blob Pipeline parameter set name
        /// </summary>
        private const string BlobPipelineParameterSet = "BlobPipeline";

        /// <summary>
        /// container pipeline paremeter set name
        /// </summary>
        private const string ContainerPipelineParmeterSet = "ContainerPipeline";

        /// <summary>
        /// blob name and container name parameter set
        /// </summary>
        private const string NameParameterSet = "NamePipeline";

        [Parameter(HelpMessage = "ICloudBlob Object", Mandatory = true,
            ValueFromPipelineByPropertyName = true, ParameterSetName = BlobPipelineParameterSet)]
        public ICloudBlob ICloudBlob { get; set; }

        [Parameter(HelpMessage = "CloudBlobContainer Object", Mandatory = true,
            ValueFromPipelineByPropertyName = true, ParameterSetName = ContainerPipelineParmeterSet)]
        public CloudBlobContainer CloudBlobContainer { get; set; }

        [Parameter(ParameterSetName = ContainerPipelineParmeterSet, Mandatory = true, Position = 0, HelpMessage = "Blob name")]
        [Parameter(ParameterSetName = NameParameterSet, Mandatory = true, Position = 0, HelpMessage = "Blob name")]
        public string Blob 
        {
            get { return BlobName; }
            set { BlobName = value; }
        }
        private string BlobName = String.Empty;

        [Parameter(HelpMessage = "Container name", Mandatory = true, Position = 1,
            ParameterSetName = NameParameterSet)]
        [ValidateNotNullOrEmpty]
        public string Container
        {
            get { return ContainerName; }
            set { ContainerName = value; }
        }
        private string ContainerName = String.Empty;

        [Parameter(HelpMessage = "Only delete blob snapshots")]
        public SwitchParameter DeleteSnapshot
        {
            get { return deleteSnapshot; }
            set { deleteSnapshot = value; }
        }
        private bool deleteSnapshot;

        [Parameter(HelpMessage = "Force to remove the blob and its snapshot without confirmation")]
        public SwitchParameter Force
        {
            get { return force; }
            set { force = value; }
        }
        private bool force = false;

        [Parameter(Mandatory = false, HelpMessage = "Return whether the specified blob is successfully removed")]
        public SwitchParameter PassThru { get; set; }

        /// <summary>
        /// Initializes a new instance of the RemoveStorageAzureBlobCommand class.
        /// </summary>
        public RemoveStorageAzureBlobCommand()
            : this(null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the RemoveStorageAzureBlobCommand class.
        /// </summary>
        /// <param name="channel">IStorageBlobManagement channel</param>
        public RemoveStorageAzureBlobCommand(IStorageBlobManagement channel)
        {
            Channel = channel;
        }

        /// <summary>
        /// confirm the remove operation
        /// </summary>
        /// <param name="message">confirmation message</param>
        /// <returns>true if the operation is confirmed by user, otherwise false</returns>
        internal virtual bool ConfirmRemove(string message)
        {
            return ShouldProcess(message);
        }

        /// <summary>
        /// check whether specified blob has snapshot
        /// </summary>
        /// <param name="blob">ICloudBlob object</param>
        /// <returns>true if the specified blob has snapshot, otherwise false</returns>
        internal bool HasSnapShot(ICloudBlob blob)
        {
            BlobListingDetails details = BlobListingDetails.Snapshots;
            BlobRequestOptions requestOptions = null;
            bool useFlatBlobListing = true;

            if (IsSnapshot(blob)) //snapshot can't have snapshot
            {
                return false;
            }

            IEnumerable<IListBlobItem> snapshots = Channel.ListBlobs(blob.Container, blob.Name, useFlatBlobListing, details, requestOptions, OperationContext);

            foreach (IListBlobItem item in snapshots)
            {
                ICloudBlob blobItem = item as ICloudBlob;

                if (blobItem != null && blobItem.Name == blob.Name && blobItem.SnapshotTime != null)
                {
                    return true;
                }
            }

            return false;
        }

        internal async Task<bool> HasSnapshotAsync(ICloudBlob blob)
        {
            bool hasSnapshot = await Task.Factory.StartNew<bool>(() => HasSnapShot(blob), CmdletCancellationToken);
            return hasSnapshot;
        }

        /// <summary>
        /// remove the azure blob 
        /// </summary>
        /// <param name="blob">ICloudblob object</param>
        /// <param name="isValidBlob">whether the ICloudblob parameter is validated</param>
        /// <returns>true if the blob is removed successfully, false if user cancel the remove operation</returns>
        internal async Task RemoveAzureBlob(ICloudBlob blob, bool isValidBlob, long taskId)
        {
            if (!isValidBlob)
            {
                ValidatePipelineICloudBlob(blob);
            }

            DeleteSnapshotsOption deleteSnapshotsOption = DeleteSnapshotsOption.None;
            AccessCondition accessCondition = null;
            BlobRequestOptions requestOptions = null;
            string result = string.Empty;

            if (IsSnapshot(blob) && deleteSnapshot)
            {
                throw new ArgumentException(String.Format(Resources.CannotDeleteSnapshotForSnapshot, blob.Name, blob.SnapshotTime));
            }

            if (deleteSnapshot)
            {
                deleteSnapshotsOption = DeleteSnapshotsOption.DeleteSnapshotsOnly;
            }
            else if (force)
            {
                deleteSnapshotsOption = DeleteSnapshotsOption.IncludeSnapshots;
            }
            //Will slow down performance 
            //else if (await HasSnapshotAsync(blob))
            //{
            //    string message = string.Format(Resources.ConfirmRemoveBlobWithSnapshot, blob.Name, blob.Container.Name);

            //    if (force)
            //    {
            //        deleteSnapshotsOption = DeleteSnapshotsOption.IncludeSnapshots;
            //    }
            //    else
            //    {
            //        result = String.Format(Resources.RemoveBlobCancelled, blob.Name, blob.Container.Name);
            //        VerboseStream.WriteStream(result);
            //    }
            //}

            await Channel.DeleteICloudBlobAsync(blob, deleteSnapshotsOption, accessCondition,
                requestOptions, OperationContext, CmdletCancellationToken);

            result = String.Format(Resources.RemoveBlobSuccessfully, blob.Name, blob.Container.Name);

            if (VerboseStream != null)
            {
                //For unit test
                VerboseStream.WriteStream(result);
            }

            if (PassThru)
            {
                OutputStream.WriteObject(taskId, true);
            }
        }

        /// <summary>
        /// remove azure blob
        /// </summary>
        /// <param name="container">CloudBlobContainer object</param>
        /// <param name="blobName">blob name</param>
        /// <returns>true if the blob is removed successfully, false if user cancel the remove operation</returns>
        internal async Task RemoveAzureBlob(CloudBlobContainer container, string blobName, long taskId)
        {
            if (!NameUtil.IsValidBlobName(blobName))
            {
                throw new ArgumentException(String.Format(Resources.InvalidBlobName, blobName));
            }

            ValidatePipelineCloudBlobContainer(container);
            AccessCondition accessCondition = null;
            BlobRequestOptions requestOptions = null;

            ICloudBlob blob = await Channel.GetBlobReferenceFromServerAsync(container, blobName, accessCondition,
                    requestOptions, OperationContext, CmdletCancellationToken);

            if (null == blob && container.ServiceClient.Credentials.IsSharedKey)
            {
                throw new ResourceNotFoundException(String.Format(Resources.BlobNotFound, blobName, container.Name));
            }
            else
            {
                //Construct the blob as CloudBlockBlob no matter what's the real blob type
                //We can't get the blob type if Credentials only have the delete permission and don't have read permission.
                blob = container.GetBlockBlobReference(blobName);
            }

            await RemoveAzureBlob(blob, true, taskId);
        }

        /// <summary>
        /// remove azure blob
        /// </summary>
        /// <param name="containerName">container name</param>
        /// <param name="blobName">blob name</param>
        /// <returns>true if the blob is removed successfully, false if user cancel the remove operation</returns>
        internal async Task RemoveAzureBlob(string containerName, string blobName, long taskId)
        {
            CloudBlobContainer container = Channel.GetContainerReference(containerName);
            await RemoveAzureBlob(container, blobName, taskId);
        }

        /// <summary>
        /// execute command
        /// </summary>
        [PermissionSet(SecurityAction.Demand, Name = "FullTrust")]
        public override void ExecuteCmdlet()
        {
            Task task = null;
            long taskId = GetAvailableTaskId();


            switch (ParameterSetName)
            {
                case BlobPipelineParameterSet:
                    task = RemoveAzureBlob(ICloudBlob, false, taskId);
                    break;

                case ContainerPipelineParmeterSet:
                    task = RemoveAzureBlob(CloudBlobContainer, BlobName, taskId);
                    break;

                case NameParameterSet:
                default:
                    task = RemoveAzureBlob(ContainerName, BlobName, taskId);
                    break;
            }

            RunConcurrentTask(task, taskId);

        }
    }
}