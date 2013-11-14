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
    using System.Threading;
    using System.Threading.Tasks;
    using Common;
    using Microsoft.WindowsAzure.Commands.Storage.Utilities;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Model.ResourceModel;

    [Cmdlet(VerbsCommon.Get, StorageNouns.CopyBlobStatus, DefaultParameterSetName = NameParameterSet),
       OutputType(typeof(AzureStorageBlob))]
    public class GetAzureStorageBlobCopyState : StorageCloudBlobCmdletBase
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

        [Parameter(HelpMessage = "Wait for copy task complete")]
        public SwitchParameter WaitForComplete
        {
            get { return waitForComplete;}
            set { waitForComplete = value; }
        }
        private bool waitForComplete;

        /// <summary>
        /// Execute command
        /// </summary>
        [PermissionSet(SecurityAction.Demand, Name = "FullTrust")]
        public override void ExecuteCmdlet()
        {
            long taskId = GetAvailableTaskId();
            ProgressRecord record = GetProgressRecord();
            Task task = null;

            switch (ParameterSetName)
            {
                case NameParameterSet:
                    task = GetBlobWithCopyStatus(ContainerName, BlobName, Context, taskId, record);
                    break;
                case ContainerPipelineParmeterSet:
                    task = GetBlobWithCopyStatus(CloudBlobContainer, BlobName, Context, taskId, record);
                    break;
                case BlobPipelineParameterSet:
                    task = GetBlobWithCopyStatus(ICloudBlob, Context, taskId, record);
                    break;
            }

            RunConcurrentTask(task, taskId);
        }

        internal ProgressRecord GetProgressRecord()
        {
            return new ProgressRecord(RecordIdUtil.GetRecordId(), Resources.CopyBlobActivity, Resources.CopyBlobActivity);
        }

        /// <summary>
        /// Write copy progress
        /// </summary>
        /// <param name="blob">ICloud blob object</param>
        /// <param name="progress">Progress record</param>
        internal void WriteCopyProgress(ICloudBlob blob, ProgressRecord progress)
        {
            long bytesCopied = blob.CopyState.BytesCopied ?? 0;
            long totalBytes = blob.CopyState.TotalBytes ?? 0;
            int percent = 0;

            if (totalBytes != 0)
            {
                percent = (int)(bytesCopied * 100 / totalBytes);
                progress.PercentComplete = percent;
            }

            string activity = String.Format(Resources.CopyBlobStatus, blob.CopyState.Status.ToString(), blob.Name, blob.Container.Name, blob.CopyState.Source.ToString());
            progress.Activity = activity;
            string message = String.Format(Resources.CopyBlobPendingStatus, percent, blob.CopyState.BytesCopied, blob.CopyState.TotalBytes);
            progress.StatusDescription = message;
            ProgressStream.WriteStream(progress);
        }

        /// <summary>
        /// Get blob with copy status by name
        /// </summary>
        /// <param name="containerName">Container name</param>
        /// <param name="blobName">blob name</param>
        /// <returns>ICloudBlob object</returns>
        private async Task GetBlobWithCopyStatus(string containerName, string blobName, AzureStorageContext context,
            long taskId, ProgressRecord record)
        {
            CloudBlobContainer container = Channel.GetContainerReference(containerName);
            await GetBlobWithCopyStatus(container, blobName, context, taskId, record);
        }

        /// <summary>
        /// Get blob with copy status by CloudBlobContainer object
        /// </summary>
        /// <param name="container">CloudBlobContainer object</param>
        /// <param name="blobName">Blob name</param>
        /// <returns>ICloudBlob object</returns>
        private async Task GetBlobWithCopyStatus(CloudBlobContainer container, string blobName,
            AzureStorageContext context, long taskId, ProgressRecord record)
        {
            AccessCondition accessCondition = null;
            BlobRequestOptions options = null;

            ValidateBlobName(blobName);
            ValidateContainerName(container.Name);

            ICloudBlob blob = default(ICloudBlob);

            try
            {
                blob = await Channel.GetBlobReferenceFromServerAsync(container, blobName, accessCondition, options, OperationContext, CmdletCancellationToken);
            }
            catch (StorageException e)
            {
                if (e.IsNotFoundException())
                {
                    throw new ResourceNotFoundException(String.Format(Resources.BlobNotFound, blobName, container.Name));
                }
            }

            await GetBlobWithCopyStatus(blob, context, taskId, record);
        }

        /// <summary>
        /// Get blob with copy status by ICloudBlob object
        /// </summary>
        /// <param name="blob">ICloudBlob object</param>
        /// <returns>ICloudBlob object</returns>
        private async Task GetBlobWithCopyStatus(ICloudBlob blob, AzureStorageContext context,
            long taskId, ProgressRecord record)
        {
            ValidateBlobName(blob.Name);

            AccessCondition accessCondition = null;
            BlobRequestOptions options = null;

            do
            {
                await Channel.FetchBlobAttributesAsync(blob, accessCondition, options, OperationContext, CmdletCancellationToken);

                if (blob.CopyState == null)
                {
                    throw new ArgumentException(String.Format(Resources.CopyTaskNotFound, blob.Name, blob.Container.Name));
                }

                if (blob.CopyState.Status == CopyStatus.Pending && waitForComplete)
                {
                    WriteCopyProgress(blob, record);
                }
                else
                {
                    OutputStream.WriteObject(taskId, blob.CopyState);
                    break;
                }

                Thread.Sleep(WaitTimeout);
            }
            while (true);
        }
    }
}
