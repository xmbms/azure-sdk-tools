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
    using System.Management.Automation;
    using System.Security.Permissions;
    using System.Threading.Tasks;
    using Common;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.RetryPolicies;
    using Model.ResourceModel;

    [Cmdlet(VerbsLifecycle.Stop, StorageNouns.CopyBlob, ConfirmImpact = ConfirmImpact.High, DefaultParameterSetName = NameParameterSet),
       OutputType(typeof(AzureStorageBlob))]
    public class StopAzureStorageBlobCopy : StorageCloudBlobCmdletBase
    {
        /// <summary>
        /// Blob Pipeline parameter set name
        /// </summary>
        private const string BlobPipelineParameterSet = "BlobPipeline";

        /// <summary>
        /// Container pipeline paremeter set name
        /// </summary>
        private const string ContainerPipelineParmeterSet = "ContainerPipeline";

        /// <summary>
        /// Blob name and container name parameter set
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

        [Parameter(HelpMessage = "Force to stop the current copy task on the specified blob")]
        public SwitchParameter Force
        {
            get { return force; }
            set { force = value; }
        }
        private bool force = false;

        [Parameter(HelpMessage = "Copy Id", Mandatory = false)]
        [ValidateNotNullOrEmpty]
        public string CopyId
        {
            get { return copyId; }
            set { copyId = value; }
        }
        private string copyId;

        /// <summary>
        /// Execute command
        /// </summary>
        [PermissionSet(SecurityAction.Demand, Name = "FullTrust")]
        public override void ExecuteCmdlet()
        {
            long taskId = GetAvailableTaskId();
            Task task = null;

            switch (ParameterSetName)
            {
                case NameParameterSet:
                    task = StopCopyBlob(ContainerName, BlobName, copyId, taskId);
                    break;
                case ContainerPipelineParmeterSet:
                    task = StopCopyBlob(CloudBlobContainer, BlobName, copyId, taskId);
                    break;
                case BlobPipelineParameterSet:
                    task = StopCopyBlob(ICloudBlob, copyId, taskId);
                    break;
            }

            RunConcurrentTask(task, taskId);
        }

        /// <summary>
        /// Stop copy operation by name
        /// </summary>
        /// <param name="containerName">Container name</param>
        /// <param name="blobName">Blob name</param>
        /// <param name="copyId">copy id</param>
        private async Task StopCopyBlob(string containerName, string blobName, string copyId, long taskId)
        {
            CloudBlobContainer container = Channel.GetContainerReference(containerName);
            await StopCopyBlob(container, blobName, copyId, taskId);
        }

        /// <summary>
        /// Stop copy operation by CloudBlobContainer
        /// </summary>
        /// <param name="container">CloudBlobContainer object</param>
        /// <param name="blobName">Blob name</param>
        /// <param name="copyId">Copy id</param>
        private async Task StopCopyBlob(CloudBlobContainer container, string blobName, string copyId, long taskId)
        {
            ValidateBlobName(blobName);

            ValidateContainerName(container.Name);

            AccessCondition accessCondition = null;
            BlobRequestOptions options = null;
            ICloudBlob blob = default(ICloudBlob);

            try
            {
                blob = await Channel.GetBlobReferenceFromServerAsync(container, blobName, accessCondition,
                    options, OperationContext, CmdletCancellationToken);
            }
            catch (StorageException e)
            {
                if (e.IsNotFoundException())
                {
                    throw new ResourceNotFoundException(String.Format(Resources.BlobNotFound, blobName, container.Name));
                }
            }

            await StopCopyBlob(blob, copyId, taskId);
        }

        /// <summary>
        /// confirm to abort copy operation
        /// </summary>
        /// <param name="msg">Confirmation message</param>
        /// <returns>True if the opeation is confirmed, otherwise return false</returns>
        internal virtual bool ConfirmAbort(string msg)
        {
            return ShouldProcess(msg);
        }

        /// <summary>
        /// Stop copy operation by ICloudBlob object
        /// </summary>
        /// <param name="blob">ICloudBlob object</param>
        /// <param name="copyId">Copy id</param>
        private async Task StopCopyBlob(ICloudBlob blob, string copyId, long taskId)
        {
            AccessCondition accessCondition = null;
            BlobRequestOptions abortRequestOption = new BlobRequestOptions();

            //Set no retry to resolve the 409 conflict exception
            abortRequestOption.RetryPolicy = new NoRetry();

            if (null == blob)
            {
                throw new ArgumentException(String.Format(Resources.ObjectCannotBeNull, typeof(ICloudBlob).Name));
            }

            string specifiedCopyId = copyId;

            if (string.IsNullOrEmpty(specifiedCopyId))
            {
                if (blob.CopyState != null)
                {
                    specifiedCopyId = blob.CopyState.CopyId;
                }
            }

            string abortCopyId = string.Empty;

            if (Force)
            {
                //Make sure we use the correct copy id to abort
                //Use default retry policy for FetchBlobAttributes
                BlobRequestOptions options = null;
                await Channel.FetchBlobAttributesAsync(blob, accessCondition, options, OperationContext, CmdletCancellationToken);

                if (blob.CopyState == null || String.IsNullOrEmpty(blob.CopyState.CopyId))
                {
                    throw new ArgumentException(String.Format(Resources.CopyTaskNotFound, blob.Name, blob.Container.Name));
                }
                else
                {
                    abortCopyId = blob.CopyState.CopyId;
                }
            }
            else
            {
                abortCopyId = specifiedCopyId;
            }

            await Channel.AbortCopyAsync(blob, abortCopyId, accessCondition, abortRequestOption,
                OperationContext, CmdletCancellationToken);

            string message = String.Format(Resources.StopCopyBlobSuccessfully, blob.Name, blob.Container.Name);
            OutputStream.WriteObject(taskId, message);
        }
    }
}
