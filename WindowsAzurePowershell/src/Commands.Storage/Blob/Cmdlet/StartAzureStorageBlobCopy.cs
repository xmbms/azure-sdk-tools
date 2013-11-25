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
    using Common;
    using Microsoft.WindowsAzure.Management.Storage.Blob;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using Model.Contract;
    using Model.ResourceModel;

    [Cmdlet(VerbsLifecycle.Start, StorageNouns.CopyBlob, ConfirmImpact = ConfirmImpact.High, DefaultParameterSetName = NameParameterSet),
       OutputType(typeof(AzureStorageBlob))]
    public class StartAzureStorageBlobCopy : StorageDataMovementCmdletBase
    {
        /// <summary>
        /// Blob Pipeline parameter set name
        /// </summary>
        private const string SrcBlobParameterSet = "BlobPipeline";

        /// <summary>
        /// Blob Pipeline parameter set name
        /// </summary>
        private const string DestBlobPipelineParameterSet = "DestBlobPipeline";

        /// <summary>
        /// Container pipeline paremeter set name
        /// </summary>
        private const string ContainerPipelineParameterSet = "ContainerPipeline";

        /// <summary>
        /// Blob name and container name parameter set
        /// </summary>
        private const string NameParameterSet = "NamePipeline";

        /// <summary>
        /// Source uri parameter set
        /// </summary>
        private const string UriParameterSet = "UriPipeline";

        [Alias("SrcICloudBlob")]
        [Parameter(HelpMessage = "ICloudBlob Object", Mandatory = true,
            ValueFromPipelineByPropertyName = true, ParameterSetName = SrcBlobParameterSet)]
        [Parameter(HelpMessage = "ICloudBlob Object", Mandatory = true,
            ValueFromPipelineByPropertyName = true, ParameterSetName = DestBlobPipelineParameterSet)]
        public ICloudBlob ICloudBlob { get; set; }

        [Parameter(HelpMessage = "CloudBlobContainer Object", Mandatory = true,
            ValueFromPipelineByPropertyName = true, ParameterSetName = ContainerPipelineParameterSet)]
        public CloudBlobContainer CloudBlobContainer { get; set; }

        [Parameter(ParameterSetName = ContainerPipelineParameterSet, Mandatory = true, Position = 0, HelpMessage = "Blob name")]
        [Parameter(ParameterSetName = NameParameterSet, Mandatory = true, Position = 0, HelpMessage = "Blob name")]
        public string SrcBlob
        {
            get { return BlobName; }
            set { BlobName = value; }
        }
        private string BlobName = String.Empty;

        [Parameter(HelpMessage = "Source Container name", Mandatory = true,
            ParameterSetName = NameParameterSet)]
        [ValidateNotNullOrEmpty]
        public string SrcContainer
        {
            get { return ContainerName; }
            set { ContainerName = value; }
        }
        private string ContainerName = String.Empty;

        [Alias("SrcUri")]
        [Parameter(HelpMessage = "Source blob uri", Mandatory = true,
            ValueFromPipelineByPropertyName = true, ParameterSetName = UriParameterSet)]
        public string AbsoluteUri { get; set; }

        [Parameter(HelpMessage = "Destination container name", Mandatory = true,
            ParameterSetName = NameParameterSet)]
        [Parameter(HelpMessage = "Destination container name", Mandatory = true,
            ParameterSetName = UriParameterSet)]
        [Parameter(HelpMessage = "Destination container name", Mandatory = true,
            ParameterSetName = SrcBlobParameterSet)]
        [Parameter(HelpMessage = "Destination container name", Mandatory = true,
            ParameterSetName = ContainerPipelineParameterSet)]
        public string DestContainer { get; set; }

        [Parameter(HelpMessage = "Destination blob name", Mandatory = true,
            ParameterSetName = UriParameterSet)]
        [Parameter(HelpMessage = "Destination blob name", Mandatory = false,
            ParameterSetName = NameParameterSet)]
        [Parameter(HelpMessage = "Destination blob name", Mandatory = false,
            ParameterSetName = SrcBlobParameterSet)]
        [Parameter(HelpMessage = "Destination container name", Mandatory = false,
            ParameterSetName = ContainerPipelineParameterSet)]
        public string DestBlob { get; set; }

        [Parameter(HelpMessage = "Destination ICloudBlob object", Mandatory = true,
            ParameterSetName = DestBlobPipelineParameterSet)]
        public ICloudBlob DestICloudBlob { get; set; }

        [Alias("SrcContext")]
        [Parameter(HelpMessage = "Source Azure Storage Context Object",
            ValueFromPipelineByPropertyName = true, ParameterSetName = NameParameterSet)]
        [Parameter(HelpMessage = "Source Azure Storage Context Object",
            ValueFromPipelineByPropertyName = true, ParameterSetName = SrcBlobParameterSet)]
        [Parameter(HelpMessage = "Source Azure Storage Context Object",
            ValueFromPipelineByPropertyName = true, ParameterSetName = DestBlobPipelineParameterSet)]
        [Parameter(HelpMessage = "Source Azure Storage Context Object",
            ValueFromPipelineByPropertyName = true, ParameterSetName = ContainerPipelineParameterSet)]
        [Parameter(HelpMessage = "Source Azure Storage Context Object", ParameterSetName = UriParameterSet)]
        public override AzureStorageContext Context { get; set; }

        [Parameter(HelpMessage = "Destination Storage context object", Mandatory = false)]
        public AzureStorageContext DestContext { get; set; }

        /// <summary>
        /// Destination Service Channel object
        /// </summary>
        private IStorageBlobManagement destChannel;

        private bool skipSourceChannelInit;

        /// <summary>
        /// Create blob client and storage service management channel if need to.
        /// </summary>
        /// <returns>IStorageManagement object</returns>
        protected override IStorageBlobManagement CreateChannel()
        {
            //Init storage blob management channel
            if (skipSourceChannelInit)
            {
                return null;
            }
            else
            {
                return base.CreateChannel();
            }
        }

        /// <summary>
        /// Begin cmdlet processing
        /// </summary>
        protected override void BeginProcessing()
        {
            if (ParameterSetName == UriParameterSet)
            {
                skipSourceChannelInit = true;
            }

            base.BeginProcessing();
        }

        /// <summary>
        /// Set up the Channel object for Destination container and blob
        /// </summary>
        internal void SetUpDestinationChannel()
        {
            //If destChannel exits, reuse it.
            //If desContext exits, use it.
            //If Channl object exists, use it.
            //Otherwise, create a new channel.
            if (destChannel == null)
            {
                if (DestContext == null)
                {
                    if (Channel != null)
                    {
                        destChannel = Channel;
                    }
                    else
                    {
                        destChannel = base.CreateChannel();
                    }
                }
                else
                {
                    destChannel = CreateChannel(DestContext.StorageAccount);
                }
            }
        }

        /// <summary>
        /// Execute command
        /// </summary>
        [PermissionSet(SecurityAction.Demand, Name = "FullTrust")]
        public override void ExecuteCmdlet()
        {
            SetUpDestinationChannel();

            bool successful = true;
            string destBlobName = string.Empty;
            string destContainerName = string.Empty;

            switch (ParameterSetName)
            {
                case NameParameterSet:
                    successful = StartCopyBlob(SrcContainer, SrcBlob, DestContainer, DestBlob);
                    destBlobName = string.IsNullOrEmpty(DestBlob) ? SrcBlob : DestBlob;
                    destContainerName = DestContainer;
                    break;

                case UriParameterSet:
                    successful = StartCopyBlob(AbsoluteUri, DestContainer, DestBlob, Context);
                    destBlobName = DestBlob;
                    destContainerName = DestContainer;
                    break;

                case SrcBlobParameterSet:
                    successful = StartCopyBlob(ICloudBlob, DestContainer, DestBlob);
                    destBlobName = string.IsNullOrEmpty(DestBlob) ? ICloudBlob.Name : DestBlob;
                    destContainerName = DestContainer;
                    break;

                case ContainerPipelineParameterSet:
                    successful = StartCopyBlob(CloudBlobContainer.Name, SrcBlob, DestContainer, DestBlob);
                    destBlobName = string.IsNullOrEmpty(DestBlob) ? SrcBlob : DestBlob;
                    destContainerName = DestContainer;
                    break;

                case DestBlobPipelineParameterSet:
                    successful = StartCopyBlob(ICloudBlob, DestICloudBlob);
                    destBlobName = DestICloudBlob.Name;
                    destContainerName = DestICloudBlob.Container.Name;
                    break;
            }

            if (successful)
            {
                String result = String.Format(Resources.BlobTransmitStartSuccessfully, destBlobName, destContainerName);
                WriteVerbose(result);
            }
            else
            {
                String result = String.Format(Resources.BlobTransmitCanceled, destBlobName, destContainerName);
                WriteVerbose(result);
            }
        }

        /// <summary>
        /// Start copy operation by source and destination ICloudBlob object
        /// </summary>
        /// <param name="srcICloudBlob">Source ICloudBlob object</param>
        /// <param name="destICloudBlob">Destination ICloudBlob object</param>
        /// <returns>Destination ICloudBlob object</returns>
        private bool StartCopyBlob(ICloudBlob srcICloudBlob, ICloudBlob destICloudBlob)
        {
            return StartCopyInTransferManager(srcICloudBlob, destICloudBlob.Container, destICloudBlob.Name);
        }

        /// <summary>
        /// Start copy operation by source ICloudBlob object
        /// </summary>
        /// <param name="srcICloudBlob">Source ICloudBlob object</param>
        /// <param name="destContainer">Destinaion container name</param>
        /// <param name="destBlobName">Destination blob name</param>
        /// <returns>Destination ICloudBlob object</returns>
        private bool StartCopyBlob(ICloudBlob srcICloudBlob, string destContainer, string destBlobName)
        {
            CloudBlobContainer container = destChannel.GetContainerReference(destContainer);
            return StartCopyInTransferManager(srcICloudBlob, container, destBlobName);
        }

        /// <summary>
        /// Start copy operation by source uri
        /// </summary>
        /// <param name="srcICloudBlob">Source uri</param>
        /// <param name="destContainer">Destinaion container name</param>
        /// <param name="destBlobName">Destination blob name</param>
        /// <returns>Destination ICloudBlob object</returns>
        private bool StartCopyBlob(string srcUri, string destContainer, string destBlobName, AzureStorageContext context)
        {
            if (context != null)
            {
                Uri sourceUri = new Uri(srcUri);
                Uri contextUri = new Uri(context.BlobEndPoint);

                if (sourceUri.Host.ToLower() == contextUri.Host.ToLower())
                {
                    CloudBlobClient blobClient = context.StorageAccount.CreateCloudBlobClient();
                    ICloudBlob blobReference = blobClient.GetBlobReferenceFromServer(sourceUri);
                    return StartCopyBlob(blobReference, destContainer, destBlobName);
                }
                else
                {
                    WriteWarning(String.Format(Resources.StartCopySourceContextMismatch, srcUri, context.BlobEndPoint));
                }
            }

            CloudBlobContainer container = destChannel.GetContainerReference(destContainer);
            return StartCopyInTransferManager(new Uri(srcUri), container, destBlobName);
        }

        /// <summary>
        /// Start copy operation by container name and blob name
        /// </summary>
        /// <param name="srcContainerName">Source container name</param>
        /// <param name="srcBlobName">Source blob name</param>
        /// <param name="destContainer">Destinaion container name</param>
        /// <param name="destBlobName">Destination blob name</param>
        /// <returns>Destination ICloudBlob object</returns>
        private bool StartCopyBlob(string srcContainerName, string srcBlobName, string destContainerName, string destBlobName)
        {
            ValidateBlobName(srcBlobName);
            ValidateContainerName(srcContainerName);
            ValidateContainerName(destContainerName);

            if (string.IsNullOrEmpty(destBlobName))
            {
                destBlobName = srcBlobName;
            }

            ValidateBlobName(destBlobName);

            AccessCondition accessCondition = null;
            BlobRequestOptions options = null;
            CloudBlobContainer container = Channel.GetContainerReference(srcContainerName);
            ICloudBlob blob = Channel.GetBlobReferenceFromServer(container, srcBlobName, accessCondition, options, OperationContext);

            if (blob == null)
            {
                throw new ResourceNotFoundException(String.Format(Resources.BlobNotFound, srcBlobName, srcContainerName));
            }

            CloudBlobContainer destContainer = destChannel.GetContainerReference(destContainerName);
            return StartCopyInTransferManager(blob, destContainer, destBlobName);
        }

        /// <summary>
        /// Start copy using transfer mangager by source ICloudBlob object
        /// </summary>
        /// <param name="blob">Source ICloudBlob object</param>
        /// <param name="destContainer">Destination CloudBlobContainer object</param>
        /// <param name="destBlobName">Destination blob name</param>
        /// <returns>Destination ICloudBlob object</returns>
        private bool StartCopyInTransferManager(ICloudBlob blob, CloudBlobContainer destContainer, string destBlobName)
        {
            if (string.IsNullOrEmpty(destBlobName))
            {
                destBlobName = blob.Name;
            }

            ValidateBlobName(blob.Name);
            ValidateContainerName(destContainer.Name);
            ValidateBlobName(destBlobName);

            Dictionary<string, string> BlobPath = new Dictionary<string, string>()
            {
                {"Container", destContainer.Name},
                {"Blob", destBlobName}
            };

            DataMovementUserData data = new DataMovementUserData()
            {
                Data = BlobPath,
                TaskId = GetAvailableTaskId(),
                Record = null
            };

            //There is no need to use progress record for start-copy
            transferManager.QueueBlobStartCopy(blob, destContainer, destBlobName, null, OnCopyTaskFinish, data);
            RunConcurrentTask(data.taskSource.Task, data.TaskId);
            return true;
        }

        /// <summary>
        /// Start copy using transfer mangager by source uri
        /// </summary>
        /// <param name="uri">source uri</param>
        /// <param name="destContainer">Destination CloudBlobContainer object</param>
        /// <param name="destBlobName">Destination blob name</param>
        /// <returns>Destination ICloudBlob object</returns>
        private bool StartCopyInTransferManager(Uri uri, CloudBlobContainer destContainer, string destBlobName)
        {
            ValidateContainerName(destContainer.Name);
            ValidateBlobName(destBlobName);

            Dictionary<string, string> BlobPath = new Dictionary<string, string>()
            {
                {"Container", destContainer.Name},
                {"Blob", destBlobName}
            };

            DataMovementUserData data = new DataMovementUserData()
            {
                Data = BlobPath,
                TaskId = GetAvailableTaskId(),
                Record = null
            };

            //There is no need to use progress record for start-copy
            transferManager.QueueBlobStartCopy(uri, destContainer, destBlobName, null, OnCopyTaskFinish, data);
            RunConcurrentTask(data.taskSource.Task, data.TaskId);
            return true;
        }

        /// <summary>
        /// Get DestinationBlob with specified copy id
        /// </summary>
        /// <param name="container">CloudBlobContainer object</param>
        /// <param name="blobName">Blob name</param>
        /// <param name="copyId">Current CopyId</param>
        /// <returns>Destination ICloudBlob object</returns>
        private ICloudBlob GetDestinationBlobWithCopyId(CloudBlobContainer container, string blobName)
        {
            AccessCondition accessCondition = null;
            BlobRequestOptions options = null;
            ICloudBlob blob = destChannel.GetBlobReferenceFromServer(container, blobName, accessCondition, options, OperationContext);
            return blob;
        }

        private void OnCopyTaskFinish(object userData, string copyId, Exception e)
        {
            DataMovementUserData data = userData as DataMovementUserData;

            if (data != null)
            {

                Dictionary<string, string> destBlobPath = data.Data as Dictionary<string, string>;

                if (destBlobPath != null)
                {
                    VerboseStream.WriteStream(String.Format(Resources.CopyDestinationBlobPending, destBlobPath["Blob"], destBlobPath["Container"], copyId));
                }
                else
                {
                    VerboseStream.WriteStream(copyId);
                }

                CloudBlobContainer container = destChannel.GetContainerReference(destBlobPath["Container"]);
                ICloudBlob destBlob = GetDestinationBlobWithCopyId(container, destBlobPath["Blob"]);
                AzureStorageBlob azureBlob = new AzureStorageBlob(destBlob);
                //Make sure the dest context is piped out
                azureBlob.Context = DestContext;
                OutputStream.WriteObject(data.TaskId, azureBlob);
            }

            OnTaskFinish(userData, e);
        }
    }
}
