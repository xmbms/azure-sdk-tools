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

namespace Microsoft.WindowsAzure.Commands.Storage.Blob.Cmdlet
{
    using System;
    using System.IO;
    using System.Management.Automation;
    using System.Security;
    using System.Security.Permissions;
    using Common;
    using Microsoft.WindowsAzure.Commands.Storage.Utilities;
    using Microsoft.WindowsAzure.Management.Storage.Blob;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using Model.Contract;
    using Model.ResourceModel;

    [Cmdlet(VerbsCommon.Get, StorageNouns.BlobContent, ConfirmImpact = ConfirmImpact.High, DefaultParameterSetName = ManualParameterSet),
        OutputType(typeof(AzureStorageBlob))]
    public class GetAzureStorageBlobContentCommand : StorageDataMovementCmdletBase
    {
        /// <summary>
        /// manually set the name parameter
        /// </summary>
        private const string ManualParameterSet = "ReceiveManual";

        /// <summary>
        /// blob pipeline
        /// </summary>
        private const string BlobParameterSet = "BlobPipeline";

        /// <summary>
        /// container pipeline
        /// </summary>
        private const string ContainerParameterSet = "ContainerPipeline";

        [Parameter(HelpMessage = "Azure Blob Object", Mandatory = true,
            ValueFromPipelineByPropertyName = true, ParameterSetName = BlobParameterSet)]
        [ValidateNotNull]
        public ICloudBlob ICloudBlob { get; set; }

        [Parameter(HelpMessage = "Azure Container Object", Mandatory = true,
            ValueFromPipelineByPropertyName = true, ParameterSetName = ContainerParameterSet)]
        [ValidateNotNull]
        public CloudBlobContainer CloudBlobContainer { get; set; }

        [Parameter(Position = 0, HelpMessage = "Blob name",
            Mandatory = true, ParameterSetName = ManualParameterSet)]
        [Parameter(Position = 0, HelpMessage = "Blob name",
            Mandatory = true, ParameterSetName = ContainerParameterSet)]
        public string Blob
        {
            get { return BlobName; }
            set { BlobName = value; }
        }
        private string BlobName = String.Empty;

        [Parameter(Position = 1, HelpMessage = "Container name",
            Mandatory = true, ParameterSetName = ManualParameterSet)]
        public string Container 
        {
            get { return ContainerName; }
            set { ContainerName = value; }
        }
        private string ContainerName = String.Empty;

        [Alias("Path")]
        [Parameter(HelpMessage = "File Path")]
        public string Destination
        {
            get { return FileName; }
            set { FileName = value; }
        }
        public string FileName = String.Empty;

        [Parameter(HelpMessage = "check the md5sum")]
        public SwitchParameter CheckMd5
        {
            get { return checkMd5; }
            set { checkMd5 = value; }
        }
        private bool checkMd5;

        private AzureToFileSystemFileNameResolver fileNameResolver;

        /// <summary>
        /// Initializes a new instance of the GetAzureStorageBlobContentCommand class.
        /// </summary>
        public GetAzureStorageBlobContentCommand()
            : this(null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the GetAzureStorageBlobContentCommand class.
        /// </summary>
        /// <param name="channel">IStorageBlobManagement channel</param>
        public GetAzureStorageBlobContentCommand(IStorageBlobManagement channel)
        {
            Channel = channel;
            fileNameResolver = new AzureToFileSystemFileNameResolver(() => NameUtil.WindowsMaxFileNameLength);
        }

        /// <summary>
        /// Download blob to local file
        /// </summary>
        /// <param name="blob">Source blob object</param>
        /// <param name="filePath">Destination file path</param>
        internal virtual void DownloadBlob(ICloudBlob blob, string filePath)
        {
            int id = RecordIdUtil.GetRecordId();
            string activity = String.Format(Resources.ReceiveAzureBlobActivity, blob.Name, filePath);
            string status = Resources.PrepareDownloadingBlob;
            ProgressRecord pr = new ProgressRecord(id, activity, status);
            DataMovementUserData data = new DataMovementUserData()
            {
                Data = blob,
                TaskId = GetAvailableTaskId(),
                Record = pr
            };

            Action<BlobTransferManager> taskAction = (transferManager) => transferManager.QueueDownload(blob, filePath, checkMd5, OnTaskStart, OnTaskProgress, OnTaskFinish, data);

            StartAsyncTaskInTransferManager(taskAction);
        }

        /// <summary>
        /// get blob content
        /// </summary>
        /// <param name="containerName">source container name</param>
        /// <param name="blobName">source blob name</param>
        /// <param name="fileName">file name</param>
        /// <returns>the downloaded AzureStorageBlob object</returns>
        internal AzureStorageBlob GetBlobContent(string containerName, string blobName, string fileName)
        {
            if (!NameUtil.IsValidBlobName(blobName))
            {
                throw new ArgumentException(String.Format(Resources.InvalidBlobName, blobName));
            }

            if (!NameUtil.IsValidContainerName(containerName))
            {
                throw new ArgumentException(String.Format(Resources.InvalidContainerName, containerName));
            }

            CloudBlobContainer container = Channel.GetContainerReference(containerName);
            BlobRequestOptions requestOptions = null;
            AccessCondition accessCondition = null;

            //FIXME
            ICloudBlob blob = Channel.GetBlobReferenceFromServer(container, blobName, accessCondition, requestOptions, OperationContext);
                
            if (null == blob)
            {
                throw new ResourceNotFoundException(String.Format(Resources.BlobNotFound, blobName, containerName));
            }

            return GetBlobContent(blob, fileName, true);
        }

        /// <summary>
        /// get blob content
        /// </summary>
        /// <param name="container">source container object</param>
        /// <param name="blobName">source blob name</param>
        /// <param name="fileName">destination file name</param>
        /// <returns>the downloaded AzureStorageBlob object</returns>
        internal AzureStorageBlob GetBlobContent(CloudBlobContainer container, string blobName, string fileName)
        {
            if (!NameUtil.IsValidBlobName(blobName))
            {
                throw new ArgumentException(String.Format(Resources.InvalidBlobName, blobName));
            }

            string filePath = GetFullReceiveFilePath(fileName, blobName, null);

            ValidatePipelineCloudBlobContainer(container);
            AccessCondition accessCondition = null;
            BlobRequestOptions requestOptions = null;
            ICloudBlob blob = Channel.GetBlobReferenceFromServer(container, blobName, accessCondition, requestOptions, OperationContext);
            
            if (null == blob)
            {
                throw new ResourceNotFoundException(String.Format(Resources.BlobNotFound, blobName, container.Name));
            }

            return GetBlobContent(blob, filePath, true);
        }

        /// <summary>
        /// get blob content
        /// </summary>
        /// <param name="blob">source ICloudBlob object</param>
        /// <param name="fileName">destination file path</param>
        /// <param name="isValidBlob">whether the source container validated</param>
        /// <returns>the downloaded AzureStorageBlob object</returns>
        internal AzureStorageBlob GetBlobContent(ICloudBlob blob, string fileName, bool isValidBlob = false)
        {
            if (null == blob)
            {
                throw new ArgumentNullException(typeof(ICloudBlob).Name, String.Format(Resources.ObjectCannotBeNull, typeof(ICloudBlob).Name));
            }

            //skip download the snapshot except the ICloudBlob pipeline
            if (IsSnapshot(blob) && ParameterSetName != BlobParameterSet)
            {
                WriteWarning(String.Format(Resources.SkipDownloadSnapshot, blob.Name, blob.SnapshotTime));
                return null;
            }

            string filePath = GetFullReceiveFilePath(fileName, blob.Name, blob.SnapshotTime);

            if (!overwrite && File.Exists(filePath))
            {
                if (!ConfirmOverwrite(filePath))
                {
                    WriteWarning(String.Format(Resources.FileAlreadyExists, filePath));
                    return null;
                }
            }

            if (!isValidBlob)
            {
                ValidatePipelineICloudBlob(blob);
            }

            //create the destination directory if not exists.
            String dirPath = Path.GetDirectoryName(filePath);

            if (!Directory.Exists(dirPath))
            {
                Directory.CreateDirectory(dirPath);
            }

            //FIXME
            //AccessCondition accessCondition = null;
            //BlobRequestOptions requestOptions = null;

            try
            {
                DownloadBlob(blob, filePath);
                //FIXME
                //Channel.FetchBlobAttributes(blob, accessCondition, requestOptions, OperationContext);
            }
            catch (Exception e)
            {
                WriteDebugLog(String.Format(Resources.DownloadBlobFailed, blob.Name, blob.Container.Name, filePath, e.Message));
                throw;
            }

            return new AzureStorageBlob(blob);
        }

        /// <summary>
        /// get full file path according to the specified file name
        /// </summary>
        /// <param name="fileName">File name</param>
        /// <param name="blobName">Source blob name</param>
        /// <param name="snapshotTime">Source blob snapshot time</param>
        /// <returns>full file path if file path is valid, otherwise throw an exception</returns>
        internal string GetFullReceiveFilePath(string fileName, string blobName, DateTimeOffset? snapshotTime)
        {
            String filePath = Path.Combine(CurrentPath(), fileName);
            fileName = Path.GetFileName(filePath);
            String dirPath = Path.GetDirectoryName(filePath);

            if (!String.IsNullOrEmpty(dirPath) && !Directory.Exists(dirPath))
            {
                throw new ArgumentException(String.Format(Resources.DirectoryNotExists, dirPath));
            }

            if (string.IsNullOrEmpty(fileName) || Directory.Exists(filePath))
            {
                fileName = fileNameResolver.ResolveFileName(blobName, snapshotTime);
                filePath = Path.Combine(filePath, fileName);
            }

            fileName = Path.GetFileName(filePath);

            if (!NameUtil.IsValidFileName(fileName))
            {
                throw new ArgumentException(String.Format(Resources.InvalidFileName, fileName));
            }

            //there is no need to check the read/write permission on the specified file path, the datamovement libraray will do that

            return filePath;
        }

        /// <summary>
        /// execute command
        /// </summary>
        [PermissionSet(SecurityAction.Demand, Name = "FullTrust")]
        public override void ExecuteCmdlet()
        {
            AzureStorageBlob azureBlob = null;
            string blobName = string.Empty;
            string containerName = string.Empty;

            switch (ParameterSetName)
            {
                case BlobParameterSet:
                    azureBlob = GetBlobContent(ICloudBlob, FileName, true);
                    blobName = ICloudBlob.Name;
                    containerName = ICloudBlob.Container.Name;
                    break;

                case ContainerParameterSet:
                    azureBlob = GetBlobContent(CloudBlobContainer, BlobName, FileName);
                    blobName = BlobName;
                    containerName = CloudBlobContainer.Name;
                    break;

                case ManualParameterSet:
                    azureBlob = GetBlobContent(ContainerName, BlobName, FileName);
                    blobName = BlobName;
                    containerName = ContainerName;
                    break;
            }

            if (azureBlob == null)
            {
                String result = String.Format(Resources.DownloadBlobCancelled, blobName, containerName);
                WriteObject(result);
            }
            else
            {
                WriteObjectWithStorageContext(azureBlob);
            }
        }

        /// <summary>
        /// On Task run successfully
        /// </summary>
        /// <param name="data">User data</param>
        protected override void OnTaskSuccessful(DataMovementUserData data)
        {
            ICloudBlob blob = data.Data as ICloudBlob;

            if (blob != null)
            {
                AzureStorageBlob azureBlob = new AzureStorageBlob(blob);
                OutputStream.WriteObject(data.TaskId, azureBlob);
            }
        }
    }
}
