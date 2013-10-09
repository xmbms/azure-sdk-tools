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
    using System.Collections;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Management.Automation;
    using Common;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using Model.Contract;
    using Model.ResourceModel;

    /// <summary>
    /// download blob from azure
    /// </summary>
    [Cmdlet(VerbsCommon.Set, StorageNouns.BlobContent, ConfirmImpact = ConfirmImpact.High, DefaultParameterSetName = ManualParameterSet),
        OutputType(typeof(AzureStorageBlob))]
    public class SetAzureBlobContentCommand : StorageDataMovementCmdletBase
    {
        /// <summary>
        /// default parameter set name
        /// </summary>
        private const string ManualParameterSet = "SendManual";

        /// <summary>
        /// blob pipeline
        /// </summary>
        private const string BlobParameterSet = "BlobPipeline";

        /// <summary>
        /// container pipeline
        /// </summary>
        private const string ContainerParameterSet = "ContainerPipeline";

        /// <summary>
        /// block blob type
        /// </summary>
        private const string BlockBlobType = "Block";

        /// <summary>
        /// page blob type
        /// </summary>
        private const string PageBlobType = "Page";

        [Alias("FullName")]
        [Parameter(Position = 0, Mandatory = true, HelpMessage = "file Path",
            ValueFromPipelineByPropertyName = true, ParameterSetName = ManualParameterSet)]
        [Parameter(Position = 0, Mandatory = true, HelpMessage = "file Path",
            ParameterSetName = ContainerParameterSet)]
        [Parameter(Position = 0, Mandatory = true, HelpMessage = "file Path",
            ParameterSetName = BlobParameterSet)]
        public string File
        {
            get { return FileName; }
            set { FileName = value; }
        }
        private string FileName = String.Empty;

        [Parameter(Position = 1, HelpMessage = "Container name", Mandatory = true, ParameterSetName = ManualParameterSet)]
        public string Container
        {
            get { return ContainerName; }
            set { ContainerName = value; }
        }
        private string ContainerName = String.Empty;

        [Parameter(HelpMessage = "Blob name", ParameterSetName = ManualParameterSet)]
        [Parameter(HelpMessage = "Blob name", ParameterSetName = ContainerParameterSet)]
        public string Blob
        {
            get { return BlobName; }
            set { BlobName = value; }
        }
        public string BlobName = String.Empty;

        [Parameter(HelpMessage = "Azure Blob Container Object", Mandatory = true,
            ValueFromPipelineByPropertyName = true,
            ParameterSetName = ContainerParameterSet)]
        public CloudBlobContainer CloudBlobContainer { get; set; }

        [Parameter(HelpMessage = "Azure Blob Object", Mandatory = true,
            ValueFromPipelineByPropertyName = true,
            ParameterSetName = BlobParameterSet)]
        public ICloudBlob ICloudBlob { get; set; }

        [Parameter(HelpMessage = "Blob Type('Block', 'Page')")]
        [ValidateSet(BlockBlobType, PageBlobType, IgnoreCase = true)]
        public string BlobType
        {
            get { return blobType; }
            set { blobType = value; }
        }
        private string blobType = BlockBlobType;

        [Parameter(HelpMessage = "Blob Properties", Mandatory = false)]
        public Hashtable Properties
        {
            get
            {
                return BlobProperties;
            }
            set
            {
                BlobProperties = value;
            }
        }
        private Hashtable BlobProperties = null;

        [Parameter(HelpMessage = "Blob Metadata", Mandatory = false)]
        public Hashtable Metadata
        {
            get
            {
                return BlobMetadata;
            }
            set
            {
                BlobMetadata = value;
            }
        }

        private Hashtable BlobMetadata = null;

        /// <summary>
        /// Amount of concurrent async tasks to run per available core.
        /// </summary>
        [Parameter(HelpMessage = "The total amount of concurrent async tasks. The default value is ProcessorCount * 8")]
        public int ConcurrentTaskCount
        {
            get { return concurrentTaskCount; }
            set { concurrentTaskCount = value; }
        }

        /// <summary>
        /// the root dir for sending file
        /// make sure the root dir is lower case.
        /// </summary>
        private string sendRootDir = String.Empty;

        /// <summary>
        /// Initializes a new instance of the SetAzureBlobContentCommand class.
        /// </summary>
        public SetAzureBlobContentCommand()
            : this(null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the SetAzureBlobContentCommand class.
        /// </summary>
        /// <param name="channel">IStorageBlobManagement channel</param>
        public SetAzureBlobContentCommand(IStorageBlobManagement channel)
        {
            Channel = channel;
        }

        /// <summary>
        /// upload file to azure blob
        /// </summary>
        /// <param name="filePath">local file path</param>
        /// <param name="blob">destination azure blob object</param>
        internal virtual void Upload2Blob(string filePath, ICloudBlob blob)
        {
            int id = 0;
            string activity = String.Format(Resources.SendAzureBlobActivity, filePath, blob.Name, blob.Container.Name);
            string status = Resources.PrepareUploadingBlob;
            ProgressRecord pr = new ProgressRecord(id, activity, status);

            Action<BlobTransferManager> taskAction = delegate(BlobTransferManager transferManager)
            {
                transferManager.QueueUpload(blob, filePath, OnTaskStart, OnTaskProgress, OnTaskFinish, pr);
            };

            StartSyncTaskInTransferManager(taskAction, pr);
        }

        /// <summary>
        /// get full file path according to the specified file name
        /// </summary>
        /// <param name="fileName">file name</param>
        /// <returns>full file path if fileName is valid, empty string if file name is directory</returns>
        internal string GetFullSendFilePath(string fileName)
        {
            if (string.IsNullOrEmpty(fileName))
            {
                throw new ArgumentException(Resources.FileNameCannotEmpty);
            }

            String filePath = Path.Combine(CurrentPath(), fileName);

            if (!System.IO.File.Exists(filePath))
            {
                if (System.IO.Directory.Exists(filePath))
                {
                    WriteWarning(String.Format(Resources.CannotSendDirectory, filePath));
                    filePath = string.Empty;
                }
                else
                {
                    throw new ArgumentException(String.Format(Resources.FileNotFound, filePath));
                }
            }

            return filePath;
        }

        /// <summary>
        /// get blob name according to the relative file path
        /// </summary>
        /// <param name="filePath">absolute file path</param>
        /// <returns>blob name</returns>
        internal string GetBlobNameFromRelativeFilePath(string filePath)
        {
            string blobName = string.Empty;
            string fileName = Path.GetFileName(filePath);
            string dirPath = Path.GetDirectoryName(filePath).ToLower();

            if (string.IsNullOrEmpty(sendRootDir) || !dirPath.StartsWith(sendRootDir))
            {
                //if sendRoot dir is empty or dir path is not sub folder of the sending root dir
                //set the current dir as the root sending dir
                sendRootDir = dirPath + Path.DirectorySeparatorChar;
                blobName = fileName;
            }
            else
            {
                blobName = filePath.Substring(sendRootDir.Length);
            }

            return blobName;
        }

        /// <summary>
        /// set azure blob content
        /// </summary>
        /// <param name="fileName">local file path</param>
        /// <param name="containerName">container name</param>
        /// <param name="blobName">blob name</param>
        /// <returns>null if user cancel the overwrite operation, otherwise return destination blob object</returns>
        internal AzureStorageBlob SetAzureBlobContent(string fileName, string containerName, string blobName)
        {
            CloudBlobContainer container = Channel.GetContainerReference(containerName);
            return SetAzureBlobContent(fileName, container, blobName);
        }

        /// <summary>
        /// set azure blob content
        /// </summary>
        /// <param name="fileName">local file path</param>
        /// <param name="container">destination container</param>
        /// <param name="blobName">blob name</param>
        /// <returns>null if user cancel the overwrite operation, otherwise return destination blob object</returns>
        internal AzureStorageBlob SetAzureBlobContent(string fileName, CloudBlobContainer container, string blobName)
        {
            string filePath = GetFullSendFilePath(fileName);

            if (string.IsNullOrEmpty(filePath))
            {
                return null;
            }

            ValidatePipelineCloudBlobContainer(container);

            if (string.IsNullOrEmpty(blobName))
            {
                blobName = GetBlobNameFromRelativeFilePath(filePath);
            }

            ICloudBlob blob = null;

            switch (CultureInfo.CurrentCulture.TextInfo.ToTitleCase(blobType))
            {
                case PageBlobType:
                    blob = container.GetPageBlobReference(blobName);
                    break;

                case BlockBlobType:
                default:
                    blob = container.GetBlockBlobReference(blobName);
                    break;
            }

            return SetAzureBlobContent(fileName, blob);
        }

        /// <summary>
        /// set azure blob
        /// </summary>
        /// <param name="fileName">local file name</param>
        /// <param name="blob">destination blob</param>
        /// <param name="isValidContainer">whether the destination container is validated</param>
        /// <returns>null if user cancel the overwrite operation, otherwise return destination blob object</returns>
        internal AzureStorageBlob SetAzureBlobContent(string fileName, ICloudBlob blob, bool isValidContainer = false)
        {
            string filePath = GetFullSendFilePath(fileName);

            if (String.IsNullOrEmpty(filePath))
            {
                return null;
            }

            if (null == blob)
            {
                throw new ArgumentException(String.Format(Resources.ObjectCannotBeNull, typeof(ICloudBlob).Name));
            }

            if (blob.BlobType == WindowsAzure.Storage.Blob.BlobType.PageBlob)
            {
                long fileSize = new FileInfo(filePath).Length;
                long pageBlobUnit = 512;
                long remainder = fileSize % pageBlobUnit;

                if (remainder != 0)
                {
                    //the blob size must be a multiple of 512 bytes.
                    throw new ArgumentException(String.Format(Resources.InvalidPageBlobSize, filePath, fileSize));
                }
            }

            if (!NameUtil.IsValidBlobName(blob.Name))
            {
                throw new ArgumentException(String.Format(Resources.InvalidBlobName, blob.Name));
            }

            if (!isValidContainer)
            {
                ValidatePipelineCloudBlobContainer(blob.Container);
            }

            AccessCondition accessCondition = null;
            BlobRequestOptions requestOptions = null;
            ICloudBlob blobRef = Channel.GetBlobReferenceFromServer(blob.Container, blob.Name, accessCondition, requestOptions, OperationContext);

            if (null != blobRef)
            {
                if (blob.BlobType != blobRef.BlobType)
                {
                    throw new ArgumentException(String.Format(Resources.BlobTypeMismatch, blobRef.Name, blobRef.BlobType));
                }
            
                if (!overwrite)
                {
                    if (!ConfirmOverwrite(blob.Name))
                    {
                        return null;
                    }
                }
            }

            try
            {
                Upload2Blob(filePath, blob);

                try
                {
                    Channel.FetchBlobAttributes(blob, accessCondition, requestOptions, OperationContext);
                }
                catch (StorageException se)
                {
                    //Storage credentials only have the write permission, and don't have the read permission.
                    if (se.IsNotFoundException())
                    {
                        WriteVerboseWithTimestamp(Resources.BlobIsNotReadable);
                    }
                    else
                    {
                        throw;
                    }
                }
            }
            catch (Exception e)
            {
                WriteDebugLog(String.Format(Resources.Upload2BlobFailed, e.Message));
                throw;
            }

            return new AzureStorageBlob(blob);
        }

        //only support the common blob properties for block blob and page blob
        //http://msdn.microsoft.com/en-us/library/windowsazure/ee691966.aspx
        private Dictionary<string, Action<BlobProperties, string>> validICloudBlobProperties = new Dictionary<string,Action<BlobProperties,string>>(StringComparer.OrdinalIgnoreCase)
            {
                {"CacheControl", (p, v) => p.CacheControl = v},
                {"ContentEncoding", (p, v) => p.ContentEncoding = v},
                {"ContentLanguage", (p, v) => p.ContentLanguage = v},
                {"ContentMD5", (p, v) => p.ContentMD5 = v},
                {"ContentType", (p, v) => p.ContentType = v},
            };

        /// <summary>
        /// check whether the blob properties is valid
        /// </summary>
        /// <param name="properties">Blob properties table</param>
        private void ValidateBlobProperties(Hashtable properties)
        {
            if(properties == null)
            {
                return ;
            }

            foreach (DictionaryEntry entry in properties)
            {
                if (!validICloudBlobProperties.ContainsKey(entry.Key.ToString()))
                {
                    throw new ArgumentException(String.Format(Resources.InvalidBlobProperties, entry.Key.ToString(), entry.Value.ToString()));
                }
            }
        }

        /// <summary>
        /// set blob properties
        /// </summary>
        /// <param name="azureBlob">ICloudBlob object</param>
        /// <param name="meta">blob properties hashtable</param>
        private void SetBlobProperties(ICloudBlob blob, Hashtable properties)
        {
            if (properties == null)
            {
                return;
            }

            foreach (DictionaryEntry entry in properties)
            {
                string key = entry.Key.ToString();
                string value = entry.Value.ToString();
                Action<BlobProperties, string> action = validICloudBlobProperties[key];

                if (action != null)
                {
                    action(blob.Properties, value);
                }
            }

            AccessCondition accessCondition = null;
            BlobRequestOptions requestOptions = null;

            Channel.SetBlobProperties(blob, accessCondition, requestOptions, OperationContext);
        }

        /// <summary>
        /// set blob properties
        /// </summary>
        /// <param name="azureBlob">azure storage blob object</param>
        /// <param name="meta">blob properties hashtable</param>
        private void SetBlobProperties(AzureStorageBlob azureBlob, Hashtable properties)
        {
            SetBlobProperties(azureBlob.ICloudBlob, properties);
        }

        /// <summary>
        /// set blob meta
        /// </summary>
        /// <param name="azureBlob">ICloudBlob object</param>
        /// <param name="meta">meta data hashtable</param>
        private void SetBlobMeta(ICloudBlob blob, Hashtable meta)
        {
            if (meta == null)
            {
                return;
            }

            foreach (DictionaryEntry entry in meta)
            {
                string key = entry.Key.ToString();
                string value = entry.Value.ToString();

                if (blob.Metadata.ContainsKey(key))
                {
                    blob.Metadata[key] = value;
                }
                else
                {
                    blob.Metadata.Add(key, value);
                }
            }

            AccessCondition accessCondition = null;
            BlobRequestOptions requestOptions = null;

            Channel.SetBlobMetadata(blob, accessCondition, requestOptions, OperationContext);
        }

        /// <summary>
        /// set blob meta
        /// </summary>
        /// <param name="azureBlob">azure storage blob object</param>
        /// <param name="meta">meta data hashtable</param>
        private void SetBlobMeta(AzureStorageBlob azureBlob, Hashtable meta)
        {
            SetBlobMeta(azureBlob.ICloudBlob, meta);
        }

        /// <summary>
        /// execute command
        /// </summary>
        public override void ExecuteCmdlet()
        {
            AzureStorageBlob blob = null;
            string containerName = string.Empty;

            if (BlobProperties != null)
            {
                ValidateBlobProperties(BlobProperties);
            }

            switch (ParameterSetName)
            {
                case ContainerParameterSet:
                    blob = SetAzureBlobContent(FileName, CloudBlobContainer, BlobName);
                    containerName = CloudBlobContainer.Name;
                    break;

                case BlobParameterSet:
                    blob = SetAzureBlobContent(FileName, ICloudBlob);
                    containerName = ICloudBlob.Container.Name;
                    break;

                case ManualParameterSet:
                default:
                    blob = SetAzureBlobContent(FileName, ContainerName, BlobName);
                    containerName = ContainerName;
                    break;
            }

            if (blob == null)
            {
                String result = String.Format(Resources.SendAzureBlobCancelled, FileName, containerName);
                WriteObject(result);
            }
            else
            {
                //set the properties and meta
                SetBlobProperties(blob, BlobProperties);
                SetBlobMeta(blob, Metadata);

                WriteObjectWithStorageContext(blob);
            }
        }
    }
}
