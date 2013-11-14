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
    using System.Globalization;
    using System.Management.Automation;
    using System.Security.Permissions;
    using Common;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Model.Contract;
    using Model.ResourceModel;

    /// <summary>
    /// create a new azure container
    /// </summary>
    [Cmdlet(VerbsCommon.New, StorageNouns.Container),
        OutputType(typeof(AzureStorageContainer))]
    public class NewAzureStorageContainerCommand : StorageCloudBlobCmdletBase
    {
        [Alias("N", "Container")]
        [Parameter(Position = 0, Mandatory = true, HelpMessage = "Container name",
            ValueFromPipeline = true,
            ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty]
        public string Name { get; set; }

        [Alias("PublicAccess")]
        [Parameter(Position = 1, Mandatory = false,
            HelpMessage = "Permission string Off/Blob/Container")]
        [ValidateSet(StorageNouns.ContainerAclOff, StorageNouns.ContainerAclBlob, StorageNouns.ContainerAclContainer, IgnoreCase = true)]
        [ValidateNotNullOrEmpty]
        public string Permission
        {
            get { return accessLevel; }
            set { accessLevel = value; }
        }
        private string accessLevel = StorageNouns.ContainerAclOff;

        /// <summary>
        /// Initializes a new instance of the NewAzureStorageContainerCommand class.
        /// </summary>
        public NewAzureStorageContainerCommand()
            : this(null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the NewAzureStorageContainerCommand class.
        /// </summary>
        /// <param name="channel">IStorageBlobManagement channel</param>
        public NewAzureStorageContainerCommand(IStorageBlobManagement channel)
        {
            Channel = channel;
            EnableMultiThread = false;
        }

        /// <summary>
        /// create a new azure container
        /// </summary>
        /// <param name="name">container name</param>
        internal AzureStorageContainer CreateAzureContainer(string name, string accesslevel)
        {
            if (!NameUtil.IsValidContainerName(name))
            {
                throw new ArgumentException(String.Format(Resources.InvalidContainerName, name));
            }

            BlobRequestOptions requestOptions = null;
            AccessCondition accessCondition = null;
            CloudBlobContainer container = Channel.GetContainerReference(name);

            bool created = Channel.CreateContainerIfNotExists(container, requestOptions, OperationContext);

            if (!created)
            {
                throw new ResourceAlreadyExistException(String.Format(Resources.ContainerAlreadyExists, name));
            }

            BlobContainerPermissions permissions = new BlobContainerPermissions();
            accessLevel = CultureInfo.CurrentCulture.TextInfo.ToTitleCase(accessLevel);

            switch (CultureInfo.CurrentCulture.TextInfo.ToTitleCase(accessLevel))
            {
                case StorageNouns.ContainerAclOff:
                    permissions.PublicAccess = BlobContainerPublicAccessType.Off;
                    break;
                case StorageNouns.ContainerAclBlob:
                    permissions.PublicAccess = BlobContainerPublicAccessType.Blob;
                    break;
                case StorageNouns.ContainerAclContainer:
                    permissions.PublicAccess = BlobContainerPublicAccessType.Container;
                    break;
                default:
                    throw new ArgumentException(Resources.OnlyOnePermissionForContainer);
            }

            if (accessLevel == StorageNouns.ContainerAclContainer || accessLevel == StorageNouns.ContainerAclBlob)
            {
                Channel.SetContainerPermissions(container, permissions, accessCondition, requestOptions, OperationContext);
            }
            else
            {
                permissions = Channel.GetContainerPermissions(container, accessCondition, requestOptions, OperationContext);
            }

            AzureStorageContainer azureContainer = new AzureStorageContainer(container, permissions);

            return azureContainer;
        }

        /// <summary>
        /// execute command
        /// </summary>
        [PermissionSet(SecurityAction.Demand, Name = "FullTrust")]
        public override void ExecuteCmdlet()
        {
            AzureStorageContainer azureContainer = CreateAzureContainer(Name, accessLevel);
            WriteObjectWithStorageContext(azureContainer);
        }
    }
}