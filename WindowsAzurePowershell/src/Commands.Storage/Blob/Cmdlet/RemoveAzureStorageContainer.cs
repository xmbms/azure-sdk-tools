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
    using Model.Contract;

    /// <summary>
    /// remove specified azure container
    /// </summary>
    [Cmdlet(VerbsCommon.Remove, StorageNouns.Container, SupportsShouldProcess = true, ConfirmImpact = ConfirmImpact.High),
        OutputType(typeof(Boolean))]
    public class RemoveAzureStorageContainerCommand : StorageCloudBlobCmdletBase
    {
        [Alias("N", "Container")]
        [Parameter(Position = 0, Mandatory = true,
            HelpMessage = "Container Name",
            ValueFromPipeline = true,
            ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty]
        public string Name { get; set; }

        [Parameter(HelpMessage = "Force to remove the container without confirm")]
        public SwitchParameter Force
        {
            get { return force; }
            set { force = value; }
        }
        private bool force;

        [Parameter(Mandatory = false, HelpMessage = "Return whether the specified container is successfully removed")]
        public SwitchParameter PassThru { get; set; }

        /// <summary>
        /// Initializes a new instance of the RemoveAzureStorageContainerCommand class.
        /// </summary>
        public RemoveAzureStorageContainerCommand()
            : this(null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the RemoveAzureStorageContainerCommand class.
        /// </summary>
        /// <param name="channel">IStorageBlobManagement channel</param>
        public RemoveAzureStorageContainerCommand(IStorageBlobManagement channel)
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
        /// remove azure container by container name
        /// </summary>
        /// <param name="name">container name</param>
        internal async Task RemoveAzureContainer(string name, long taskId)
        {
            if (!NameUtil.IsValidContainerName(name))
            {
                throw new ArgumentException(String.Format(Resources.InvalidContainerName, name));
            }

            BlobRequestOptions requestOptions = null;
            AccessCondition accessCondition = null;

            CloudBlobContainer container = Channel.GetContainerReference(name);

            string result = string.Empty;
            bool removed = false;

            bool exist = await Channel.DoesContainerExistAsync(container, requestOptions, OperationContext, CmdletCancellationToken);

            if (!exist)
            {
                throw new ResourceNotFoundException(String.Format(Resources.ContainerNotFound, name));
            }

            if (force || ConfirmRemove(name))
            {
                await Channel.DeleteContainerAsync(container, accessCondition, requestOptions,
                    OperationContext, CmdletCancellationToken);
                removed = true;
                result = String.Format(Resources.RemoveContainerSuccessfully, name);
            }
            else
            {
                result = String.Format(Resources.RemoveContainerCancelled, name);
            }

            VerboseStream.WriteStream(result);

            if (PassThru)
            {
                OutputStream.WriteObject(taskId, removed);
            }

        }

        /// <summary>
        /// execute command
        /// </summary>
        [PermissionSet(SecurityAction.Demand, Name = "FullTrust")]
        public override void ExecuteCmdlet()
        {
            long taskId = GetAvailableTaskId();
            Task task = RemoveAzureContainer(Name, taskId);
            RunConcurrentTask(task, taskId);
        }
    }
}
