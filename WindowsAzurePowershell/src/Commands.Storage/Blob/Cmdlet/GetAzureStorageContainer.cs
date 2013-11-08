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
    /// List azure storage container
    /// </summary>
    [Cmdlet(VerbsCommon.Get, StorageNouns.Container, DefaultParameterSetName = NameParameterSet),
        OutputType(typeof(AzureStorageContainer))]
    public class GetAzureStorageContainerCommand : StorageCloudBlobCmdletBase
    {
        /// <summary>
        /// Default parameter set name
        /// </summary>
        private const string NameParameterSet = "ContainerName";

        /// <summary>
        /// Prefix parameter set name
        /// </summary>
        private const string PrefixParameterSet = "ContainerPrefix";

        [Alias("N", "Container")]
        [Parameter(Position = 0, HelpMessage = "Container Name",
            ValueFromPipelineByPropertyName = true,
            ValueFromPipeline = true,
            ParameterSetName = NameParameterSet)]
        public string Name { get; set; }

        [Parameter(HelpMessage = "Container Prefix",
            ParameterSetName = PrefixParameterSet, Mandatory = true)]
        [ValidateNotNullOrEmpty]
        public string Prefix { get; set; }

        /// <summary>
        /// Initializes a new instance of the GetAzureStorageContainerCommand class.
        /// </summary>
        public GetAzureStorageContainerCommand()
            : this(null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the GetAzureStorageContainerCommand class.
        /// </summary>
        /// <param name="channel">IStorageBlobManagement channel</param>
        public GetAzureStorageContainerCommand(IStorageBlobManagement channel)
        {
            Channel = channel;
        }

        /// <summary>
        /// List containers by container name pattern.
        /// </summary>
        /// <param name="name">Container name pattern</param>
        /// <returns>An enumerable collection of cloudblob container</returns>
        internal IEnumerable<CloudBlobContainer> ListContainersByName(string name)
        {
            ContainerListingDetails details = ContainerListingDetails.Metadata;
            string prefix = string.Empty;
            BlobRequestOptions requestOptions = null;
            AccessCondition accessCondition = null;

            if (String.IsNullOrEmpty(name) || WildcardPattern.ContainsWildcardCharacters(name))
            {
                IEnumerable<CloudBlobContainer> containers = Channel.ListContainers(prefix, details, requestOptions, OperationContext);
                WildcardOptions options = WildcardOptions.IgnoreCase | WildcardOptions.Compiled;
                WildcardPattern wildcard = null;
                
                if (!string.IsNullOrEmpty(name))
                {
                    wildcard = new WildcardPattern(name, options);
                }

                foreach (CloudBlobContainer container in containers)
                {
                    if (null == wildcard || wildcard.IsMatch(container.Name))
                    {
                        yield return container;
                    }
                }
            }
            else
            {
                if (!NameUtil.IsValidContainerName(name))
                {
                    throw new ArgumentException(String.Format(Resources.InvalidContainerName, name));
                }

                CloudBlobContainer container = Channel.GetContainerReference(name);

                if (Channel.DoesContainerExist(container, requestOptions, OperationContext))
                {
                    //fetch container attributes
                    Channel.FetchContainerAttributes(container, accessCondition, requestOptions, OperationContext);
                    yield return container;
                }
                else
                {
                    throw new ResourceNotFoundException(String.Format(Resources.ContainerNotFound, name));
                }
            }
        }

        /// <summary>
        /// List containers by container name prefix
        /// </summary>
        /// <param name="prefix">Container name prefix</param>
        /// <returns>An enumerable collection of cloudblobcontainer</returns>
        internal IEnumerable<CloudBlobContainer> ListContainersByPrefix(string prefix)
        {
            ContainerListingDetails details = ContainerListingDetails.Metadata;
            BlobRequestOptions requestOptions = null;

            if (!NameUtil.IsValidContainerPrefix(prefix))
            {
                throw new ArgumentException(String.Format(Resources.InvalidContainerName, prefix));
            }

            IEnumerable<CloudBlobContainer> containers = Channel.ListContainers(prefix, details, requestOptions, OperationContext);
            return containers;
        }

        /// <summary>
        /// Pack CloudBlobContainer and it's permission to AzureStorageContainer object
        /// </summary>
        /// <param name="containerList">An enumerable collection of CloudBlobContainer</param>
        /// <returns>An enumerable collection of AzureStorageContainer</returns>
        internal void PackCloudBlobContainerWithAcl(IEnumerable<CloudBlobContainer> containerList)
        {
            if (null == containerList)
            {
                return;
            }

            foreach (CloudBlobContainer container in containerList)
            {
                long taskId = GetAvailableTaskId();
                Task task = GetContainerPermission(container, taskId, Context);
                RunConcurrentTask(task, taskId);
            }
        }

        /// <summary>
        /// Async get container permission
        /// </summary>
        /// <param name="container">CloudBlobContainer object</param>
        /// <param name="taskId">Task id</param>
        /// <param name="context">Azure storage context</param>
        /// <returns></returns>
        internal async Task GetContainerPermission(CloudBlobContainer container, long taskId, AzureStorageContext context)
        {
            BlobRequestOptions requestOptions = null;
            AccessCondition accessCondition = null;
            BlobContainerPermissions permissions = await Channel.GetContainerPermissionsAsync(container, accessCondition,
                    requestOptions, OperationContext, CmdletCancellationToken).ConfigureAwait(false);
            AzureStorageContainer azureContainer = new AzureStorageContainer(container, permissions);
            azureContainer.Context = context;
            OutputStream.WriteObject(taskId, azureContainer);
        }

        /// <summary>
        /// Execute command
        /// </summary>
        [PermissionSet(SecurityAction.Demand, Name = "FullTrust")]
        public override void ExecuteCmdlet()
        {
            IEnumerable<CloudBlobContainer> containerList = null;

            if (PrefixParameterSet == ParameterSetName)
            {
                containerList = ListContainersByPrefix(Prefix);
            }
            else
            {
                containerList = ListContainersByName(Name);
            }

            PackCloudBlobContainerWithAcl(containerList);
        }
    }
}
