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

namespace Microsoft.WindowsAzure.Management.Storage.Common.Cmdlet
{
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Shared.Protocol;
    using System;
    using System.Globalization;
    using System.Management.Automation;
    using System.Security.Permissions;

    /// <summary>
    /// Show azure storage service properties
    /// </summary>
    [Cmdlet(VerbsCommon.Get, StorageNouns.StorageServiceMetrics),
        OutputType(typeof(ServiceProperties))]
    public class GetAzureStorageServiceMetricsCommand : StorageCloudBlobCmdletBase
    {
        [Parameter(Mandatory = true, Position = 0, HelpMessage = "Azure storage type")]
        [ValidateSet(StorageNouns.BlobService, StorageNouns.TableService, StorageNouns.QueueService, IgnoreCase = true)]
        public string Type { get; set; }

        /// <summary>
        /// Get storage service properties
        /// </summary>
        /// <param name="account">Cloud storage account</param>
        /// <param name="type">Service type</param>
        /// <returns>Storage service properties</returns>
        internal static ServiceProperties GetStorageServiceProperties(CloudStorageAccount account, string type)
        {
            switch (CultureInfo.CurrentCulture.TextInfo.ToTitleCase(type))
            {
                case StorageNouns.BlobService:
                    return account.CreateCloudBlobClient().GetServiceProperties();
                case StorageNouns.QueueService:
                    return account.CreateCloudQueueClient().GetServiceProperties();
                case StorageNouns.TableService:
                    return account.CreateCloudTableClient().GetServiceProperties();
                default:
                    throw new ArgumentException(Resources.InvalidStorageServiceType, "type");
            }
        }

        /// <summary>
        /// Execute command
        /// </summary>
        [PermissionSet(SecurityAction.Demand, Name = "FullTrust")]
        public override void ExecuteCmdlet()
        {
            CloudStorageAccount account = GetCloudStorageAccount();
            ServiceProperties serviceProperties = GetStorageServiceProperties(account, Type);
            WriteObject(serviceProperties.Metrics);
        }
    }
}
