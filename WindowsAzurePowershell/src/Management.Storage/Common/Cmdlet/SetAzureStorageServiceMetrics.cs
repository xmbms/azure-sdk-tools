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
    using StorageClient = WindowsAzure.Storage.Shared.Protocol;

    /// <summary>
    /// Show azure storage service properties
    /// </summary>
    [Cmdlet(VerbsCommon.Set, StorageNouns.StorageServiceMetrics),
        OutputType(typeof(ServiceProperties))]
    public class SetAzureStorageServiceMetricsCommand : StorageCloudBlobCmdletBase
    {
        [Parameter(Mandatory = true, Position = 0, HelpMessage = "Azure storage type")]
        [ValidateSet(StorageNouns.BlobService, StorageNouns.TableService, StorageNouns.QueueService,
            IgnoreCase = true)]
        public string Type { get; set; }

        [Parameter(HelpMessage = "Metrics version")]
        public double? Version { get; set; }

        [Parameter(HelpMessage = "Metrics retention days. Zero means disable Metrics, otherwise enable.")]
        [ValidateRange(0, 365)]
        public int? RetentionDays { get; set; }

        [Parameter(HelpMessage = "Metrics level.(None/Service/ServiceAndApi)")]
        [ValidateSet(StorageNouns.OffMetrics, StorageNouns.MinimalMetrics, StorageNouns.VerboseMetrics,
            IgnoreCase = true)]
        public string MetricsLevel { get; set; }

        [Parameter(Mandatory = false, HelpMessage = "Display ServiceProperties")]
        public SwitchParameter PassThru { get; set; }

        /// <summary>
        /// Update the specified service properties according to the input
        /// </summary>
        /// <param name="serviceProperties">Service properties</param>
        internal void UpdateServiceProperties(ServiceProperties serviceProperties)
        {
            if (Version != null)
            {
                serviceProperties.Metrics.Version = Version.ToString();
            }

            if (RetentionDays != null)
            {
                if (RetentionDays == 0)
                {
                    //Disable metrics
                    serviceProperties.Metrics.RetentionDays = null;
                }
                else
                {
                    serviceProperties.Metrics.RetentionDays = RetentionDays;
                }
            }

            if (MetricsLevel != null)
            {
                MetricsLevel metricsLevel = GetMetricsLevel(MetricsLevel);
                serviceProperties.Metrics.MetricsLevel = metricsLevel;
                //Set default metrics version
                if (string.IsNullOrEmpty(serviceProperties.Metrics.Version))
                {
                    string defaultMetricsVersion = "1.0";
                    serviceProperties.Metrics.Version = defaultMetricsVersion;
                }
            }
        }

        /// <summary>
        /// Get metrics level
        /// </summary>
        /// <param name="MetricsLevel">The string type of Metrics level</param>
        /// <example>GetMetricsLevel("None"), GetMetricsLevel("Service")</example>
        /// <returns>MetricsLevel object</returns>
        internal MetricsLevel GetMetricsLevel(string MetricsLevel)
        {
            try
            {
                return (MetricsLevel)Enum.Parse(typeof(MetricsLevel), MetricsLevel, true);
            }
            catch 
            {
                throw new ArgumentException(String.Format(Resources.InvalidEnumName, MetricsLevel));
            }
        }

        /// <summary>
        /// Set storage service properties
        /// </summary>
        /// <param name="account">Cloud storage account</param>
        /// <param name="type">Service type</param>
        /// <param name="serviceProperties">Service properties</param>
        private void SetStorageServiceProperties(CloudStorageAccount account, string type,
            ServiceProperties serviceProperties)
        {
            switch (CultureInfo.CurrentCulture.TextInfo.ToTitleCase(type))
            {
                case StorageNouns.BlobService:
                    account.CreateCloudBlobClient().SetServiceProperties(serviceProperties);
                    break;
                case StorageNouns.QueueService:
                    account.CreateCloudQueueClient().SetServiceProperties(serviceProperties);
                    break;
                case StorageNouns.TableService:
                    account.CreateCloudTableClient().SetServiceProperties(serviceProperties);
                    break;
                default:
                    throw new ArgumentException(Resources.InvalidStorageServiceType);
            }
        }

        /// <summary>
        /// Execute command
        /// </summary>
        [PermissionSet(SecurityAction.Demand, Name = "FullTrust")]
        public override void ExecuteCmdlet()
        {
            CloudStorageAccount account = GetCloudStorageAccount();
            ServiceProperties serviceProperties = 
                GetAzureStorageServiceMetricsCommand.GetStorageServiceProperties(account, Type);
            UpdateServiceProperties(serviceProperties);
            SetStorageServiceProperties(account, Type, serviceProperties);

            if (PassThru)
            {
                WriteObject(serviceProperties.Metrics);
            }
        }
    }
}
