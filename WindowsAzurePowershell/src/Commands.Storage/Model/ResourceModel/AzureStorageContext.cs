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

namespace Microsoft.WindowsAzure.Commands.Storage.Model.ResourceModel
{
    using System;
    using Microsoft.WindowsAzure.Storage;

    /// <summary>
    /// Storage context
    /// </summary>
    public class AzureStorageContext
    {
        /// <summary>
        /// Storage account name used in this context
        /// </summary>
        public string StorageAccountName { get; private set; }

        /// <summary>
        /// Blob end point of the storage context
        /// </summary>
        public string BlobEndPoint { get; private set; }

        /// <summary>
        /// Table end point of the storage context
        /// </summary>
        public string TableEndPoint { get; private set; }

        /// <summary>
        /// Queue end point of the storage context
        /// </summary>
        public string QueueEndPoint { get; private set; }

        /// <summary>
        /// Self reference, it could enable New-AzureStorageContext can be used in pipeline 
        /// </summary>
        public AzureStorageContext Context { get; private set; }

        /// <summary>
        /// Name place holder, and force pipeline to ignore this property
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Storage account in context
        /// </summary>
        public CloudStorageAccount StorageAccount { get; private set; }

        /// <summary>
        /// Create a storage context usign cloud storage account
        /// </summary>
        /// <param name="account">cloud storage account</param>
        public AzureStorageContext(CloudStorageAccount account)
        {
            StorageAccount = account;

            if (account.BlobEndpoint != null)
            {
                BlobEndPoint = account.BlobEndpoint.ToString();
            }

            if (account.TableEndpoint != null)
            {
                TableEndPoint = account.TableEndpoint.ToString();
            }

            if (account.QueueEndpoint != null)
            {
                QueueEndPoint = account.QueueEndpoint.ToString();
            }

            StorageAccountName = account.Credentials.AccountName;
            Context = this;
            Name = String.Empty;

            if (string.IsNullOrEmpty(StorageAccountName))
            {
                if (account.Credentials.IsSAS)
                {
                    StorageAccountName = Resources.SasTokenAccountName;
                }
                else
                {
                    StorageAccountName = Resources.AnonymousAccountName;
                }
            }
        }
    }
}
