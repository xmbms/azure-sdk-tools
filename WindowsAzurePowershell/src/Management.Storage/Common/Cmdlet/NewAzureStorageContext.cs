﻿// ----------------------------------------------------------------------------------
//
// Copyright 2012 Microsoft Corporation
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
    using Microsoft.Samples.WindowsAzure.ServiceManagement.ResourceModel;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Auth;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Management.Automation;
    using System.Text;
    
    /// <summary>
    /// new storage context
    /// </summary>
    [Cmdlet(VerbsCommon.New, StorageNouns.StorageContext,
        DefaultParameterSetName = AccountNameKeyParameterSet)]
    public class NewAzureStorageContext : StorageCmdletBase
    {
        /// <summary>
        /// default parameter set name
        /// </summary>
        private const string AccountNameKeyParameterSet = "AccountNameAndKey";

        /// <summary>
        /// sas token parameter set name
        /// </summary>
        private const string SasTokenParameterSet = "SasToken";

        /// <summary>
        /// connection string parameter set name
        /// </summary>
        private const string ConnectionStringParameterSet = "ConnectionString";

        /// <summary>
        /// local development account parameter set name
        /// </summary>
        private const string LocalParameterSet = "LocalDevelopment";

        /// <summary>
        /// anonymous storage account parameter set name
        /// </summary>
        private const string AnonymousParameterSet = "AnonymousAccount";

        [Parameter(Position = 0, HelpMessage = "Azure Storage Acccount Name",
            Mandatory = true, ParameterSetName = AccountNameKeyParameterSet)]
        [Parameter(Position = 0, HelpMessage = "Azure Storage Acccount Name",
            Mandatory = true, ParameterSetName = AnonymousParameterSet)]
        [Parameter(Position = 0, HelpMessage = "Azure Storage Acccount Name",
            Mandatory = true, ParameterSetName = SasTokenParameterSet)]
        [ValidateNotNullOrEmpty]
        public string StorageAccountName { get; set; }

        [Parameter(Position = 1, HelpMessage = "Azure Storage Account Key",
            Mandatory = true, ParameterSetName = AccountNameKeyParameterSet)]
        [ValidateNotNullOrEmpty]
        public string StorageAccountKey { get; set; }

        [Alias("sas")]
        [Parameter(HelpMessage = "Azure Storage SAS Token",
            Mandatory = true, ParameterSetName = SasTokenParameterSet)]
        [ValidateNotNullOrEmpty]
        public string SasToken { get; set; }

        [Alias("conn")]
        [Parameter(HelpMessage = "Azure Storage Connection String",
            Mandatory = true, ParameterSetName = ConnectionStringParameterSet)]
        [ValidateNotNullOrEmpty]
        public string ConnectionString { get; set; }

        [Parameter(HelpMessage = "Use local development storage account",
            Mandatory = true, ParameterSetName = LocalParameterSet)]
        public SwitchParameter Local
        {
            get { return isLocalDevAccount; }
            set { isLocalDevAccount = value; }
        }
        private bool isLocalDevAccount;

        [Alias("anon")]
        [Parameter(HelpMessage = "Use anonymous storage account",
            Mandatory = true, ParameterSetName = AnonymousParameterSet)]
        public SwitchParameter Anonymous
        {
            get { return isAnonymous; }
            set { isAnonymous = value; }
        }
        private bool isAnonymous;

        [Parameter(HelpMessage = "Protocol specification (HTTP or HTTPS), default is HTTPS",
            ParameterSetName = AccountNameKeyParameterSet)]
        [Parameter(HelpMessage = "Protocol specification (HTTP or HTTPS), default is HTTPS",
            ParameterSetName = SasTokenParameterSet)]
        [ValidateSet(StorageNouns.HTTP, StorageNouns.HTTPS)]
        public string Protocol
        {
            get { return protocolType; }
            set { protocolType = value; }
        }
        private string protocolType = StorageNouns.HTTPS;

        /// <summary>
        /// get storage account by account name and account key
        /// </summary>
        /// <param name="accountName">storage account name</param>
        /// <param name="accountKey">storage account key</param>
        /// <param name="useHttps">whether use https</param>
        /// <returns>a storage account</returns>
        internal CloudStorageAccount GetStorageAccountByNameAndKey(string accountName, string accountKey, bool useHttps)
        {
            StorageCredentials credential = new StorageCredentials(accountName, accountKey);
            return new CloudStorageAccount(credential, useHttps);
        }

        /// <summary>
        /// get storage account by sastoken
        /// </summary>
        /// <param name="storageAccountName">storage account name, it's used for build end point</param>
        /// <param name="sasToken">sas token</param>
        /// <param name="useHttps">whether use https</param>
        /// <returns>a storage account</returns>
        internal CloudStorageAccount GetStorageAccountBySasToken(string storageAccountName, string sasToken, bool useHttps)
        {
            StorageCredentials credential = new StorageCredentials(SasToken);
            return GetStorageAccountWithEndPoint(credential, storageAccountName);
        }

        /// <summary>
        /// get storage account by connection string
        /// </summary>
        /// <param name="connectionString">azure storage connection string</param>
        /// <returns>a storage account</returns>
        internal CloudStorageAccount GetStorageAccountByConnectionString(string connectionString)
        {
            return CloudStorageAccount.Parse(connectionString);
        }

        /// <summary>
        /// get local development storage account
        /// </summary>
        /// <returns>a storage account</returns>
        internal CloudStorageAccount GetLocalDevelopmentStorageAccount()
        {
            return CloudStorageAccount.DevelopmentStorageAccount;
        }

        /// <summary>
        /// get anonymous storage account
        /// </summary>
        /// <param name="storageAccountName">storage account name, it's used for build end point</param>
        /// <returns>a storage account</returns>
        internal CloudStorageAccount GetAnonymousStorageAccount(string storageAccountName)
        {
            StorageCredentials credential = new StorageCredentials();
            return GetStorageAccountWithEndPoint(credential, storageAccountName);
        }

        /// <summary>
        /// get storage account and use specific end point
        /// </summary>
        /// <param name="credential">storage credentail</param>
        /// <param name="storageAccountName">storage account name, it's used for build end point</param>
        /// <returns>a storage account</returns>
        internal CloudStorageAccount GetStorageAccountWithEndPoint(StorageCredentials credential, string storageAccountName)
        {
            string blobEndPoint = String.Format(Resources.DefaultBlobEndPointFormat, storageAccountName);
            string tableEndPoint = String.Format(Resources.DefaultTableEndPointFormat, storageAccountName);
            string queueEndPoint = String.Format(Resources.DefaultQueueEndPointFormat, storageAccountName);
            return new CloudStorageAccount(credential, new Uri(blobEndPoint), new Uri(tableEndPoint), new Uri(queueEndPoint));
        }

        /// <summary>
        /// execute command
        /// </summary>
        public override void ExecuteCmdlet()
        {
            CloudStorageAccount account = null;
            bool useHttps = !("http" == protocolType.ToLower());

            switch (ParameterSetName)
            {
                case AccountNameKeyParameterSet:
                    account = GetStorageAccountByNameAndKey(StorageAccountName, StorageAccountKey, useHttps);
                    break;
                case SasTokenParameterSet:
                    account = GetStorageAccountBySasToken(StorageAccountName, SasToken, useHttps);
                    break;
                case ConnectionStringParameterSet:
                    account = GetStorageAccountByConnectionString(ConnectionString);
                    break;
                case LocalParameterSet:
                    account = GetLocalDevelopmentStorageAccount();
                    break;
                case AnonymousParameterSet:
                    account = GetAnonymousStorageAccount(StorageAccountName);
                    break;
                default:
                    throw new ArgumentException(Resources.DefaultStorageCredentialsNotFound);
            }

            StorageContext context = new StorageContext(account);
            WriteObject(context);
        }
    }
}