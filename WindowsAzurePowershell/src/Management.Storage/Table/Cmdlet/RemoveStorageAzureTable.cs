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

namespace Microsoft.WindowsAzure.Management.Storage.Table.Cmdlet
{
    using Microsoft.WindowsAzure.Management.Storage.Common;
    using Microsoft.WindowsAzure.Management.Storage.Model.Contract;
    using Microsoft.WindowsAzure.Storage.Table;
    using System;
    using System.Management.Automation;
    using System.Security.Permissions;

    /// <summary>
    /// remove an azure table
    /// </summary>
    [Cmdlet(VerbsCommon.Remove, StorageNouns.Table, SupportsShouldProcess = true, ConfirmImpact = ConfirmImpact.High),
        OutputType(typeof(string))]
    public class RemoveAzureStorageTableCommand : StorageCloudTableCmdletBase
    {
        [Alias("N", "Table")]
        [Parameter(Position = 0, Mandatory = true, HelpMessage = "Table name",
            ValueFromPipeline = true,
            ValueFromPipelineByPropertyName = true)]
        public string Name { get; set; }

        [Parameter(HelpMessage = "Force to remove the table without confirm")]
        public SwitchParameter Force
        {
            get { return force; }
            set { force = value; }
        }
        private bool force;

        /// <summary>
        /// Initializes a new instance of the RemoveAzureStorageTableCommand class.
        /// </summary>
        public RemoveAzureStorageTableCommand()
            : this(null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the RemoveAzureStorageTableCommand class.
        /// </summary>
        /// <param name="channel">IStorageTableManagement channel</param>
        public RemoveAzureStorageTableCommand(IStorageTableManagement channel)
        {
            Channel = channel;
        }

        /// <summary>
        /// confirm the remove operation
        /// </summary>
        /// <param name="message">confirmation message</param>
        /// <returns>true if user confirm the remove operation, otherwise false</returns>
        internal virtual bool ConfirmRemove(string message)
        {
            return ShouldProcess(message);
        }

        /// <summary>
        /// remove azure table
        /// </summary>
        /// <param name="name">table name</param>
        /// <returns>
        /// true if the table is removed, false if user cancel the operation,
        /// otherwise throw an exception</returns>
        internal bool RemoveAzureTable(string name)
        {
            if (!NameUtil.IsValidTableName(name))
            {
                throw new ArgumentException(String.Format(Resources.InvalidTableName, name));
            }

            TableRequestOptions requestOptions = null;
            CloudTable table = Channel.GetTableReference(name);

            if (!Channel.IsTableExists(table, requestOptions, OperationContext))
            {
                throw new ResourceNotFoundException(String.Format(Resources.TableNotFound, name));
            }

            if (force || ConfirmRemove(name))
            {
                Channel.Delete(table, requestOptions, OperationContext);
                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// execute command
        /// </summary>
        [PermissionSet(SecurityAction.Demand, Name = "FullTrust")]
        public override void ExecuteCmdlet()
        {
            string result = string.Empty;
            bool removed = RemoveAzureTable(Name);

            if (removed)
            {
                result = String.Format(Resources.RemoveTableSuccessfully, Name);
            }
            else
            {
                result = String.Format(Resources.RemoveTableCancelled, Name);
            }

            WriteObject(result);
        }
    }
}