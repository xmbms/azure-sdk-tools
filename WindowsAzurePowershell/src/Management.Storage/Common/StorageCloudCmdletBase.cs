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

namespace Microsoft.WindowsAzure.Management.Storage.Common
{
    using Microsoft.WindowsAzure.Management.Storage.Model.ResourceModel;
    using Microsoft.WindowsAzure.Storage;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Management.Automation;
    using ServiceModel = System.ServiceModel;
    using Microsoft.WindowsAzure.Management.Utilities.Common;
    using System.Threading;

    /// <summary>
    /// Base cmdlet for all storage cmdlet that works with cloud
    /// </summary>
    public class StorageCloudCmdletBase<T> : CloudBaseCmdlet<T>
        where T : class
    {
        [Parameter(HelpMessage = "Azure Storage Context Object",
            ValueFromPipelineByPropertyName = true)]
        public virtual AzureStorageContext Context {get; set;}

        /// <summary>
        /// Cancellation Token Source
        /// </summary>

        private CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

        protected CancellationToken CmdletCancellationToken;

        /// <summary>
        /// Cmdlet operation context.
        /// </summary>
        protected OperationContext OperationContext 
        {
            get
            {
                return CmdletOperationContext.GetStorageOperationContext(WriteDebugLog);
            }    
        }

        /// <summary>
        /// Write log in debug mode
        /// </summary>
        /// <param name="msg">Debug log</param>
        internal void WriteDebugLog(string msg)
        {
            WriteDebugWithTimestamp(msg);
        }

        /// <summary>
        /// Get cloud storage account 
        /// </summary>
        /// <returns>Storage account</returns>
        internal CloudStorageAccount GetCloudStorageAccount()
        {
            if (Context != null)
            {
                WriteDebugLog(String.Format(Resources.UseStorageAccountFromContext, Context.StorageAccountName));
                return Context.StorageAccount;
            }
            else
            {
                CloudStorageAccount account = null;
                bool shouldInitChannel = ShouldInitServiceChannel();

                try
                {
                    if (shouldInitChannel)
                    {
                        account = GetStorageAccountFromSubscription();
                    }
                    else
                    {
                        account = GetStorageAccountFromEnvironmentVariable();
                    }
                }
                catch (Exception e)
                {
                    //stop the pipeline if storage account is missed.
                    WriteTerminatingError(e);
                }

                //Set the storage context and use it in pipeline
                Context = new AzureStorageContext(account);

                return account;
            }
        }

        /// <summary>
        /// Output azure storage object with storage context
        /// </summary>
        /// <param name="item">An AzureStorageBase object</param>
        internal void WriteObjectWithStorageContext(AzureStorageBase item)
        {
            item.Context = Context;

            WriteObject(item);
        }

        /// <summary>
        /// Init channel with or without subscription in storage cmdlet
        /// </summary>
        /// <param name="force">Force to create a new channel</param>
        protected override void InitChannelCurrentSubscription(bool force)
        {
            //Create storage management channel
            CreateChannel();
        }

        /// <summary>
        /// Whether should init the service channel or not
        /// </summary>
        /// <returns>True if it need to init the service channel, otherwise false</returns>
        internal virtual bool ShouldInitServiceChannel()
        {
            //Storage Context is empty and have already set the current storage account in subscription
            if (Context == null && CurrentSubscription != null &&
                !String.IsNullOrEmpty(CurrentSubscription.CurrentStorageAccount))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// Output azure storage object with storage context
        /// </summary>
        /// <param name="item">An enumerable collection fo azurestorage object</param>
        internal void WriteObjectWithStorageContext(IEnumerable<AzureStorageBase> itemList)
        {
            if (null == itemList)
            {
                return;
            }

            foreach (AzureStorageBase item in itemList)
            {
                WriteObjectWithStorageContext(item);
            }
        }

        /// <summary>
        /// Get current storage account from azure subscription
        /// </summary>
        /// <returns>A storage account</returns>
        private CloudStorageAccount GetStorageAccountFromSubscription()
        {
            string CurrentStorageAccount = CurrentSubscription.CurrentStorageAccount;

            if (string.IsNullOrEmpty(CurrentStorageAccount))
            {
                throw new ArgumentException(Resources.DefaultStorageCredentialsNotFound);
            }
            else
            {
                WriteDebugLog(String.Format(Resources.UseCurrentStorageAccountFromSubscription, CurrentStorageAccount, CurrentSubscription.SubscriptionName));

                try
                {
                    //The service channel initialized by subscription
                    return CurrentSubscription.GetCurrentStorageAccount();
                }
                catch (ServiceModel.CommunicationException e)
                {
                    WriteVerboseWithTimestamp(Resources.CannotGetSotrageAccountFromSubscription);

                    if (e.IsNotFoundException())
                    {
                        //Repack the 404 error
                        string errorMessage = String.Format(Resources.CurrentStorageAccountNotFoundOnAzure, CurrentStorageAccount, CurrentSubscription.SubscriptionName);
                        ServiceModel.CommunicationException exception = new ServiceModel.CommunicationException(errorMessage, e);
                        throw exception;
                    }
                    else
                    {
                        throw;
                    }
                }
            }
        }

        /// <summary>
        /// Get storage account from environment variable "AZURE_STORAGE_CONNECTION_STRING"
        /// </summary>
        /// <returns>Cloud storage account</returns>
        private CloudStorageAccount GetStorageAccountFromEnvironmentVariable()
        {
            String connectionString = System.Environment.GetEnvironmentVariable(Resources.EnvConnectionString);

            if (String.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentException(Resources.DefaultStorageCredentialsNotFound);
            }
            else
            {
                WriteDebugLog(Resources.GetStorageAccountFromEnvironmentVariable);

                try
                {
                    return CloudStorageAccount.Parse(connectionString);
                }
                catch
                {
                    WriteVerboseWithTimestamp(Resources.CannotGetStorageAccountFromEnvironmentVariable);
                    throw;
                }
            }
        }

        /// <summary>
        /// Write error with category and identifier
        /// </summary>
        /// <param name="e">an exception object</param>
        protected override void WriteExceptionError(Exception e)
        {
            Debug.Assert(e != null, Resources.ExceptionCannotEmpty);
            
            if (e is StorageException)
            {
                e = ((StorageException) e).RepackStorageException();
            }
            
            WriteError(new ErrorRecord(e, e.GetType().Name, GetExceptionErrorCategory(e), null));
        }

        /// <summary>
        /// Get the error category for specificed exception
        /// </summary>
        /// <param name="e">Exception object</param>
        /// <returns>Error category</returns>
        protected ErrorCategory GetExceptionErrorCategory(Exception e)
        {
            ErrorCategory errorCategory = ErrorCategory.CloseError; //default error category

            if (e is ArgumentException)
            {
                errorCategory = ErrorCategory.InvalidArgument;
            }
            else if (e is ResourceNotFoundException)
            {
                errorCategory = ErrorCategory.ObjectNotFound;
            }
            else if (e is ResourceAlreadyExistException)
            {
                errorCategory = ErrorCategory.ResourceExists;
            }

            return errorCategory;
        }

        /// <summary>
        /// write terminating error
        /// </summary>
        /// <param name="e">exception object</param>
        protected void WriteTerminatingError(Exception e)
        {
            Debug.Assert(e != null, Resources.ExceptionCannotEmpty);
            ThrowTerminatingError(new ErrorRecord(e, e.GetType().Name, GetExceptionErrorCategory(e), null));
        }

        /// <summary>
        /// Cmdlet begin process
        /// </summary>
        protected override void BeginProcessing()
        {
            CmdletOperationContext.Init();
            CmdletCancellationToken = cancellationTokenSource.Token;
            WriteDebugLog(String.Format(Resources.InitOperationContextLog, this.GetType().Name, CmdletOperationContext.ClientRequestId));
            base.BeginProcessing();
        }

        /// <summary>
        /// End processing
        /// </summary>
        protected override void EndProcessing()
        {
            double timespan = CmdletOperationContext.GetRunningMilliseconds();
            string message = string.Format(Resources.EndProcessingLog,
                this.GetType().Name, CmdletOperationContext.StartedRemoteCallCounter, CmdletOperationContext.FinishedRemoteCallCounter, timespan, CmdletOperationContext.ClientRequestId);
            WriteDebugLog(message);
            base.EndProcessing();
        }

        /// <summary>
        /// stop processing
        /// time-consuming operation should work with ShouldForceQuit
        /// </summary>
        protected override void StopProcessing()
        {
            //ctrl + c and etc
            cancellationTokenSource.Cancel();
            base.StopProcessing();
        }

        /// <summary>
        /// Is the cmdlet operation canceled
        /// </summary>
        /// <returns>True if cancel, otherwise false</returns>
        protected bool IsCanceledOperation()
        {
            if (CmdletCancellationToken != null && CmdletCancellationToken.IsCancellationRequested)
            {
                return true;
            }

            return false;
        }
    }
}
