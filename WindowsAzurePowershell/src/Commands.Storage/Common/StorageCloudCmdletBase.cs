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

namespace Microsoft.WindowsAzure.Commands.Storage.Common
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Management.Automation;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Commands.Utilities.Common;
    using Microsoft.WindowsAzure.Storage;
    using Model.ResourceModel;
    using ServiceModel = System.ServiceModel;

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
            if (Context == null && HasCurrentSubscription && CurrentSubscription != null &&
                !String.IsNullOrEmpty(CurrentSubscription.CurrentStorageAccountName))
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
            string CurrentStorageAccountName = CurrentSubscription.CurrentStorageAccountName;

            if (string.IsNullOrEmpty(CurrentStorageAccountName))
            {
                throw new ArgumentException(Resources.DefaultStorageCredentialsNotFound);
            }
            else
            {
                WriteDebugLog(String.Format(Resources.UseCurrentStorageAccountFromSubscription, CurrentStorageAccountName, CurrentSubscription.SubscriptionName));

                try
                {
                    //The service channel initialized by subscription
                    return CurrentSubscription.GetCloudStorageAccount();
                }
                catch (ServiceModel.CommunicationException e)
                {
                    WriteVerboseWithTimestamp(Resources.CannotGetSotrageAccountFromSubscription);

                    if (e.IsNotFoundException())
                    {
                        //Repack the 404 error
                        string errorMessage = String.Format(Resources.CurrentStorageAccountNotFoundOnAzure, CurrentStorageAccountName, CurrentSubscription.SubscriptionName);
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

            if (enableMultiThread)
            {
                SetUpMultiThreadEnvironment();
            }

            base.BeginProcessing();
        }

        /// <summary>
        /// End processing
        /// </summary>
        protected override void EndProcessing()
        {
            if (enableMultiThread)
            {
                MultiThreadEndProcessing();
            }

            double timespan = CmdletOperationContext.GetRunningMilliseconds();
            string message = string.Format(Resources.EndProcessingLog,
                this.GetType().Name, CmdletOperationContext.StartedRemoteCallCounter, CmdletOperationContext.FinishedRemoteCallCounter, timespan, CmdletOperationContext.ClientRequestId);
            WriteDebugLog(message);
            base.EndProcessing();
        }

        /// <summary>
        /// End processing in multi thread environment
        /// </summary>
        internal void MultiThreadEndProcessing()
        {
            TaskCounter.Signal();

            do
            {
                //When task add to datamovement library, it will immediately start.
                //So, we'd better output status at first.
                GatherStreamToMainThread(true);
            }
            while (!TaskCounter.Wait(WaitTimeout, CmdletCancellationToken));

            GatherStreamToMainThread(true);

            WriteVerbose(String.Format(Resources.TransferSummary, TaskTotalCount, TaskFinishedCount, TaskFailedCount));
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

        #region multithread related source code
        /// <summary>
        /// Enable or disable multithread
        ///     If the storage cmdlet want to disable the multithread feature,
        ///     it can disable when construct and beginProcessing
        /// </summary>
        protected bool EnableMultiThread
        {
            get { return enableMultiThread; }
            set { enableMultiThread = value; }
        }
        private bool enableMultiThread = true;
        private SemaphoreSlim limitedConcurrency = null;

        /// <summary>
        /// Summary progress record on multithread task
        /// </summary>
        protected ProgressRecord summaryRecord;

        /// <summary>
        /// MultiThread WriteObject/WriteError in order
        /// </summary>
        internal OrderedStreamWriter OutputStream
        {
            get;
            private set;
        }

        /// <summary>
        /// MultiThread WriteVerbose
        /// </summary>
        internal UnorderedStreamWriter<string> VerboseStream
        {
            get;
            private set;
        }

        /// <summary>
        /// MultiThread WriteProgress
        /// </summary>
        internal UnorderedStreamWriter<ProgressRecord> ProgressStream
        {
            get;
            private set;
        }

        /// <summary>
        /// Active task counter
        /// </summary>
        protected CountdownEvent TaskCounter;

        //CountDownEvent wait time out and output time interval.
        protected const int WaitTimeout = 1000;//ms

        /// <summary>
        /// Task number counter
        ///     The following counter should be used with Interlocked
        /// </summary>
        protected long TaskTotalCount = 0;
        protected long TaskFailedCount = 0;
        protected long TaskFinishedCount = 0;

        /// <summary>
        /// Get available task id
        ///     thread unsafe since it should only run in main thread
        /// </summary>
        protected long GetAvailableTaskId()
        {
            return TaskTotalCount;
        }

        /// <summary>
        /// Get the concurrency value
        /// </summary>
        /// <returns>The max number of concurrent task/rest call</returns>
        protected int GetCmdletConcurrency()
        {
            //TODO should provide a global configuration.
            //Note:
            //1. The concurrent task count of the storage context will be used as the concurrency level
            //   if running "StorageCmdlet -Context $context"
            //2. The default concurrency will be used if running "$context | StorageCmdlet"
            //   since we don't know how to define the correct concurrency for "($context1, $context2) | StorageCmdlet" 
            //   if the concurrent task count were defined in both $context1 and $context2.
            int concurrency = 0;

            /// Hard code number for default task amount per core
            int asyncTasksPerCoreMultiplier = 8;

            if (Context != null && Context.ConcurrentTaskCount != null)
            {
                concurrency = Context.ConcurrentTaskCount.Value;
            }

            if (concurrency <= 0)
            {
                concurrency = Environment.ProcessorCount * asyncTasksPerCoreMultiplier;
            }

            return concurrency;
        }

        /// <summary>
        /// Configure Service Point
        /// </summary>
        private void ConfigureServicePointManager()
        {
            ServicePointManager.DefaultConnectionLimit = GetCmdletConcurrency();
            ServicePointManager.Expect100Continue = false;
            ServicePointManager.UseNagleAlgorithm = true;
        }

        /// <summary>
        /// Init the multithread run time resource
        /// </summary>
        internal void InitMutltiThreadResources()
        {
            int initCount = 1;
            TaskCounter = new CountdownEvent(initCount);

            int summaryRecordId = 0;
            string summary = String.Format(Resources.TransmitActiveSummary, TaskTotalCount,
                TaskFinishedCount, TaskFailedCount, TaskTotalCount);
            summaryRecord = new ProgressRecord(summaryRecordId, Resources.TransmitActivity, summary);
            limitedConcurrency = new SemaphoreSlim(GetCmdletConcurrency());
        }

        /// <summary>
        /// Init multi thread WriteObject/WriteError/WriteVerbose/WriteProgress
        /// </summary>
        private void InitMultiThreadOutputStream()
        {
            OutputStream = new OrderedStreamWriter(WriteObject, WriteExceptionError);
            VerboseStream = new UnorderedStreamWriter<string>(WriteVerbose);
            ProgressStream = new UnorderedStreamWriter<ProgressRecord>(WriteProgress);
        }

        /// <summary>
        /// Set up MultiThread environment
        /// </summary>
        internal void SetUpMultiThreadEnvironment()
        {
            ConfigureServicePointManager();
            InitMultiThreadOutputStream();
            InitMutltiThreadResources();
            TaskTotalCount = 0;
        }

        /// <summary>
        /// Write progress/error/verbose/output to main thread
        /// </summary>
        protected virtual void GatherStreamToMainThread(bool isEndProcessing = false)
        {
            WriteTransmitSummaryStatus(isEndProcessing);
            ProgressStream.Output();
            VerboseStream.Output();
            OutputStream.Output();
        }

        /// <summary>
        /// Write transmit summary status
        /// </summary>
        protected void WriteTransmitSummaryStatus(bool isEndProcessing = false)
        {
            int currentCount = TaskCounter.CurrentCount;

            //The init count for TaskCounter is 1 which could make TaskCounter.CurrentCount greater than TotalCount
            if (!isEndProcessing)
            {
                currentCount--;
            }

            string summary = String.Format(Resources.TransmitActiveSummary, TaskTotalCount, TaskFinishedCount, TaskFailedCount, currentCount);
            summaryRecord.StatusDescription = summary;
            WriteProgress(summaryRecord);
        }

        /// <summary>
        /// Run async task
        /// </summary>
        /// <param name="task">Task operation</param>
        /// <param name="taskId">task id</param>
        protected async void RunConcurrentTask(Task task, long taskId)
        {
            //Most Tasks do the web requests.
            //ServicePointManager.DefaultConnectionLimit will limit the max connection.
            //So there is no need to limit the amount of concurrent tasks.
            TaskTotalCount++;
            TaskCounter.AddCount();

            try
            {
                limitedConcurrency.Wait(CmdletCancellationToken);
                await task;
                Interlocked.Increment(ref TaskFinishedCount);
            }
            catch (Exception e)
            {
                Interlocked.Increment(ref TaskFailedCount);
                OutputStream.WriteError(taskId, e);
            }
            finally
            {
                limitedConcurrency.Release();
                TaskCounter.Signal();
            }
        }

        #endregion
    }
}
