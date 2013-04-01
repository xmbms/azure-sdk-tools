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

namespace Microsoft.WindowsAzure.Management.Storage.Blob
{
    using Microsoft.WindowsAzure.Management.Storage.Common;
    using Microsoft.WindowsAzure.Management.Storage.Model.ResourceModel;
    using Microsoft.WindowsAzure.Storage.DataMovement;
    using System;
    using System.Management.Automation;
    using System.Net;
    using System.Security.Permissions;
    using System.Threading;

    public class StorageDataMovementCmdletBase : StorageCloudBlobCmdletBase, IDisposable
    {
        /// <summary>
        /// Amount of concurrent async tasks to run per available core.
        /// </summary>
        protected int concurrentTaskCount = 0;

        /// <summary>
        /// Blob Transfer Manager
        /// </summary>
        private BlobTransferManager transferManager;

        /// <summary>
        /// Default task per core
        /// </summary>
        private const int asyncTasksPerCoreMultiplier = 8;

        [Parameter(HelpMessage = "Force to overwrite the existing blob or file")]
        public SwitchParameter Force
        {
            get { return overwrite; }
            set { overwrite = value; }
        }
        protected bool overwrite;

        /// <summary>
        /// MultiThread WriteObject
        /// </summary>
        internal MultiThreadStreamWriter<object> OutputStream
        {
            get;
            private set;
        }

        /// <summary>
        /// MultiThread WriteException
        /// </summary>
        internal MultiThreadStreamWriter<Exception> ErrorStream
        {
            get;
            private set;
        }

        /// <summary>
        /// MultiThread WriteVerbose
        /// </summary>
        internal MultiThreadStreamWriter<string> VerboseStream
        {
            get;
            private set;
        }

        /// <summary>
        /// MultiThread WriteProgress
        /// </summary>
        internal MultiThreadStreamWriter<ProgressRecord> ProgressStream
        {
            get;
            private set;
        }

        /// <summary>
        /// Active task counter
        /// </summary>
        private CountdownEvent TaskCounter;

        /// <summary>
        /// Copy task count
        /// </summary>
        private int TotalCount = 0;
        private int FailedCount = 0;
        private int FinishedCount = 0;

        private ProgressRecord summaryRecord;

        /// <summary>
        /// Available task id
        /// </summary>
        protected int AvailableTaskId
        {
            get { return TotalCount; }
        }

        /// <summary>
        /// Start index for detail process record id. O is reseverd for summary record.
        /// </summary>
        protected const int DetailRecordStartIndex = 1;

        /// <summary>
        /// The default count for detail progress record. Notice: we actually use DefaultDetailRecordCount progress records + 1 summary progress record
        /// </summary>
        protected const int DefaultDetailRecordCount = 4;

        private bool isEndProcessing;

        /// <summary>
        /// Size formats
        /// </summary>
        private string[] sizeFormats =
        {
            Resources.HumanReadableSizeFormat_Bytes,
            Resources.HumanReadableSizeFormat_KiloBytes,
            Resources.HumanReadableSizeFormat_MegaBytes,
            Resources.HumanReadableSizeFormat_GigaBytes,
            Resources.HumanReadableSizeFormat_TeraBytes,
            Resources.HumanReadableSizeFormat_PetaBytes,
            Resources.HumanReadableSizeFormat_ExaBytes
        };

        /// <summary>
        /// Translate a size in bytes to human readable form.
        /// </summary>
        /// <param name="size">Size in bytes.</param>
        /// <returns>Human readable form string.</returns>
        internal string BytesToHumanReadableSize(double size)
        {
            int order = 0;

            while (size >= 1024 && order + 1 < sizeFormats.Length)
            {
                ++order;
                size /= 1024;
            }

            return string.Format(sizeFormats[order], size);
        }

        /// <summary>
        /// Confirm the overwrite operation
        /// </summary>
        /// <param name="msg">Confirmation message</param>
        /// <returns>True if the opeation is confirmed, otherwise return false</returns>
        internal virtual bool ConfirmOverwrite(string destinationPath)
        {
            string overwriteMessage = String.Format(Resources.OverwriteConfirmation, destinationPath);
            return overwrite || ShouldProcess(destinationPath);
        }

        /// <summary>
        /// Configure Service Point
        /// </summary>
        private void ConfigureServicePointManager()
        {
            ServicePointManager.DefaultConnectionLimit = concurrentTaskCount;
            ServicePointManager.Expect100Continue = false;
            ServicePointManager.UseNagleAlgorithm = true;
        }

        /// <summary>
        /// on download start
        /// </summary>
        /// <param name="progress">progress information</param>
        internal virtual void OnTaskStart(object data)
        {
            DataMovementUserData userData = data as DataMovementUserData;

            if (null == userData)
            {
                return;
            }

            ProgressRecord pr = userData.Record;

            if (null != pr)
            {
                pr.PercentComplete = 0;
                ProgressStream.WriteStream(pr);
            }
        }

        /// <summary>
        /// on downloading 
        /// </summary>
        /// <param name="progress">progress information</param>
        /// <param name="speed">download speed</param>
        /// <param name="percent">download percent</param>
        internal virtual void OnTaskProgress(object data, double speed, double percent)
        {
            DataMovementUserData userData = data as DataMovementUserData;

            if (null == userData)
            {
                return;
            }

            ProgressRecord pr = userData.Record;

            pr.PercentComplete = (int)percent;
            pr.StatusDescription = String.Format(Resources.FileTransmitStatus, pr.PercentComplete, BytesToHumanReadableSize(speed));
            ProgressStream.WriteStream(pr);
        }

        /// <summary>
        /// on downloading finish
        /// </summary>
        /// <param name="progress">progress information</param>
        /// <param name="e">run time exception</param>
        internal virtual void OnTaskFinish(object data, Exception e)
        {
            try
            {
                DataMovementUserData userData = data as DataMovementUserData;

                string status = string.Empty;

                if (null == e)
                {
                    Interlocked.Increment(ref FinishedCount);
                    status = Resources.TransmitSuccessfully;
                    OnTaskSuccessful(userData);
                }
                else
                {
                    Interlocked.Increment(ref FailedCount);
                    status = String.Format(Resources.TransmitFailed, e.Message);
                    ErrorStream.WriteStream(e);
                }

                if (userData != null && userData.Record != null)
                {
                    userData.Record.PercentComplete = 100;
                    userData.Record.StatusDescription = status;
                    ProgressStream.WriteStream(userData.Record);
                }                
            }
            finally
            {
                TaskCounter.Signal();
            }
        }

        /// <summary>
        /// On Task run successfully
        /// </summary>
        /// <param name="data">User data</param>
        protected virtual void OnTaskSuccessful(DataMovementUserData data)
        {}

        /// <summary>
        /// Set up the cmdlet runtime for multi thread
        /// </summary>
        internal void SetupMultiThreadOutputStream()
        {
            OutputStream = new MultiThreadStreamWriter<object>((item) =>
                {
                    AzureStorageBase storageItem = item as AzureStorageBase;

                    if (storageItem != null)
                    {
                        WriteObjectWithStorageContext(storageItem);
                    }
                    else
                    {
                        WriteObject(item);
                    }
                });
            ErrorStream = new MultiThreadStreamWriter<Exception>(WriteExceptionError);
            VerboseStream = new MultiThreadStreamWriter<string>(WriteVerboseWithTimestamp);
            ProgressStream = new MultiThreadStreamWriter<ProgressRecord>(WriteProgress);
        }

        /// <summary>
        /// Begin processing
        /// </summary>
        protected override void BeginProcessing()
        {
            if (concurrentTaskCount == 0)
            {
                concurrentTaskCount = Environment.ProcessorCount * asyncTasksPerCoreMultiplier;
            }
            
            ConfigureServicePointManager();
            int initCount = 1;
            TaskCounter = new CountdownEvent(initCount);
            SetupMultiThreadOutputStream();

            BlobTransferOptions opts = new BlobTransferOptions();
            opts.Concurrency = concurrentTaskCount;
            opts.AppendToClientRequestId(CmdletOperationContext.ClientRequestId);
            transferManager = new BlobTransferManager(opts);

            int summaryRecordId = 0;
            int initActiveTaskCount = 0;
            string summary = String.Format(Resources.TransmitActiveSummary, TotalCount, FinishedCount, FailedCount, initActiveTaskCount);
            summaryRecord = new ProgressRecord(summaryRecordId, Resources.TransmitActivity, summary);
            isEndProcessing = false;
            base.BeginProcessing();
        }

        /// <summary>
        /// End processing
        /// </summary>
        protected override void EndProcessing()
        {
            TaskCounter.Signal();
            isEndProcessing = true;
            int waitTimeout = 1000;

            do
            {
                //When task add to datamovement library, it will immediately start.
                //So, we'd better output status at first.
                GatherStreamToMainThread();
                WriteTransmitSummaryStatus();
                Console.Write(".");
            }
            while (!TaskCounter.Wait(waitTimeout) && !ShouldForceQuit);

            if (ShouldForceQuit)
            {
                transferManager.CancelWorkAndWaitForCompletion();
            }

            GatherStreamToMainThread();

            WriteVerbose(String.Format(Resources.TransferSummary, TotalCount, FinishedCount, FailedCount));

            base.EndProcessing();
        }

        /// <summary>
        /// Write progress/error/verbose/output to main thread
        /// </summary>
        protected void GatherStreamToMainThread()
        {
            ProgressStream.WriteStreamToMainThread();
            ErrorStream.WriteStreamToMainThread();
            VerboseStream.WriteStreamToMainThread();
            OutputStream.WriteStreamToMainThread();
        }

        /// <summary>
        /// Start async task in transfer manager
        /// </summary>
        /// <param name="taskStartAction">Task  start action</param>
        /// <param name="record"></param>
        protected void StartAsyncTaskInTransferManager(Action<BlobTransferManager> taskStartAction)
        {
            TotalCount++;

            TaskCounter.AddCount();

            WriteTransmitSummaryStatus();

            try
            {
                taskStartAction(transferManager);
            }
            catch
            {
                TaskCounter.Signal();
                throw;
            }
        }

        /// <summary>
        /// Write transmit summary status 
        /// </summary>
        protected void WriteTransmitSummaryStatus()
        {
            int currentCount = TaskCounter.CurrentCount;

            //The init count for TaskCounter is 1 which could make TaskCounter.CurrentCount greater than TotalCount
            if (!isEndProcessing)
            {
                currentCount--;
            }

            string summary = String.Format(Resources.TransmitActiveSummary, TotalCount, FinishedCount, FailedCount, currentCount);
            summaryRecord.StatusDescription = summary;
            WriteProgress(summaryRecord);
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            if (transferManager != null)
            {
                transferManager.WaitForCompletion();
                transferManager.Dispose();
                transferManager = null;
            }
        }
    }
}
