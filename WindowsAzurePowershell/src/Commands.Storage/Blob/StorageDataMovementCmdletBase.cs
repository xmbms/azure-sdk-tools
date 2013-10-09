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

namespace Microsoft.WindowsAzure.Commands.Storage.Blob
{
    using System;
    using System.Management.Automation;
    using System.Net;
    using System.Threading;
    using Microsoft.WindowsAzure.Storage.DataMovement;

    public class StorageDataMovementCmdletBase : StorageCloudBlobCmdletBase, IDisposable
    {
        /// <summary>
        /// Amount of concurrent async tasks to run per available core.
        /// </summary>
        protected int concurrentTaskCount = 0;

        /// <summary>
        /// whether the transfer progress finished
        /// </summary>
        private bool finished;

        /// <summary>
        /// exception thrown during transfer
        /// </summary>
        private Exception runtimeException;

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
        /// Copy task count
        /// </summary>
        private int TotalCount = 0;
        private int FailedCount = 0;
        private int FinishedCount = 0;

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
        internal virtual void OnTaskStart(object progress)
        {
            ProgressRecord pr = progress as ProgressRecord;

            if (null != pr)
            {
                pr.PercentComplete = 0;
            }
        }

        /// <summary>
        /// on downloading 
        /// </summary>
        /// <param name="progress">progress information</param>
        /// <param name="speed">download speed</param>
        /// <param name="percent">download percent</param>
        internal virtual void OnTaskProgress(object progress, double speed, double percent)
        {
            ProgressRecord pr = progress as ProgressRecord;

            if (null == pr)
            {
                return;
            }

            pr.PercentComplete = (int)percent;
            pr.StatusDescription = String.Format(Resources.FileTransmitStatus, pr.PercentComplete, BytesToHumanReadableSize(speed));
        }

        /// <summary>
        /// on downloading finish
        /// </summary>
        /// <param name="progress">progress information</param>
        /// <param name="e">run time exception</param>
        internal virtual void OnTaskFinish(object progress, Exception e)
        {
            finished = true;
            runtimeException = e;

            ProgressRecord pr = progress as ProgressRecord;

            if (null == pr)
            {
                return;
            }

            pr.PercentComplete = 100;

            if (null == e)
            {
                pr.StatusDescription = Resources.TransmitSuccessfully;
            }
            else
            {
                pr.StatusDescription = String.Format(Resources.TransmitFailed, e.Message);
            }
        }

        /// <summary>
        /// Cmdlet begin processing
        /// </summary>
        protected override void BeginProcessing()
        {
            if (concurrentTaskCount == 0)
            {
                concurrentTaskCount = Environment.ProcessorCount * asyncTasksPerCoreMultiplier;
            }
            
            ConfigureServicePointManager();

            BlobTransferOptions opts = new BlobTransferOptions();
            opts.Concurrency = concurrentTaskCount;
            transferManager = new BlobTransferManager(opts);
            
            base.BeginProcessing();
        }

        /// <summary>
        /// Cmdlet end processing
        /// </summary>
        protected override void EndProcessing()
        {
            WriteVerbose(String.Format(Resources.TransferSummary, TotalCount, FinishedCount, FailedCount));

            base.EndProcessing();
        }

        /// <summary>
        /// Start sync task using transfermanager from datamovement library.
        /// </summary>
        /// <param name="taskAction"></param>
        /// <param name="record"></param>
        protected void StartSyncTaskInTransferManager(Action<BlobTransferManager> taskAction, ProgressRecord record = null)
        {
            finished = false;
            TotalCount++;

            //status update interval
            int interval = 1 * 1000; //in millisecond

            taskAction(transferManager);

            while (!finished)
            {
                if (record != null)
                {
                    WriteProgress(record);
                }

                Thread.Sleep(interval);
                if (ShouldForceQuit)
                {
                    //can't output verbose log for this operation since the Output stream is already stopped.
                    transferManager.CancelWork();
                    transferManager.WaitForCompletion();
                    break;
                }
            }

            if (runtimeException != null)
            {
                FailedCount++;
                RuntimeException rtException = new RuntimeException(runtimeException.Message, runtimeException);
                throw rtException;
            }
            else
            {
                FinishedCount++;
            }
        }

        /// <summary>
        /// Dispose DataMovement cmdlet
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }

        /// <summary>
        /// Dispose DataMovement cmdlet
        /// </summary>
        /// <param name="disposing">User disposing</param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
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
}
