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
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Commands.Storage.Common;
    using Microsoft.WindowsAzure.Commands.Storage.Utilities;
    using Microsoft.WindowsAzure.Management.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.DataMovement;

    public class StorageDataMovementCmdletBase : StorageCloudBlobCmdletBase, IDisposable
    {
        /// <summary>
        /// Amount of concurrent async tasks to run per available core.
        /// </summary>
        protected int concurrentTaskCount = 0;

        /// <summary>
        /// Blob Transfer Manager
        /// </summary>
        protected BlobTransferManager transferManager;

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
        internal virtual bool ConfirmOverwrite(string sourcePath, string destinationPath)
        {
            string overwriteMessage = String.Format(Resources.OverwriteConfirmation, destinationPath);
            return overwrite || ConfirmAsyc(overwriteMessage).Result;
        }

        /// <summary>
        /// on download start
        /// </summary>
        /// <param name="progress">progress information</param>
        internal virtual void OnTaskStart(object data)
        {
            Console.WriteLine("Start");
            if (IsCanceledOperation())
            {
                return;
            }

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
            if (IsCanceledOperation())
            {
                return;
            }

            DataMovementUserData userData = data as DataMovementUserData;

            if (null == userData)
            {
                return;
            }

            ProgressRecord pr = userData.Record;

            pr.PercentComplete = (int)percent;
            pr.StatusDescription = String.Format(Resources.FileTransmitStatus, pr.PercentComplete, Util.BytesToHumanReadableSize(speed));
            ProgressStream.WriteStream(pr);
        }

        /// <summary>
        /// on downloading finish
        /// </summary>
        /// <param name="progress">progress information</param>
        /// <param name="e">run time exception</param>
        internal virtual void OnTaskFinish(object data, Exception e)
        {
            Console.WriteLine("Task finished");
            //try
            //{
                if (IsCanceledOperation())
                {
                    return;
                }

                DataMovementUserData userData = data as DataMovementUserData;

                string status = string.Empty;

                if (null == e)
                {
                    //FIXME remove
                    //Interlocked.Increment(ref TaskFinishedCount);
                    status = Resources.TransmitSuccessfully;
                    OnTaskSuccessful(userData);
                    userData.taskSource.SetResult(true);
                }
                else
                {
                    //Interlocked.Increment(ref TaskFailedCount);
                    status = String.Format(Resources.TransmitFailed, e.Message);
                    //OutputStream.WriteError(userData.TaskId, e);
                    userData.taskSource.SetException(e);
                }

                if (userData != null && userData.Record != null)
                {
                    if (e == null)
                    {
                        userData.Record.PercentComplete = 100;
                    }

                    userData.Record.StatusDescription = status;
                    ProgressStream.WriteStream(userData.Record);
                }
            //}
            //finally
            //{
            //    TaskCounter.Signal();
            //}
        }

        /// <summary>
        /// On Task run successfully
        /// </summary>
        /// <param name="data">User data</param>
        protected virtual void OnTaskSuccessful(DataMovementUserData data)
        { }

        /// <summary>
        /// Begin processing
        /// </summary>
        protected override void BeginProcessing()
        {
            base.BeginProcessing();

            BlobTransferOptions opts = new BlobTransferOptions();
            opts.Concurrency = GetCmdletConcurrency();
            opts.AppendToClientRequestId(CmdletOperationContext.ClientRequestId);
            opts.OverwritePromptCallback = ConfirmOverwrite;
            transferManager = new BlobTransferManager(opts);

            CmdletCancellationToken.Register(() => transferManager.CancelWorkAndWaitForCompletion());
        }

        /// <summary>
        /// Start async task in transfer manager
        /// </summary>
        /// <param name="taskStartAction">Task  start action</param>
        /// <param name="record"></param>
        //protected void StartAsyncTaskInTransferManager(Func<BlobTransferManager, Task> taskStartAction, long taskId)
        //{
        //    //FIXME convert to task based api and run with RunConcurrentTask
        //    //TaskTotalCount++;

        //    //TaskCounter.AddCount();

        //    //try
        //    //{
        //    RunConcurrentTask(taskStartAction(transferManager));
        //    //}
        //    //catch
        //    //{
        //    //    TaskCounter.Signal();
        //    //    throw;
        //    //}
        //FIXME Do we need to output the main stream?
        //    GatherStreamToMainThread();
        //}

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
