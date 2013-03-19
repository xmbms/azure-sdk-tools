namespace Microsoft.WindowsAzure.Management.Storage.Blob.Cmdlet
{
    using Microsoft.WindowsAzure.Management.Storage.Common;
    using Microsoft.WindowsAzure.Management.Storage.Model.ResourceModel;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Management.Automation;
    using System.Text;
    using System.Threading;

    /// <summary>
    /// List azure storage container
    /// </summary>
    [Cmdlet(VerbsCommon.Get, "AzContainer"),
        OutputType(typeof(AzureStorageContainer))]
    public class GetAzureStorageContainerInMasterSlaveMode : GetAzureStorageContainerCommand
    {
        private bool useMultiThread = false;
        private CancellationTokenSource tokenSource;

        public bool IsMultiThread
        {
            get
            {
                return useMultiThread;
            }
        }

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
        /// Set up the cmdlet runtime for multi thread
        /// </summary>
        internal void SetupMultiThreadOutputStream()
        {
            OutputStream = new MultiThreadStreamWriter<object>();
            ErrorStream = new MultiThreadStreamWriter<Exception>();
            VerboseStream = new MultiThreadStreamWriter<string>();
            ProgressStream = new MultiThreadStreamWriter<ProgressRecord>();
            useMultiThread = true;
        }

        private JobManager<CloudBlobContainer> jobManager;

        /// <summary>
        /// Cmdlet begin process
        /// </summary>
        protected override void BeginProcessing()
        {
            base.BeginProcessing();
            tokenSource = new CancellationTokenSource();
            SetupMultiThreadOutputStream();
            jobManager = new JobManager<CloudBlobContainer>(tokenSource.Token, ErrorStream, GetContainerInThreadWorker);
        }

        protected bool GetContainerInThreadWorker(CloudBlobContainer container)
        {
            BlobRequestOptions requestOptions = null;
            AccessCondition accessCondition = null;

            BlobContainerPermissions permissions = null;

            try
            {
                permissions = Channel.GetContainerPermissions(container, accessCondition, requestOptions, OperationContext);
            }
            catch (Exception e)
            {
                //Log the error message and continue the process
                ErrorStream.WriteStream(e);
            }

            AzureStorageContainer azureContainer = new AzureStorageContainer(container, permissions);
            OutputStream.WriteStream(azureContainer);
            return true;
        }

        /// <summary>
        /// Pack CloudBlobContainer and it's permission to AzureStorageContainer object
        /// </summary>
        /// <param name="containerList">An enumerable collection of CloudBlobContainer</param>
        /// <returns>An enumerable collection of AzureStorageContainer</returns>
        internal void MasterPackCloudBlobContainerWithAcl(IEnumerable<CloudBlobContainer> containerList)
        {
            foreach (CloudBlobContainer container in containerList)
            {
                jobManager.DispatchJob(container);
            }
        }

        public override void ExecuteCmdlet()
        {
            IEnumerable<CloudBlobContainer> containerList = null;

            containerList = ListContainersByName(String.Empty);

            MasterPackCloudBlobContainerWithAcl(containerList);
        }

        protected override void EndProcessing()
        {
            do
            {
                GatherStreamToMainThread();
            }
            while (!jobManager.PeriodWaitForComplete());

            GatherStreamToMainThread();

            base.EndProcessing();
        }

        protected void GatherStreamToMainThread()
        {
            if (IsMultiThread)
            {
                ProgressStream.WriteStreamToMainThread(WriteProgress);
                ErrorStream.WriteStreamToMainThread(WriteExceptionError);
                VerboseStream.WriteStreamToMainThread(WriteDebugLog);
                OutputStream.WriteStreamToMainThread(WriteObject);
            }
        }
    }
}