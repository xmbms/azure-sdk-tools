namespace Microsoft.WindowsAzure.Management.Storage.Blob.Cmdlet
{
    using Microsoft.WindowsAzure.Management.Storage.Common;
    using Microsoft.WindowsAzure.Management.Storage.Model.ResourceModel;
    using Microsoft.WindowsAzure.Storage.Blob;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Management.Automation;
    using System.Net;
    using System.Text;
    using System.Threading;

    /// <summary>
    /// List azure storage container
    /// </summary>
    [Cmdlet(VerbsCommon.Get, "AsyncContainer"),
        OutputType(typeof(AzureStorageContainer))]
    public class GetAzureStorageContainerInAsync : GetAzureStorageContainerCommand
    {
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
        }

        bool quit = false;
        int count = 0;
        /// <summary>
        /// Pack CloudBlobContainer and it's permission to AzureStorageContainer object
        /// </summary>
        /// <param name="containerList">An enumerable collection of CloudBlobContainer</param>
        /// <returns>An enumerable collection of AzureStorageContainer</returns>
        internal void AsyncPackCloudBlobContainerWithAcl(IEnumerable<CloudBlobContainer> containerList)
        {
            ThreadPool.SetMinThreads(100, 100);

            foreach (CloudBlobContainer container in containerList)
            {
                Interlocked.Increment(ref count);
                container.BeginGetPermissions(ar =>
                    {
                        BlobContainerPermissions permission = container.EndGetPermissions(ar);
                        AzureStorageContainer azureContainer = new AzureStorageContainer(container, permission);
                        OutputStream.WriteStream(azureContainer);
                        Interlocked.Decrement(ref count);
                    }, null);
            }
            quit = true;
        }

        protected override void BeginProcessing()
        {
            ConfigureServicePointManager();
            base.BeginProcessing();
        }

        public override void ExecuteCmdlet()
        {
            SetupMultiThreadOutputStream();
            IEnumerable<CloudBlobContainer> containerList = null;

            containerList = ListContainersByName(String.Empty);

            AsyncPackCloudBlobContainerWithAcl(containerList);
        }

        protected override void EndProcessing()
        {
            do
            {
                GatherStreamToMainThread();
                Thread.Sleep(1000);
            }
            while (!quit || count != 0);
            
            GatherStreamToMainThread();

            base.EndProcessing();
        }

        /// <summary>
        /// Configure Service Point
        /// </summary>
        private void ConfigureServicePointManager()
        {
            ServicePointManager.DefaultConnectionLimit = 100;
            ServicePointManager.Expect100Continue = false;
            ServicePointManager.UseNagleAlgorithm = true;
        }

        protected void GatherStreamToMainThread()
        {
            ProgressStream.WriteStreamToMainThread(WriteProgress);
            ErrorStream.WriteStreamToMainThread(WriteExceptionError);
            VerboseStream.WriteStreamToMainThread(WriteDebugLog);
            OutputStream.WriteStreamToMainThread(WriteObject);
        }
    }
}
