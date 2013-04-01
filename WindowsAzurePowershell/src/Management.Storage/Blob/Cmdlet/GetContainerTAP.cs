using Microsoft.WindowsAzure.Management.Storage.Model.ResourceModel;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Management.Automation;
using System.Text;
using System.Threading;
using Microsoft.WindowsAzure.Management.Storage.Extensions;

namespace Microsoft.WindowsAzure.Management.Storage.Blob.Cmdlet
{
    /// <summary>
    /// List azure storage container
    /// </summary>
    [Cmdlet(VerbsCommon.Get, "TAPContainer"),
        OutputType(typeof(AzureStorageContainer))]
    public class GetContainerTAP : GetAzureStorageContainerInAsync
    {
        /// <summary>
        /// Pack CloudBlobContainer and it's permission to AzureStorageContainer object
        /// </summary>
        /// <param name="containerList">An enumerable collection of CloudBlobContainer</param>
        /// <returns>An enumerable collection of AzureStorageContainer</returns>
        internal async void TAPAsyncPackCloudBlobContainerWithAcl(IEnumerable<CloudBlobContainer> containerList)
        {
            foreach (CloudBlobContainer container in containerList)
            {
                GetTAPContainer(container);
            }
            quit = true;
        }

        internal async void GetTAPContainer(CloudBlobContainer container)
        {
            Interlocked.Increment(ref count);
            BlobContainerPermissions permission = await container.GetPermissionsAsync();
            AzureStorageContainer azureContainer = new AzureStorageContainer(container, permission);
            OutputStream.WriteStream(azureContainer);
            Interlocked.Decrement(ref count);
        }

        public override void ExecuteCmdlet()
        {
            SetupMultiThreadOutputStream();
            IEnumerable<CloudBlobContainer> containerList = null;

            containerList = ListContainersByName(String.Empty);

            TAPAsyncPackCloudBlobContainerWithAcl(containerList);
        }
    }
}
