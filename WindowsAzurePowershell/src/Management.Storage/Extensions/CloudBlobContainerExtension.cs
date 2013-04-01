using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Management.Storage.Extensions
{
    public static class CloudBlobContainerExtension
    {
        public static Task<BlobContainerPermissions> GetPermissionsAsync(
            this CloudBlobContainer container)
        {
            var tcs = new TaskCompletionSource<BlobContainerPermissions>();
            container.BeginGetPermissions(result =>
                {
                    try 
                    {
                        Thread.Sleep(1000);
                        tcs.SetResult(container.EndGetPermissions(result)); 
                    }
                    catch (Exception e)
                    {
                        tcs.SetException(e);
                    }
                }, null);
            return tcs.Task;
        }
    }
}
