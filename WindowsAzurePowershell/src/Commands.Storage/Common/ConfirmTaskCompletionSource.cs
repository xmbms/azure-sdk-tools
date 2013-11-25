namespace Microsoft.WindowsAzure.Commands.Storage.Common
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    internal class ConfirmTaskCompletionSource : TaskCompletionSource<bool>
    {
        public string Message { get; private set; }

        public ConfirmTaskCompletionSource(string message) : base()
        {
            Message = message;
        }
    }
}
