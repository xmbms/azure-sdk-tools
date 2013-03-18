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

namespace Microsoft.WindowsAzure.Management.Storage.Common
{
    using Microsoft.WindowsAzure.Storage.Blob;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;

    internal class MultiThreadStreamWriter<T>
    {
        private ConcurrentQueue<T> Stream;

        public MultiThreadStreamWriter()
        {
            Stream = new ConcurrentQueue<T>();
        }

        public void WriteStream(T e)
        {
            Stream.Enqueue(e);
        }

        public void WriteStreamToMainThread(Action<T> clientWriter)
        {
            //There is no concurrent call in client
            int count = Stream.Count();
            List<T> tempResults = new List<T>();
            T result = default(T);
            bool successful = false;

            while (count > 0)
            {
                successful = Stream.TryDequeue(out result);

                if (successful)
                {
                    count--;

                    if (clientWriter != null)
                    {
                        try
                        {
                            clientWriter(result);
                        }
                        catch
                        {}
                    }
                }
            }
        }
    }
}
