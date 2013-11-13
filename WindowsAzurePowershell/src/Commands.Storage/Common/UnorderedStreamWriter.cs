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

namespace Microsoft.WindowsAzure.Commands.Storage.Common
{
    using System;
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.Linq;
    using System.Management.Automation;

    /// <summary>
    /// Stream writer used in multi-thread environment
    /// It can be used as Multi-thread WriteDebug/WriteError/WriteObject/WriteVerbose/WriteWarning
    /// </summary>
    /// <typeparam name="T">Output Message Type</typeparam>
    internal class UnorderedStreamWriter<T>
    {
        /// <summary>
        /// Message Queue
        /// </summary>
        private ConcurrentQueue<T> Stream;

        /// <summary>
        /// The output writer in main thread
        /// </summary>
        private Action<T> MainThreadWriter;

        /// <summary>
        /// MultiThreadStreamWriter Constructor
        /// </summary>
        public UnorderedStreamWriter(Action<T> writer)
        {
            Stream = new ConcurrentQueue<T>();
            MainThreadWriter = writer;
        }

        /// <summary>
        /// Write message to multithread stream writer.
        /// </summary>
        /// <param name="message">Output message</param>
        public void WriteStream(T message)
        {
            Stream.Enqueue(message);
        }

        /// <summary>
        /// Write output stream to main thread
        /// There is no concurrent issue since it should only run in main thread.
        /// </summary>
        public void Output()
        {
            //Use the current count in stream queue.
            //so it can't always occupy the main thread.
            int count = Stream.Count();
            T result = default(T);
            bool successful = false;

            while (count > 0)
            {
                successful = Stream.TryDequeue(out result);

                if (successful) //It should be always successful.
                {
                    count--;

                    if (MainThreadWriter != null)
                    {
                        try
                        {
                            MainThreadWriter(result);
                        }
                        catch (PipelineStoppedException)
                        {
                            //Directly stop the output stream when throw an exception.
                            //If so, we could quickly response for ctrl + c and etc.
                            break;
                        }
                        catch (Exception e)
                        {
                            Debug.Fail(Resources.DebugMainThreadWriterThrowException, e.Message);
                            break;
                        }
                    }
                }
                else
                {
                    Debug.Fail(Resources.DebugTryDequeueShouldNeverFail);
                }
            }
        }
    }
}
