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
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;

    class JobManager<T> : IDisposable
    {
        private int AsyncTasksPerCoreMultiplier = 2;
        protected CancellationToken Token;

        /// <summary>
        /// MultiThread WriteException
        /// </summary>
        private MultiThreadStreamWriter<Exception> ErrorStream;

        private List<ThreadWorker<T>> ConcurrentWorker;

        private CountdownEvent ThreadCounter;
        private bool CouldQuit;
        private int jobCount = 0;

        private JobManager()
        { 
        }

        public JobManager(CancellationToken token, MultiThreadStreamWriter<Exception> errorStream, Func<T, bool> process, int threadCount)
        {
            ThreadCounter = new CountdownEvent(1);
            CouldQuit = false;
            Token = token;
            ErrorStream = errorStream;
            ConcurrentWorker = new List<ThreadWorker<T>>();
            InitThreadWorker(process, threadCount);
        }

        public int DispatchJob(T jobItem)
        {
            int workerNumber = jobCount % ConcurrentWorker.Count;
            ConcurrentWorker[workerNumber].JobList.Enqueue(jobItem);
            //Console.WriteLine("Dispatch job to thread {0}", workerNumber);
            jobCount++;
            return workerNumber;
        }

        public int InitThreadWorker(Func<T, bool> process, int threadCount)
        {
            if (threadCount == 0)
            {
                threadCount = Environment.ProcessorCount* AsyncTasksPerCoreMultiplier;
            }

            for (int i = 0, count = threadCount; i < count; i++)
            {
                ThreadCounter.AddCount();

                try
                {
                    ThreadWorker<T> worker = new ThreadWorker<T>(Token, ThreadCounter, ErrorStream);
                    worker.ThreadId = i;
                    worker.ProcessJobItem = process;
                    ConcurrentWorker.Add(worker);
                    ThreadPool.QueueUserWorkItem(worker.Run);
                }
                catch
                {
                    ThreadCounter.Signal();
                    throw;
                }
            }

            //Console.WriteLine("Add {0} Thread", threadCount);

            return threadCount;
        }

        public bool PeriodWaitForComplete()
        {
            if (!CouldQuit)
            {
                //Console.WriteLine("Thread before realse {0}", ThreadCounter.CurrentCount);
                CouldQuit = true;
                ThreadCounter.Signal();
                ConcurrentWorker.ForEach((worker) => worker.EnableQuit());
                //Console.WriteLine("Release the init thread. current thread {0}", ThreadCounter.CurrentCount);
            }
            //Console.WriteLine("Current {0} Thread", ThreadCounter.CurrentCount);
            int millisecondsTimeout = 1000;
            bool quit = ThreadCounter.Wait(millisecondsTimeout, Token);
            //Console.WriteLine("Could Quit : {0}", quit);
            return quit;
        }

        public void Abort()
        {
            ConcurrentWorker.ForEach((worker) => worker.Abort());
        }

        public void Dispose()
        {
            Abort();
        }
    }
}
