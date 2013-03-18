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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;

    internal class ThreadWorker<T>
    {
        protected CancellationToken Token;
        private ConcurrentQueue<T> jobList;
        private List<T> remainedJobList;
        private int processPointer;
        private bool forceQuit;
        public int ThreadId
        {
            get;
            set;
        }

        private bool enableQuit
        {
            get;
            set;
        }
        
        public ConcurrentQueue<T> JobList
        {
            get
            {
                return jobList;
            }
        }

        public Func<T, bool> ProcessJobItem;

        protected CountdownEvent ThreadCounter
        {
            get;
            set;
        }

        protected MultiThreadStreamWriter<Exception> ErrorStream
        {
            get;
            private set;
        }

        private ThreadWorker()
        {
            
        }

        public ThreadWorker(CancellationToken token, CountdownEvent threadCounter, MultiThreadStreamWriter<Exception> errorStream)
        {
            Token = token;
            ThreadCounter = threadCounter;
            ErrorStream = errorStream;
            processPointer = 0;
            jobList = new ConcurrentQueue<T>();
            remainedJobList = new List<T>();
        }

        public void EnableQuit()
        {
            enableQuit = true;
        }

        public void Abort()
        {
            forceQuit = true;
            enableQuit = true;
        }

        public virtual void Run(object data)
        {
            Console.WriteLine("Thread {0} running", ThreadId);

            try
            {
                T jobItem = default(T);
                int yieldTimeSlice = 100;

                while (!Token.IsCancellationRequested && !forceQuit)
                {
                    bool dequeueSuccessed = jobList.TryDequeue(out jobItem);
                    bool finished = true;
                    if (!dequeueSuccessed) //In most case it should be successed except wait for adding task or job done
                    {
                        if (remainedJobList.Count > 0)
                        {
                            if (processPointer >= remainedJobList.Count)
                            {
                                processPointer = 0;
                            }

                            jobItem = remainedJobList[processPointer];
                        }
                        else
                        {
                            if (enableQuit)
                            {
                                //All task has done
                                break;
                            }
                            else
                            {
                                //The thread should yields the rest of its current slice of processor time
                                //The yiled time slice should be shorter than the time of one single rest call
                                //so it will be better if there are only few command to do.
                                Thread.Sleep(yieldTimeSlice);
                                //Console.WriteLine("Sleep");
                                continue;
                            }
                        }
                    }

                    try
                    {
                        if (ProcessJobItem != null)
                        {
                            finished = ProcessJobItem(jobItem);
                        }
                    }
                    catch (Exception e)
                    {
                        ErrorStream.WriteStream(e);
                        finished = true;
                    }
                    finally
                    {
                        if (finished)
                        {
                            if (!dequeueSuccessed)
                            {
                                remainedJobList.RemoveAt(processPointer);
                            }
                            Console.WriteLine("Job finished");
                        }
                        else
                        {
                            if (dequeueSuccessed)
                            {
                                Console.WriteLine("Added to remained job");
                                remainedJobList.Add(jobItem);
                            }
                            else
                            {
                                processPointer++;
                                Console.WriteLine("Process next job");
                            }
                        }
                    }
                }
            }
            finally
            {
                Console.WriteLine("Thread {0} quit", ThreadId);
                ThreadCounter.Signal();
            }
        }
    }
}
