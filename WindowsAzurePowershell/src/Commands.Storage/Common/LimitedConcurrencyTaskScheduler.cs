namespace Microsoft.WindowsAzure.Commands.Storage.Common
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    internal class LimitedConcurrencyTaskScheduler
    {
        /// <summary>
        /// Task number counter
        ///     The following counter should be used with Interlocked
        /// </summary>
        private long totalTaskCount = 0;
        private long failedTaskCount = 0;
        private long finishedTaskCount = 0;
        private long activeTaskCount = 0;
        private int maxConcurrency = 0;
        private CancellationToken cancellationToken;

        public long TotalTaskCount { get { return Interlocked.Read(ref totalTaskCount); } }
        public long FailedTaskCount { get { return Interlocked.Read(ref failedTaskCount); } }
        public long FinishedTaskCount { get { return Interlocked.Read(ref finishedTaskCount); } }
        public long ActiveTaskCount { get { return Interlocked.Read(ref activeTaskCount); } }

        public delegate void ErrorEventHandler(object sender, ExceptionEventArgs e);

        public event ErrorEventHandler OnError;

        private ConcurrentQueue<Tuple<long, Func<Task>>> taskQueue;

        public bool IsCompleted 
        {
            get 
            {
                return ActiveTaskCount == 0 && taskQueue.Count == 0;
            }
        }

        /// <summary>
        /// Task status
        /// Key: Output id
        /// Value: Task is done or not.
        /// </summary>
        private ConcurrentDictionary<long, bool> TaskStatus;

        public LimitedConcurrencyTaskScheduler(int maxConcurrency, CancellationToken cancellationToken)
        {
            taskQueue = new ConcurrentQueue<Tuple<long, Func<Task>>>();
            this.maxConcurrency = maxConcurrency;
            this.cancellationToken = cancellationToken;
        }

        /// <summary>
        /// Get available task id
        ///     thread unsafe since it should only run in main thread
        /// </summary>
        public long GetAvailableTaskId()
        {
            return totalTaskCount;
        }

        public bool IsTaskCompleted(long taskId)
        {
            bool finished = false;
            bool existed = TaskStatus.TryGetValue(taskId, out finished);
            return existed && finished;
        }

        /// <summary>
        /// Run async task
        /// </summary>
        /// <param name="task">Task operation</param>
        /// <param name="taskId">Task id</param>
        protected async void RunConcurrentTask(long taskId, Task task)
        {
            bool initTaskStatus = false;
            bool finishedTaskStatus = true;

            Interlocked.Increment(ref activeTaskCount);

            try
            {
                TaskStatus.TryAdd(taskId, initTaskStatus);
                await task.ConfigureAwait(false);
                Interlocked.Increment(ref finishedTaskCount);
            }
            catch (Exception e)
            {
                Interlocked.Increment(ref failedTaskCount);

                if (OnError != null)
                {
                    ExceptionEventArgs eventArgs = new ExceptionEventArgs(taskId, e);
                    OnError(this, eventArgs);
                }
            }
            finally
            {
                TaskStatus.TryUpdate(taskId, finishedTaskStatus, initTaskStatus);
            }

            Interlocked.Decrement(ref activeTaskCount);

            RunRemainingTask();
        }

        private void RunRemainingTask()
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            Tuple<long, Func<Task>> remainingTask = null;
            taskQueue.TryDequeue(out remainingTask);
            if (remainingTask != null)
            {
                Task task = remainingTask.Item2();
                RunConcurrentTask(remainingTask.Item1, task);
            }
        }

        public void RunTask(long taskId, Func<Task> taskGenerator)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            Interlocked.Increment(ref totalTaskCount);

            if (Interlocked.Read(ref activeTaskCount) < maxConcurrency)
            {
                Task task = taskGenerator();
                RunConcurrentTask(taskId, task);
            }
            else
            {
                Tuple<long, Func<Task>> waitTask = new Tuple<long, Func<Task>>(taskId, taskGenerator);
                taskQueue.Enqueue(waitTask);
            }
        }
    }

    public class ExceptionEventArgs : EventArgs
    {
        public long TaskId { get; private set; }
        public Exception Exception { get; private set; }

        public ExceptionEventArgs(long taskId, Exception e)
        {
            TaskId = taskId;
            Exception = e;
        }
    }
}
