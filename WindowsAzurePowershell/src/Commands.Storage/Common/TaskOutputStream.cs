namespace Microsoft.WindowsAzure.Commands.Storage.Common
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Management.Automation;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    internal class TaskOutputStream
    {
        /// <summary>
        /// Ouput Stream which store all the output from sub-thread.
        /// Both the object and exception are the valid output.
        /// Key: Output id
        /// Value: OutputUnit object which store the output data
        /// </summary>
        private ConcurrentDictionary<long, OutputUnit> OutputStream;

        /// <summary>
        /// Current output id
        /// </summary>
        private long CurrentOutputId;
        
        /// <summary>
        /// Main thread output writer. WriteObject is a good candidate for it.
        /// </summary>
        private Action<object> OutputWriter;

        /// <summary>
        /// Main thread error writer. WriteError is a goog candidate for it.
        /// </summary>
        private Action<Exception> ErrorWriter;

        private Action<string> VerboseWriter;

        private Action<ProgressRecord> ProgressWriter;

        private Func<string, string, string, bool> ConfirmWriter;

        public AutoResetEvent dataEvent;

        /// <summary>
        /// Create an OrderedStreamWriter
        /// </summary>
        /// <param name="outputWriter">Main thread output writer</param>
        /// <param name="errorWriter">Main thread error writer</param>
        public TaskOutputStream(Action<object> outputWriter, Action<Exception> errorWriter,
            Action<string> verboseWriter, Action<ProgressRecord> progressWriter, Func<string, string, string, bool> confirmWriter)
        {
            OutputWriter = outputWriter;
            ErrorWriter = errorWriter;
            VerboseWriter = verboseWriter;
            ProgressWriter = progressWriter;
            OutputStream = new ConcurrentDictionary<long, OutputUnit>();
            CurrentOutputId = 0;
            ConfirmWriter = confirmWriter;
            dataEvent = new AutoResetEvent(false);
        }

        /// <summary>
        /// Write output unit into OutputStream
        /// </summary>
        /// <param name="id">Output id</param>
        /// <param name="unit">Output unit</param>
        private void WriteOutputUnit(long id, OutputUnit unit)
        {
            OutputStream.AddOrUpdate(id, unit, (key, oldUnit) => 
                {
                    //Merge data queue
                    List<object> newDataQueue = unit.DataQueue.ToList();
                    foreach (object data in newDataQueue)
                    {
                        oldUnit.DataQueue.Enqueue(data);
                    }
                    return oldUnit;
                });
        }

        /// <summary>
        /// Write object into OutputStream
        /// </summary>
        /// <param name="taskId">Output id</param>
        /// <param name="data">Output data</param>
        public void WriteObject(long taskId, object data)
        {
            OutputUnit unit = new OutputUnit(data, OutputType.Object);
            WriteOutputUnit(taskId, unit);
            dataEvent.Set();
        }

        /// <summary>
        /// Write error into OutputStream
        /// </summary>
        /// <param name="taskId">Output id</param>
        /// <param name="e">Exception object</param>
        public void WriteError(long taskId, Exception e)
        {
            OutputUnit unit = new OutputUnit(e, OutputType.Error);
            WriteOutputUnit(taskId, unit);
            dataEvent.Set();
        }

        public void WriteVerbose(long taskId, string message)
        {
            OutputUnit unit = new OutputUnit(message, OutputType.Verbose);
            WriteOutputUnit(taskId, unit);
        }

        public void WriteProgress(long taskId, ProgressRecord record)
        {
            OutputUnit unit = new OutputUnit(record, OutputType.Progress);
            WriteOutputUnit(taskId, unit);
            dataEvent.Set();
        }

        public bool Wait(int millisecondsTimeout, CancellationToken cancellationToken)
        {
            return !cancellationToken.IsCancellationRequested && dataEvent.WaitOne(millisecondsTimeout);
        }

        /// <summary>
        /// The operation that should be confirmed by user.
        /// </summary>
        private Lazy<ConcurrentQueue<ConfirmTaskCompletionSource>> ConfirmQueue = new Lazy<ConcurrentQueue<ConfirmTaskCompletionSource>>(
            () => new ConcurrentQueue<ConfirmTaskCompletionSource>(), true);

        /// <summary>
        /// Asyc confirm to continue.
        /// *****Please note*****
        /// Dead lock will happen if the main thread is blocked.
        /// </summary>
        /// <param name="message">Confirm message</param>
        public Task<bool> ConfirmAsyc(string message)
        {
            ConfirmTaskCompletionSource tcs = new ConfirmTaskCompletionSource(message);
            ConfirmQueue.Value.Enqueue(tcs);
            dataEvent.Set();
            return tcs.Task;
        }

        internal void ConfirmRequest(ConfirmTaskCompletionSource tcs)
        {
            bool result = ConfirmWriter(string.Empty, tcs.Message, Resources.ConfirmCaption);
            tcs.SetResult(result);
        }

        protected void ConfirmQueuedRequest()
        {
            if (ConfirmQueue.IsValueCreated)
            {
                ConfirmTaskCompletionSource tcs = null;
                while (ConfirmQueue.Value.TryDequeue(out tcs))
                {
                    ConfirmRequest(tcs);
                }
            }
        }

        /// <summary>
        /// Output data into main thread
        /// There is no concurrent call on this method since it should be only called in main thread of the powershell instance.
        /// </summary>
        public void Output()
        {
            ConfirmQueuedRequest();

            OutputUnit unit = null;
            bool removed = false;
            bool taskDone = false;
            bool existed = false;
            do
            {
                existed = TaskStatus.TryGetValue(CurrentOutputId, out taskDone);

                removed = OutputStream.TryRemove(CurrentOutputId, out unit);

                if (removed)
                {
                    try
                    {
                        object data = null;
                        while (unit.DataQueue.TryDequeue(out data))
                        {
                            switch(unit.Type)
                            {
                                case OutputType.Object:
                                    OutputWriter(data);
                                    break;
                                case OutputType.Error:
                                    ErrorWriter(data as Exception);
                                    break;
                                case OutputType.Verbose:
                                    VerboseWriter(data as string);
                                    break;
                                case OutputType.Progress:
                                    ProgressWriter(data as ProgressRecord);
                                    break;
                            }
                        }
                    }
                    catch (PipelineStoppedException)
                    {
                        //Directly stop the output stream when throw an exception.
                        //If so, we could quickly response for ctrl + c and etc.
                        break;
                    }
                    catch (Exception e)
                    {
                        Debug.Fail(String.Format("{0}", e));
                        break;
                    }
                }

                if (existed && taskDone)
                {
                    CurrentOutputId++;
                }   //Otherwise wait for the task completion
            }
            while (removed);
        }

        /// <summary>
        /// Output type
        /// </summary>
        private enum OutputType
        {
            Object,
            Error,
            Verbose,
            Progress
        };

        /// <summary>
        /// Output unit
        /// </summary>
        private class OutputUnit
        {
            /// <summary>
            /// Output type
            /// </summary>
            public OutputType Type;

            /// <summary>
            /// Output list
            /// All the output unit which has the same output key will be merged in the OutputStream
            /// </summary>
            public ConcurrentQueue<object> DataQueue;

            /// <summary>
            /// Create an OutputUnit
            /// </summary>
            /// <param name="type">Output type</param>
            /// <param name="data">Output data</param>
            public OutputUnit(object data, OutputType type)
            {
                Type = type;
                DataQueue = new ConcurrentQueue<object>();
                DataQueue.Enqueue(data);
            }
        }
    }
}
