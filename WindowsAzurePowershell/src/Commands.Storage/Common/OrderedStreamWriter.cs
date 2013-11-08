﻿// ----------------------------------------------------------------------------------
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
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

    /// <summary>
    /// Gather the output from sub-thread and output them in the main thread accroding to the input order
    /// Note: The input order means the order that the main thread dispatch working item to sub-thread, 
    /// instead of the order that sub-thread output the result.
    /// </summary>
    internal class OrderedStreamWriter
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
        /// Locked output id.
        /// The output unit may be too large to output at only one time,
        /// so we have to lock the output id till the output is done.
        /// Key: Output id
        /// Value: the bool indicates whether to lock the output id.
        /// </summary>
        private Lazy<ConcurrentDictionary<long, bool>> LockedStream;
        
        /// <summary>
        /// Main thread output writer. WriteObject is a good candidate for it.
        /// </summary>
        private Action<object> OutputWriter;

        /// <summary>
        /// Main thread error writer. WriteError is a goog candidate for it.
        /// </summary>
        private Action<Exception> ErrorWriter;

        /// <summary>
        /// Create an OrderedStreamWriter
        /// </summary>
        /// <param name="outputWriter">Main thread output writer</param>
        /// <param name="errorWriter">Main thread error writer</param>
        public OrderedStreamWriter(Action<object> outputWriter, Action<Exception> errorWriter)
        {
            OutputWriter = outputWriter;
            ErrorWriter = errorWriter;
            OutputStream = new ConcurrentDictionary<long, OutputUnit>();
            LockedStream = new Lazy<ConcurrentDictionary<long, bool>>();
            CurrentOutputId = 0;
        }

        /// <summary>
        /// Lock the output stream in order to work with IEnumerable
        /// </summary>
        /// <param name="id">The output id</param>
        public void LockStream(long id)
        {
            LockedStream.Value.TryAdd(id, false);
        }

        /// <summary>
        /// Unlock the output stream
        /// </summary>
        /// <param name="id">The output id</param>
        public void UnLockStream(long id)
        {
            bool trivialState = false;
            LockedStream.Value.TryRemove(id, out trivialState);
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
        /// <param name="id">Output id</param>
        /// <param name="data">Output data</param>
        public void WriteObject(long id, object data)
        {
            OutputUnit unit = new OutputUnit(OutputType.Object, data);
            WriteOutputUnit(id, unit);
        }

        /// <summary>
        /// Write error into OutputStream
        /// </summary>
        /// <param name="id">Output id</param>
        /// <param name="e">Exception object</param>
        public void WriteError(long id, Exception e)
        {
            OutputUnit unit = new OutputUnit(OutputType.Error, e);
            WriteOutputUnit(id, unit);
        }

        /// <summary>
        /// Output data into main thread
        /// There is no concurrent call on this method since it should be only called in main thread of the powershell instance.
        /// </summary>
        public void Output()
        {
            OutputUnit unit = null;
            bool removed = false;

            do
            {
                removed = OutputStream.TryRemove(CurrentOutputId, out unit);

                if (removed)
                {
                    try
                    {
                        object data = null;
                        while (unit.DataQueue.TryDequeue(out data))
                        {
                            if (unit.Type == OutputType.Error)
                            {
                                ErrorWriter(data as Exception);
                            }
                            else
                            {
                                OutputWriter(data);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Debug.Fail(String.Format("{0}", e));
                    }

                    if (!LockedStream.IsValueCreated || !LockedStream.Value.ContainsKey(CurrentOutputId))
                    {
                        CurrentOutputId++;
                    }
                }
            }
            while (removed);
        }

        /// <summary>
        /// Output type
        /// </summary>
        private enum OutputType
        {
            Object,
            Error
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
            public OutputUnit(OutputType type, object data)
            {
                Type = type;
                DataQueue = new ConcurrentQueue<object>();
                DataQueue.Enqueue(data);
            }
        }
    }
}
