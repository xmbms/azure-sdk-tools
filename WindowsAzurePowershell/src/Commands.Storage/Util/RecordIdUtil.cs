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

namespace Microsoft.WindowsAzure.Commands.Storage.Utilities
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Management.Automation;
    using System.Text;
    using System.Threading;

    public static class RecordIdUtil
    {
        /// <summary>
        /// Start index for detail process record id. O is reseverd for summary record.
        /// </summary>
        private static int DetailRecordStartIndex = 1;

        /// <summary>
        /// The default count for detail progress record. Notice: we actually use DefaultDetailRecordCount progress records + 1 summary progress record
        /// </summary>
        private static int MaxDetailRecordCount = 4;

        /// <summary>
        /// Current record id
        /// </summary>
        private static int CurrentRecordId = 0;

        /// <summary>
        /// Get Record id
        /// </summary>
        /// <returns>Record id</returns>
        public static int GetRecordId()
        {
            int id = Interlocked.Increment(ref CurrentRecordId);
            id = DetailRecordStartIndex + id % MaxDetailRecordCount;
            Interlocked.CompareExchange(ref CurrentRecordId, 0, MaxDetailRecordCount);
            return id;
        }
    }
}
