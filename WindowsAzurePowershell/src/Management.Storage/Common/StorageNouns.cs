﻿// ----------------------------------------------------------------------------------
//
// Copyright 2011 Microsoft Corporation
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

    /// <summary>
    /// storage nouns for cmdlet name
    /// </summary>
    public static class StorageNouns
    {
        /// <summary>
        /// blob cmdlet name
        /// </summary>
        public const string Blob = "AzureStorageBlob";

        /// <summary>
        /// blobcontent cmdlet name
        /// </summary>
        public const string BlobContent = "AzureStorageBlobContent";

        /// <summary>
        /// container cmdlet name
        /// </summary>
        public const string Container = "AzureStorageContainer";

        /// <summary>
        /// http protocol
        /// </summary>
        public const string HTTP = "http";

        /// <summary>
        /// https protocol
        /// </summary>
        public const string HTTPS = "https";

        /// <summary>
        /// queue cmdlet name
        /// </summary>
        public const string Queue = "AzureStorageQueue";

        /// <summary>
        /// storage context cmdlet name
        /// </summary>
        public const string StorageContext = "AzureStorageContext";

        /// <summary>
        /// table cmdlet name
        /// </summary>
        public const string Table = "AzureStorageTable";
    }
}