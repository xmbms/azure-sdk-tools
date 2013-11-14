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

namespace Microsoft.WindowsAzure.Commands.Storage.Utilities
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;

    internal static class FileUtil
    {
        /// <summary>
        /// the root dir for uploading file
        /// make sure the root dir is lower case since windows file path is case insensitive
        /// </summary>
        private static string CurrentUploadRootDir = String.Empty;

        /// <summary>
        /// Directory exist cache
        /// </summary>
        private static Dictionary<string, bool> DirectoryCache = new Dictionary<string, bool>();

        /// <summary>
        /// get blob name according to the relative file path
        /// </summary>
        /// <param name="filePath">absolute file path</param>
        /// <returns>blob name</returns>
        public static string GetInferredBlobNameFromFilePath(string filePath)
        {
            string blobName = string.Empty;
            string fileName = Path.GetFileName(filePath);
            string dirPath = Path.GetDirectoryName(filePath).ToLower();

            if (string.IsNullOrEmpty(CurrentUploadRootDir) || !dirPath.StartsWith(CurrentUploadRootDir))
            {
                //if CurrentUploadRootDir dir is empty or dir path is not sub folder of the sending root dir
                //set the current dir as the root sending dir
                CurrentUploadRootDir = dirPath + Path.DirectorySeparatorChar;
                blobName = fileName;
            }
            else
            {
                blobName = filePath.Substring(CurrentUploadRootDir.Length);
            }

            return blobName;
        }

        /// <summary>
        /// Clear File system cache
        /// </summary>
        public static void ClearCache()
        {
            DirectoryCache.Clear();
        }

        private static string FromatDirectorPath(string absolutePath)
        {
            if (!string.IsNullOrEmpty(absolutePath))
            {
                if (absolutePath[absolutePath.Length - 1] != Path.PathSeparator)
                {
                    absolutePath += Path.PathSeparator;
                }
            }

            return absolutePath;
        }

        public static bool DoesDirectoryExist(string absolutePath)
        {
            absolutePath = FromatDirectorPath(absolutePath);

            if (!DirectoryCache.ContainsKey(absolutePath))
            {
                bool exists = Directory.Exists(absolutePath);
                DirectoryCache[absolutePath] = exists;
            }

            return DirectoryCache[absolutePath];
        }
    }
}
