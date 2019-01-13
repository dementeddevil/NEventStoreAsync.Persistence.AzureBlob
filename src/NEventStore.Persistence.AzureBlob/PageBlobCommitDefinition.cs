using System;

namespace NEventStore.Persistence.AzureBlob
{
    /// <summary>
    /// Definition of a single commit contained within a page blob
    /// </summary>
    [Serializable]
    public class PageBlobCommitDefinition
    {
        // this is the size of an azure blob page
        private const int PageSizeBytes = 512;

        /// <summary>
        /// Get the total number of bytes used for this commit
        /// </summary>
        public int DataSizeBytes { get; }

        /// <summary>
        /// Id of the commit
        /// </summary>
        public Guid CommitId { get; }

        /// <summary>
        /// The utc time of the commit
        /// </summary>
        public DateTime CommitStampUtc { get; }

        /// <summary>
        /// Get the revision
        /// </summary>
        public int Revision { get; }

        /// <summary>
        /// The index into the collections of commits in the stream
        /// </summary>
        public int CommitIndex { get; }

        /// <summary>
        /// Get the start page for the commit
        /// </summary>
        public int StartPage { get; }

        /// <summary>
        /// Get the Checkpoint
        /// </summary>
        public long Checkpoint { get; }

        /// <summary>
        /// Get if the commit has been dispatched
        /// </summary>
        public bool IsDispatched { get; set; }

        /// <summary>
        /// Get the total number of pages used by this commit
        /// </summary>
        public int TotalPagesUsed => DataSizeBytes / PageSizeBytes + ((DataSizeBytes % PageSizeBytes) > 0 ? 1 : 0);

        /// <summary>
        /// Create a new PageBlobCommitDefinition
        /// </summary>
        /// <param name="dataSizeBytes">Size of the blob in bytes.</param>
        /// <param name="commitId">Commit Id.</param>
        /// <param name="revision">Stream Revision.</param>
        /// <param name="commitStampUtc">Commit date/time stamp.</param>
        /// <param name="commitIndex">Commit index</param>
        /// <param name="startPage">start page for this commit in the page blog</param>
        /// <param name="checkPoint">the checkpoint for the commit</param>
        public PageBlobCommitDefinition(int dataSizeBytes, Guid commitId, int revision, DateTime commitStampUtc, int commitIndex, int startPage, long checkPoint)
        {
            DataSizeBytes = dataSizeBytes;
            CommitId = commitId;
            Revision = revision;
            CommitStampUtc = commitStampUtc;
            CommitIndex = commitIndex;
            StartPage = startPage;
            Checkpoint = checkPoint;
            IsDispatched = false;
        }
    }
}
