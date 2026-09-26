/*-------------------------------------------------------------------------
 *
 * fast_recovery_worker.c
 *    Background worker that proactively replays WAL for all pages in
 *    the LSN index, so that on-demand replay overhead is eliminated
 *    over time.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "access/xlog_internal.h"
#include "access/rmgr.h"
#include "lib/dshash.h"
#include "storage/bufmgr.h"
#include "access/lsn_indexer.h"
#include "miscadmin.h"
#include "postmaster/auxprocess.h"
#include "storage/ipc.h"
#include "postmaster/fast_recovery_worker.h"

/*
 * FastRecoveryMain — iterate every page in the LSN index and read it,
 * which triggers on-demand WAL replay via ReadBuffer_common.
 *
 * IMPORTANT: dshash_seq_next holds the partition LWLock until the next
 * call, term, or partition crossing.  ReadBufferWithoutRelcache below
 * triggers LSNIndexReplayPage, which calls dshash_find() — that would
 * try to re-acquire the SAME partition lock and self-deadlock (LWLocks
 * are not reentrant).
 *
 * To break that, collect all tags into a local list under the seq
 * iteration, terminate the iteration to release partition locks, and
 * THEN read each buffer.
 */
static void
FastRecoveryMain(void)
{
	dshash_seq_status	seq;
	PageLSNEntry	   *entry;
	BufferTag		   *tags = NULL;
	int					ntags = 0;
	int					ncap = 0;

	elog(LOG, "Fast recovery worker started");

	LSNIndexAttach();

	/*
	 * Phase 1: snapshot all tags.  dshash_seq_next holds the partition
	 * lock and asserts it on the next call, so we MUST NOT call
	 * dshash_release_lock between iterations — let seq_next/term manage
	 * the partition lock lifecycle.  We just copy the tag value out.
	 */
	dshash_seq_init(&seq, lsn_hash_for_worker(), false);
	while ((entry = (PageLSNEntry *) dshash_seq_next(&seq)) != NULL)
	{
		if (ntags == ncap)
		{
			ncap = ncap ? ncap * 2 : 1024;
			tags = (BufferTag *) repalloc(tags ? tags : palloc(0),
										  sizeof(BufferTag) * ncap);
		}
		tags[ntags++] = entry->tag;
	}
	dshash_seq_term(&seq);

	elog(LOG, "Fast recovery worker: replaying %d pages", ntags);

	/* Phase 2: trigger on-demand replay for each page. */
	for (int i = 0; i < ntags; i++)
	{
		Buffer buffer;

		buffer = ReadBufferWithoutRelcache(BufTagGetRelFileLocator(&tags[i]),
										   tags[i].forkNum,
										   tags[i].blockNum,
										   RBM_NORMAL, NULL, true);
		ReleaseBuffer(buffer);
	}

	if (tags)
		pfree(tags);

	/* All pages recovered — destroy the index */
	LSNIndexDestroy();

	elog(LOG, "Fast recovery complete. Exiting.");
}

void
FastRecoveryWorkerMain(const void *startup_data, size_t startup_data_len)
{
	MyBackendType = B_FAST_RECOVERY_WORKER;
	AuxiliaryProcessMainCommon();

	/*
	 * Properly accept or ignore signals the postmaster might send us.
	 */
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGCHLD, SIG_DFL);

	FastRecoveryMain();

	proc_exit(0);
}
