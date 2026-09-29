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
#include "libpq/pqsignal.h"
#include "storage/bufmgr.h"
#include "access/lsn_indexer.h"
#include "miscadmin.h"
#include "postmaster/auxprocess.h"
#include "postmaster/bgwriter.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/procsignal.h"
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

		/* keep up with ProcSignal barriers and config reloads */
		ProcessMainLoopInterrupts();

		buffer = ReadBufferWithoutRelcache(BufTagGetRelFileLocator(&tags[i]),
										   tags[i].forkNum,
										   tags[i].blockNum,
										   RBM_NORMAL, NULL, true);
		ReleaseBuffer(buffer);
	}

	if (tags)
		pfree(tags);

	/*
	 * Every page has been replayed.  End fast recovery: stop new replays,
	 * wait out the running ones, free the index.  Checkpoints are allowed
	 * from then on.
	 */
	LSNIndexFinish();

	/*
	 * Checkpoints were refused until now, so the last one is still the one
	 * from before the crash, and another crash would have to recover all of
	 * that WAL again.  Ask for a new checkpoint, like the end-of-recovery
	 * checkpoint normal crash recovery takes, but without waiting for it.
	 */
	RequestCheckpoint(CHECKPOINT_FORCE);

	elog(LOG, "Fast recovery complete. Exiting.");
}

void
FastRecoveryWorkerMain(const void *startup_data, size_t startup_data_len)
{
	MyBackendType = B_FAST_RECOVERY_WORKER;
	AuxiliaryProcessMainCommon();

	/*
	 * Properly accept or ignore signals that might be sent to us.
	 *
	 * SIGTERM is ignored on purpose.  A smart or fast shutdown must not write
	 * its shutdown checkpoint while pages are still unrecovered, so the
	 * postmaster waits for us to finish instead of stopping us: we are in
	 * the set of processes it waits for in PM_WAIT_BACKENDS.  An immediate
	 * shutdown uses SIGQUIT, whose handler InitPostmasterChild already set
	 * up; it writes no checkpoint, so the next startup recovers again.
	 */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, PG_SIG_IGN);
	pqsignal(SIGTERM, PG_SIG_IGN);
	pqsignal(SIGALRM, PG_SIG_IGN);
	pqsignal(SIGPIPE, PG_SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, PG_SIG_IGN);
	pqsignal(SIGCHLD, PG_SIG_DFL);

	/* Unblock signals (they were blocked when the postmaster forked us) */
	sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);

	FastRecoveryMain();

	proc_exit(0);
}
