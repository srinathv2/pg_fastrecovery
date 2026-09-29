/*-------------------------------------------------------------------------
 *
 * lsn_indexer.c
 *    LSN-based WAL indexing for fast crash recovery.
 *
 * During crash recovery the startup process scans WAL records and,
 * instead of replaying them, records each (BufferTag -> LSN) mapping
 * in a dshash table backed by a DSA area.  Later, when a backend or
 * the fast-recovery worker reads a page from disk, the pending WAL
 * records for that page are replayed on demand.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xlogreader.h"
#include "access/xlog_internal.h"
#include "access/xlogutils.h"
#include "access/lsn_indexer.h"
#include "lib/dshash.h"
#include "miscadmin.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/dsa.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"
#include "common/hashfn.h"

/* GUC variable */
bool enable_fast_recovery = false;

/* Control structure in traditional shared memory */
LSNIndexControl *LSNIndexCtl = NULL;

/* Per-process DSA and dshash handles (set by LSNIndexAttach) */
static dsa_area   *lsn_dsa  = NULL;
static dshash_table *lsn_hash = NULL;

/* State for on-demand replay */
BufferTag targetTag;
bool inReplayPageWals = false;

/*
 * dshash parameters for the LSN index.
 *
 * Key = BufferTag (first field of PageLSNEntry).
 * Built-in memcmp/memhash/memcpy work fine for BufferTag.
 */
static const dshash_parameters lsn_dsh_params = {
	sizeof(BufferTag),					/* key_size */
	sizeof(PageLSNEntry),				/* entry_size */
	dshash_memcmp,						/* compare_function */
	dshash_memhash,						/* hash_function */
	dshash_memcpy,						/* copy_function */
	LWTRANCHE_LSN_INDEX_HASH			/* tranche_id */
};

static void LSNIndexShmemRequest(void *arg);

const ShmemCallbacks LsnIndexerShmemCallbacks = {
	.request_fn = LSNIndexShmemRequest,
	.init_fn = LSNIndexShmemInit,
};

/* ----------------------------------------------------------------
 *  Shared-memory sizing and initialisation
 * ----------------------------------------------------------------
 */

// /*
//  * LSNIndexShmemSize - size for the fixed-size control struct only.
//  *
//  * The DSA/dshash that hold the per-page LSN lists are NOT in main shmem;
//  * they live in DSM segments allocated lazily by the startup process.
//  * See LSNIndexInit().
//  */
// Size
// LSNIndexShmemSize(void)
// {
// 	return MAXALIGN(sizeof(LSNIndexControl));
// }

static void
LSNIndexShmemRequest(void *arg)
{
	Size size;

	size = MAXALIGN(sizeof(LSNIndexControl));

	if(!enable_fast_recovery)
		return;
	ShmemRequestStruct(.name = "LSNIndex Ctl",
					.size = size,
					.ptr = (void **) &LSNIndexCtl,
		);
}

/*
 * LSNIndexShmemInit - postmaster-safe phase 1 of init.
 *
 * Allocates the small control struct in main shmem and initializes the
 * DSA/dshash handles to "invalid".  This must be safe to run in the
 * postmaster, so we deliberately do NOT call dsa_create here — dsm.c
 * forbids that with an assertion (no PROC entry, no error path, etc.).
 *
 * Children inherit the LSNIndexCtl pointer through fork.  Whichever
 * process needs the index first calls LSNIndexInit() to materialize the
 * DSA+dshash and publish their handles via LSNIndexCtl.
 *
 * Called from CreateOrAttachShmemStructs.
 */
void
LSNIndexShmemInit(void *arg)
{
	if (!enable_fast_recovery)
		return;

	LSNIndexCtl->dsa_handle = DSA_HANDLE_INVALID;
	LSNIndexCtl->hash_handle = DSHASH_HANDLE_INVALID;
	LSNIndexCtl->is_active = false;
	pg_atomic_init_u32(&LSNIndexCtl->nreplaying, 0);
}

/* ----------------------------------------------------------------
 *  Lazy create + attach
 * ----------------------------------------------------------------
 */

/*
 * LSNIndexInit - phase 2: create the DSA+dshash if not yet done, else attach.
 *
 * Must run in a backend context (the startup process is fine).  Called by
 * the startup process when it begins crash recovery.  Pins the DSA so it
 * survives past startup-process exit; subsequent attachers (the worker and
 * regular backends) just call LSNIndexAttach().
 *
 * Modeled on init_dsm_registry() in src/backend/storage/ipc/dsm_registry.c.
 */
void
LSNIndexInit(void)
{
	MemoryContext	oldcxt;

	/* Quick exit if already attached in this process. */
	if (lsn_hash != NULL)
		return;

	Assert(LSNIndexCtl != NULL);

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);

	LWLockAcquire(LSNIndexLock, LW_EXCLUSIVE);

	if (LSNIndexCtl->dsa_handle == DSA_HANDLE_INVALID)
	{
		/* First in — create the DSA + dshash and publish handles. */
		lsn_dsa = dsa_create(LWTRANCHE_LSN_INDEX_DSA);
		dsa_pin(lsn_dsa);
		dsa_pin_mapping(lsn_dsa);

		lsn_hash = dshash_create(lsn_dsa, &lsn_dsh_params, NULL);

		LSNIndexCtl->dsa_handle = dsa_get_handle(lsn_dsa);
		LSNIndexCtl->hash_handle = dshash_get_hash_table_handle(lsn_hash);
		LSNIndexCtl->is_active = true;
	}
	else
	{
		/* Already created — just attach. */
		lsn_dsa = dsa_attach(LSNIndexCtl->dsa_handle);
		dsa_pin_mapping(lsn_dsa);
		lsn_hash = dshash_attach(lsn_dsa, &lsn_dsh_params,
								 LSNIndexCtl->hash_handle, NULL);
	}

	LWLockRelease(LSNIndexLock);
	MemoryContextSwitchTo(oldcxt);
}

/*
 * LSNIndexAttach - attach to an already-created index.
 *
 * Used by backends and the fast-recovery worker.  Returns silently if the
 * index doesn't exist or has already been destroyed by the worker.
 */
void
LSNIndexAttach(void)
{
	MemoryContext	oldcxt;

	if (lsn_hash != NULL)
		return;					/* already attached */

	if (LSNIndexCtl == NULL || !LSNIndexCtl->is_active ||
		LSNIndexCtl->dsa_handle == DSA_HANDLE_INVALID)
		return;

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	LWLockAcquire(LSNIndexLock, LW_SHARED);

	/* Re-check under the lock — the worker may have torn it down. */
	if (LSNIndexCtl->is_active &&
		LSNIndexCtl->dsa_handle != DSA_HANDLE_INVALID)
	{
		lsn_dsa = dsa_attach(LSNIndexCtl->dsa_handle);
		dsa_pin_mapping(lsn_dsa);
		lsn_hash = dshash_attach(lsn_dsa, &lsn_dsh_params,
								 LSNIndexCtl->hash_handle, NULL);
	}

	LWLockRelease(LSNIndexLock);
	MemoryContextSwitchTo(oldcxt);
}

void
LSNIndexDetach(void)
{
	if (lsn_hash != NULL)
	{
		dshash_detach(lsn_hash);
		lsn_hash = NULL;
	}
	if (lsn_dsa != NULL)
	{
		dsa_detach(lsn_dsa);
		lsn_dsa = NULL;
	}
}

bool
LSNIndexIsActive(void)
{
	return (LSNIndexCtl != NULL && LSNIndexCtl->is_active);
}

/* ----------------------------------------------------------------
 *  Building the index (startup process, during WAL scan)
 * ----------------------------------------------------------------
 */

void
LSNIndexAddEntry(XLogReaderState *state)
{
	RelFileLocator	rlocator;
	ForkNumber		forknum;
	BlockNumber		blkno;
	BufferTag		tag;
	PageLSNEntry   *entry;
	bool			found;

	Assert(lsn_dsa != NULL && lsn_hash != NULL);

	for (int blk_id = 0; blk_id <= state->record->max_block_id; blk_id++)
	{
		LSNNode	   *node;
		dsa_pointer	node_dp;

		if (!XLogRecHasBlockRef(state, blk_id))
			continue;

		XLogRecGetBlockTag(state, blk_id, &rlocator, &forknum, &blkno);
		InitBufferTag(&tag, &rlocator, forknum, blkno);

		entry = (PageLSNEntry *) dshash_find_or_insert(lsn_hash, &tag, &found);

		if (!found)
		{
			entry->lsn_head = InvalidDsaPointer;
			entry->lsn_tail = InvalidDsaPointer;
			entry->max_lsn = InvalidXLogRecPtr;
		}

		/* Allocate a new LSNNode in DSA memory */
		node_dp = dsa_allocate(lsn_dsa, sizeof(LSNNode));
		node = (LSNNode *) dsa_get_address(lsn_dsa, node_dp);
		node->lsn = state->ReadRecPtr;
		node->next = InvalidDsaPointer;

		/* Append to tail of linked list */
		if (!DsaPointerIsValid(entry->lsn_tail))
		{
			entry->lsn_head = node_dp;
			entry->lsn_tail = node_dp;
		}
		else
		{
			LSNNode *tail = (LSNNode *) dsa_get_address(lsn_dsa, entry->lsn_tail);
			tail->next = node_dp;
			entry->lsn_tail = node_dp;
		}

		/* Track max LSN for quick skip checks */
		if (!found || state->ReadRecPtr > entry->max_lsn)
			entry->max_lsn = state->ReadRecPtr;

		dshash_release_lock(lsn_hash, entry);
	}
}

/*
 * LSNIndexForgetRelation - forget the pages of a relation fork that WAL
 * replay is dropping.
 *
 * Called from XLogDropRelation(), next to forgetting the fork's invalid
 * pages, when replay of a commit or abort record is about to unlink the
 * files of a dropped relation.  Their pending records must never be
 * replayed: the pages are dead, and reading them later would fail because
 * the files are gone.
 *
 * Only entries indexed so far are removed.  WAL is scanned in LSN order, so
 * these are exactly the records written before the drop; should the
 * relfilenumber be reused later in the WAL, the new relation's entries are
 * added after this and kept.
 *
 * This scans the whole index, which is fine for a handful of drops.  A
 * workload dropping many relations would want a per-relation list of the
 * blocks in the index.
 */
void
LSNIndexForgetRelation(RelFileLocator rlocator, ForkNumber forknum)
{
	dshash_seq_status seq;
	PageLSNEntry *entry;
	int			nforgotten = 0;

	/* Only the startup process, while it builds the index. */
	if (lsn_hash == NULL || !AmStartupProcess())
		return;

	dshash_seq_init(&seq, lsn_hash, true);
	while ((entry = (PageLSNEntry *) dshash_seq_next(&seq)) != NULL)
	{
		RelFileLocator entry_rlocator = BufTagGetRelFileLocator(&entry->tag);
		dsa_pointer node_dp;

		if (!RelFileLocatorEquals(entry_rlocator, rlocator) ||
			BufTagGetForkNum(&entry->tag) != forknum)
			continue;

		/* free the page's list of LSNs, then the entry itself */
		node_dp = entry->lsn_head;
		while (DsaPointerIsValid(node_dp))
		{
			dsa_pointer next_dp;

			next_dp = ((LSNNode *) dsa_get_address(lsn_dsa, node_dp))->next;
			dsa_free(lsn_dsa, node_dp);
			node_dp = next_dp;
		}
		dshash_delete_current(&seq);
		nforgotten++;
	}
	dshash_seq_term(&seq);

	if (nforgotten > 0)
		elog(DEBUG1, "fast recovery: forgot %d pages of dropped relation %u/%u/%u fork %d",
			 nforgotten, rlocator.spcOid, rlocator.dbOid, rlocator.relNumber,
			 forknum);
}

/* ----------------------------------------------------------------
 *  On-demand WAL replay for a single page
 * ----------------------------------------------------------------
 */

/*
 * Replay, in LSN order, the indexed records that touch the page 'tag'.
 */
static void
ReplayPageRecords(BufferTag *tag, dsa_pointer head_dp)
{
	dsa_pointer		node_dp;
	XLogReaderState *xlogreader;
	char		   *errormsg = NULL;

	RmgrStartup();

	inReplayPageWals = true;
	targetTag = *tag;

	xlogreader =
		XLogReaderAllocate(wal_segment_size, NULL,
						   XL_ROUTINE(.page_read = &read_local_xlog_page,
									  .segment_open = wal_segment_open,
									  .segment_close = wal_segment_close),
						   NULL);
	if (!xlogreader)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating a WAL reading processor.")));

	for (node_dp = head_dp;
		 DsaPointerIsValid(node_dp);
		 )
	{
		LSNNode	   *node = (LSNNode *) dsa_get_address(lsn_dsa, node_dp);
		XLogRecPtr	lsn = node->lsn;
		dsa_pointer	next_dp = node->next;

		XLogBeginRead(xlogreader, lsn);
		if (!XLogReadRecord(xlogreader, &errormsg))
		{
			if (errormsg)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read WAL at %X/%X: %s",
								LSN_FORMAT_ARGS(lsn), errormsg)));
			else
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read WAL at %X/%X",
								LSN_FORMAT_ARGS(lsn))));
		}

		/* Find the block reference matching our target tag and replay */
		for (int blk_id = 0; blk_id <= xlogreader->record->max_block_id; blk_id++)
		{
			RelFileLocator	rlocator;
			ForkNumber		forknum;
			BlockNumber		blkno;
			BufferTag		wal_tag;

			if (!XLogRecHasBlockRef(xlogreader, blk_id))
				continue;

			XLogRecGetBlockTag(xlogreader, blk_id, &rlocator, &forknum, &blkno);
			InitBufferTag(&wal_tag, &rlocator, forknum, blkno);

			if (BufferTagsEqual(tag, &wal_tag))
			{
				RmgrTable[XLogRecGetRmid(xlogreader)].rm_redo(xlogreader);
				break;
			}
		}

		node_dp = next_dp;
	}

	XLogReaderFree(xlogreader);
	RmgrCleanup();
}

/*
 * Register this process as replaying a page, unless fast recovery is over.
 *
 * From here until LSNIndexEndReplay(), FastRecoveryInProgress() counts us,
 * which keeps a checkpoint from starting while the page we are replaying has
 * not been dirtied yet.  We check is_active and increment the counter under
 * LSNIndexLock, so LSNIndexFinish(), which clears is_active under the same
 * lock in exclusive mode, sees every replay that got in.
 */
static bool
LSNIndexBeginReplay(void)
{
	bool		active;

	if (LSNIndexCtl == NULL)
		return false;

	LWLockAcquire(LSNIndexLock, LW_SHARED);
	active = LSNIndexCtl->is_active;
	if (active)
		pg_atomic_fetch_add_u32(&LSNIndexCtl->nreplaying, 1);
	LWLockRelease(LSNIndexLock);

	return active;
}

static void
LSNIndexEndReplay(void)
{
	inReplayPageWals = false;
	pg_atomic_fetch_sub_u32(&LSNIndexCtl->nreplaying, 1);
}

/*
 * LSNIndexReplayPage - replay the pending WAL records of one page.
 *
 * Called from the buffer read path.  If redo fails, the PG_FINALLY block
 * still unregisters us; a leaked count would block checkpoints until the
 * next restart.
 */
void
LSNIndexReplayPage(BufferTag *tag)
{
	if (!enable_fast_recovery || !LSNIndexBeginReplay())
		return;

	PG_TRY();
	{
		PageLSNEntry *entry = NULL;

		/*
		 * Attach lazily.  Our registration keeps the index from being freed
		 * under us.  If LSNIndexFinish() cleared is_active after we
		 * registered, LSNIndexAttach() does nothing and we skip the page:
		 * the worker has already replayed every page in the index.
		 */
		if (lsn_hash == NULL)
			LSNIndexAttach();
		if (lsn_hash != NULL)
			entry = (PageLSNEntry *) dshash_find(lsn_hash, tag, false);

		if (entry != NULL)
		{
			dsa_pointer head_dp = entry->lsn_head;

			dshash_release_lock(lsn_hash, entry);
			ReplayPageRecords(tag, head_dp);
		}
	}
	PG_FINALLY();
	{
		LSNIndexEndReplay();
	}
	PG_END_TRY();
}

/* ----------------------------------------------------------------
 *  Accessor for the dshash table (used by fast-recovery worker)
 * ----------------------------------------------------------------
 */

dshash_table *
lsn_hash_for_worker(void)
{
	Assert(lsn_hash != NULL);
	return lsn_hash;
}

/* ----------------------------------------------------------------
 *  End of fast recovery
 * ----------------------------------------------------------------
 */

/*
 * FastRecoveryInProgress - may pre-crash WAL still be replayed onto a page?
 *
 * True while the index is active, and afterwards until the last replay that
 * was already running has finished.  No checkpoint may start while this is
 * true.  A completed checkpoint promises that everything before its redo
 * point is on disk, and a crash after it starts recovery at that redo point.
 * A page that has not been replayed yet, or that a running replay dirties
 * after the checkpoint has collected its dirty buffers, would then never get
 * its pre-crash WAL replayed.
 */
bool
FastRecoveryInProgress(void)
{
	bool		result;

	if (LSNIndexCtl == NULL)
		return false;

	LWLockAcquire(LSNIndexLock, LW_SHARED);
	result = LSNIndexCtl->is_active ||
		pg_atomic_read_u32(&LSNIndexCtl->nreplaying) > 0;
	LWLockRelease(LSNIndexLock);

	return result;
}

/*
 * Free the DSA and dshash.  Only safe once nobody can reach them anymore.
 */
static void
LSNIndexDestroy(void)
{
	Assert(!LSNIndexCtl->is_active);
	Assert(pg_atomic_read_u32(&LSNIndexCtl->nreplaying) == 0);

	if (lsn_hash != NULL)
	{
		dshash_destroy(lsn_hash);
		lsn_hash = NULL;
	}
	if (lsn_dsa != NULL)
	{
		dsa_unpin(lsn_dsa);
		dsa_detach(lsn_dsa);
		lsn_dsa = NULL;
	}

	LWLockAcquire(LSNIndexLock, LW_EXCLUSIVE);
	LSNIndexCtl->dsa_handle = DSA_HANDLE_INVALID;
	LSNIndexCtl->hash_handle = DSHASH_HANDLE_INVALID;
	LWLockRelease(LSNIndexLock);
}

/*
 * LSNIndexFinish - end fast recovery.
 *
 * Called by the fast recovery worker once it has read every page in the
 * index.  Stops new on-demand replays, waits for the ones already running,
 * and only then frees the index.  FastRecoveryInProgress() turns false when
 * the wait is over, not before.
 */
void
LSNIndexFinish(void)
{
	LWLockAcquire(LSNIndexLock, LW_EXCLUSIVE);
	LSNIndexCtl->is_active = false;
	LWLockRelease(LSNIndexLock);

	while (pg_atomic_read_u32(&LSNIndexCtl->nreplaying) > 0)
	{
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 10L, WAIT_EVENT_FAST_RECOVERY_DRAIN);
		ResetLatch(MyLatch);
	}

	LSNIndexDestroy();
}
