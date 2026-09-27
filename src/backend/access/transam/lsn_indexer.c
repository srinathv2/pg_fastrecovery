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
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/dsa.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
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
	bool	found;

	if(!enable_fast_recovery)
		return;

	LSNIndexCtl->dsa_handle = DSA_HANDLE_INVALID;
	LSNIndexCtl->hash_handle = DSHASH_HANDLE_INVALID;
	LSNIndexCtl->is_active = false;
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

/* ----------------------------------------------------------------
 *  On-demand WAL replay for a single page
 * ----------------------------------------------------------------
 */

void
LSNIndexReplayPage(BufferTag *tag)
{
	PageLSNEntry   *entry;
	dsa_pointer		node_dp;
	dsa_pointer		head_dp;
	XLogReaderState *xlogreader;
	char		   *errormsg = NULL;

	if (!enable_fast_recovery || !LSNIndexIsActive())
		return;

	/* Attach lazily if not yet done */
	if (lsn_hash == NULL)
		LSNIndexAttach();
	if (lsn_hash == NULL)
		return;

	/* Look up this page in the hash table (shared lock) */
	entry = (PageLSNEntry *) dshash_find(lsn_hash, tag, false);
	if (entry == NULL)
		return;					/* no pending WALs for this page */

	/* Copy what we need and release the dshash lock */
	head_dp = entry->lsn_head;
	dshash_release_lock(lsn_hash, entry);

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

	inReplayPageWals = false;
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
 *  Destroy (called by fast-recovery worker on completion)
 * ----------------------------------------------------------------
 */

void
LSNIndexDestroy(void)
{
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
	if (LSNIndexCtl != NULL)
		LSNIndexCtl->is_active = false;
}
