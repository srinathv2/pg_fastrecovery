/*-------------------------------------------------------------------------
 *
 * lsn_indexer.h
 *    Header for LSN-based WAL indexing for fast recovery.
 *
 * This module builds a mapping of BufferTag to a list of WAL LSNs
 * during crash recovery and supports later use for on-demand WAL replay.
 *
 * Uses dshash (dynamic shared hash table) backed by dsa (dynamic shared
 * area) so the hash table grows dynamically — no upfront size estimate
 * needed.
 *
 *-------------------------------------------------------------------------
 */

#ifndef LSN_INDEXER_H
#define LSN_INDEXER_H

#include "access/xlogreader.h"
#include "storage/block.h"
#include "storage/buf_internals.h"
#include "lib/dshash.h"
#include "utils/dsa.h"

/*
 * LSNNode — one WAL record LSN for a page, stored in DSA memory.
 * Linked via dsa_pointer instead of raw pointers.
 */
typedef struct LSNNode
{
	XLogRecPtr		lsn;
	dsa_pointer		next;		/* dsa_pointer to next LSNNode, or InvalidDsaPointer */
} LSNNode;

/*
 * PageLSNEntry — dshash entry: BufferTag -> list of LSNs.
 * The key (BufferTag) must be at the start for dshash.
 */
typedef struct PageLSNEntry
{
	BufferTag		tag;
	dsa_pointer		lsn_head;	/* dsa_pointer to first LSNNode */
	dsa_pointer		lsn_tail;	/* dsa_pointer to last LSNNode */
	XLogRecPtr		max_lsn;
} PageLSNEntry;

/*
 * LSNIndexControl — small fixed-size struct in traditional shared memory.
 * Holds handles so any process can attach to the DSA and dshash.
 */
typedef struct LSNIndexControl
{
	dsa_handle			dsa_handle;
	dshash_table_handle	hash_handle;
	bool				is_active;
} LSNIndexControl;

/* GUC */
extern bool enable_fast_recovery;

/* Shared control structure (in traditional shmem) */
extern LSNIndexControl *LSNIndexCtl;

/* Global flag: true while replay_page_wals is executing */
extern bool inReplayPageWals;

/* The tag currently being replayed (used to filter redo operations) */
extern BufferTag targetTag;

/* Phase 1: postmaster-safe — allocates control struct in main shmem */
extern Size LSNIndexShmemSize(void);
extern void LSNIndexShmemInit(void *arg);

/* Phase 2: backend-context — creates the DSA+dshash on first call,
 * attaches on subsequent calls.  Called by the startup process. */
extern void LSNIndexInit(void);

/* Attach to an existing index — used by backends and the worker */
extern void LSNIndexAttach(void);
extern void LSNIndexDetach(void);

/* Check if the LSN index is active */
extern bool LSNIndexIsActive(void);

/* Add an entry during WAL scan (startup process) */
extern void LSNIndexAddEntry(XLogReaderState *state);

/* On-demand replay for a single page (called from ReadBuffer path) */
extern void LSNIndexReplayPage(BufferTag *tag);

/* Get the dshash table pointer (for sequential scans by the worker) */
extern dshash_table *lsn_hash_for_worker(void);

/* Destroy the index when recovery is complete */
extern void LSNIndexDestroy(void);

#endif							/* LSN_INDEXER_H */
