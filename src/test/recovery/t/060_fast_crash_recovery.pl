# Copyright (c) 2026, PostgreSQL Global Development Group

# Crash recovery with on-demand WAL replay (fast_crash_recovery): the server
# accepts connections once the WAL has been scanned, each page is recovered
# when it is first read, and a background worker recovers the rest.  The
# data must be right at every stage: right after the server opens, after the
# worker has drained the index, and after a clean restart.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->init;
$node->append_conf(
	'postgresql.conf', qq(
fast_crash_recovery = on
checkpoint_timeout = 1d
max_wal_size = 10GB
log_checkpoints = on
));
$node->start;

# A table with an index, checkpointed, then modified so that the changes
# exist only in WAL when we crash: updates, deletes, inserts, and a table
# that is created and dropped inside the window.
$node->safe_psql(
	'postgres', q(
CREATE TABLE t (id int PRIMARY KEY, v int NOT NULL, pad text);
INSERT INTO t SELECT g, 0, repeat('x', 200) FROM generate_series(1, 20000) g;
CHECKPOINT;
UPDATE t SET v = v + 1 WHERE id % 3 = 0;
DELETE FROM t WHERE id % 10 = 0;
INSERT INTO t SELECT g, -1, '' FROM generate_series(20001, 25000) g;
CREATE TABLE dropped AS SELECT g FROM generate_series(1, 5000) g;
DROP TABLE dropped;
VACUUM t;
));

my $seqscan = 'SELECT count(*), sum(v), sum(length(pad)) FROM t';
my $indexscan =
  'SET enable_seqscan = off; SET enable_bitmapscan = off; '
  . 'SELECT count(*), sum(v) FROM t WHERE id > 0';
my $expected_seq = $node->safe_psql('postgres', $seqscan);
my $expected_idx = $node->safe_psql('postgres', $indexscan);

# Crash.
$node->stop('immediate');
my $log_offset = -s $node->logfile;
$node->start;

ok( $node->log_contains('Fast recovery worker started', $log_offset),
	'crash recovery used on-demand WAL replay');

# 1. Right after the server opened: pages are replayed as they are read, by
# a sequential scan and by an index scan.
is($node->safe_psql('postgres', $seqscan),
	$expected_seq, 'data correct right after the server opened (seq scan)');
is($node->safe_psql('postgres', $indexscan),
	$expected_idx, 'data correct right after the server opened (index scan)');

# 2. After the worker has recovered every remaining page.
$node->wait_for_log(qr/Fast recovery complete/, $log_offset);
is($node->safe_psql('postgres', $seqscan),
	$expected_seq, 'data correct after the worker drained the index');

# Checkpoints are allowed again once the index is gone.
$node->safe_psql('postgres', 'CHECKPOINT');
ok( $node->log_contains('checkpoint complete', $log_offset),
	'a checkpoint ran after the index was drained');

# 3. After a clean restart, which must not need recovery at all.
$log_offset = -s $node->logfile;
$node->restart;
ok( !$node->log_contains('automatic recovery in progress', $log_offset),
	'clean restart needed no recovery');
is($node->safe_psql('postgres', $seqscan),
	$expected_seq, 'data correct after a clean restart');

# The dropped table's pages were forgotten, not resurrected.
is( $node->safe_psql(
		'postgres', "SELECT count(*) FROM pg_class WHERE relname = 'dropped'"),
	'0',
	'table dropped inside the window stays dropped');

$node->stop;
done_testing();
