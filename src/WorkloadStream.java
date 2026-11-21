import java.io.BufferedOutputStream;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Homework 3.
 *
 * Task 1 (Yeabsira): replaces the Homework 2 write-only deadlock workload with a
 * mixed stream. Transaction i draws p ~ Uniform[0, 1) and becomes one of four
 * kinds. A fixed set of workers keeps the database busy by starting a new
 * transaction as soon as the previous one commits.
 *
 * Equilibrium (why arrival rate equals commit rate): there is no separate
 * generator and no queue. Worker w runs exactly one transaction at a time,
 * then takes the next one from the stream. A new transaction is admitted only
 * because another transaction just committed on that same worker. With W
 * workers the number of in-flight transactions stays at W, and over any
 * interval the number started equals the number committed (plus at most W
 * still in flight at the end). This is a closed system: by Little's law the
 * throughput is W / (mean transaction latency).
 *
 * The stream is fixed: transaction i is generated from (hw3.streamSeed, i)
 * only, so the serial baseline and every concurrent run execute the same
 * sequence of transactions. With -Dhw3.transactions=N exactly the first N
 * transactions are run and the makespan is measured; otherwise new
 * transactions are admitted for hw3.seconds.
 *
 * Starvation freedom: the only transactions that take conflicting row locks
 * are writes (reads and scans are MVCC snapshots, inserts use fresh keys).
 * A deadlock victim backs off randomly and retries the same statements in
 * the same order. After hw3.escalateAfter aborts it takes an exclusive, fair
 * gate that normal writes hold in shared mode, so it runs while no other
 * write transaction is active. It can no longer be part of a deadlock cycle
 * and therefore commits. Every transaction commits after a bounded number of
 * aborts.
 *
 * Task 3 optimizations (hw3.profile=optimized, the default; each one can be
 * switched individually for the ablation):
 *   hw3.sslmode=disable        no TLS on the localhost connection
 *   hw3.bufferedOutput=true    console lines go to a 64 KiB buffer instead of
 *                              a flush per line; the sums are still printed
 *   hw3.autocommitSingle=true  single-statement transactions (insert, scan)
 *                              run in autocommit: one round trip instead of
 *                              statement + COMMIT
 * hw3.profile=original restores the Task 1 behaviour (TLS as the driver
 * negotiates it, one flushed line per transaction, explicit COMMIT).
 *
 * Inserted tuples use data_id &gt;= num_tuple. Every fresh start of this
 * program drops blocking_data first, so those rows do not survive into the
 * next run.
 */
public class WorkloadStream {
	static final double READ_CUTOFF = 0.6;
	static final double INSERT_CUTOFF = 0.7;
	static final double SCAN_CUTOFF = 0.8;

	static final String profile = System.getProperty("hw3.profile", "optimized");
	static final boolean optimized = "optimized".equals(profile);

	static final String sslmode = System.getProperty("hw3.sslmode", optimized ? "disable" : "prefer");
	static final boolean bufferedOutput = Boolean.parseBoolean(
			System.getProperty("hw3.bufferedOutput", String.valueOf(optimized)));
	static final boolean autocommitSingle = Boolean.parseBoolean(
			System.getProperty("hw3.autocommitSingle", String.valueOf(optimized)));
	/** Durability trade-off, off by default in both profiles; only for the ablation. */
	static final boolean asyncCommit = Boolean.getBoolean("hw3.asyncCommit");
	/**
	 * Insert and write lines are not required by the sheet. Read and scan sums
	 * are always printed (required).
	 */
	static final boolean verbose = Boolean.parseBoolean(
			System.getProperty("hw3.verbose", String.valueOf(!optimized)));

	static final int num_tuple = Integer.getInteger("hw3.tuples", 50000);
	static final int num_queries_per_task = Integer.getInteger("hw3.queries", 3);
	static final String mode = System.getProperty("hw3.mode", "concurrent");
	static final int threads = "serial".equals(mode) ? 1 : Integer.getInteger("hw3.threads", 4);
	static final int seconds = Integer.getInteger("hw3.seconds", 30);
	static final long maxTransactions = Long.getLong("hw3.transactions", 0L);
	static final long streamSeed = Long.getLong("hw3.streamSeed", 20261124L);
	static final int escalateAfter = Integer.getInteger("hw3.escalateAfter", 3);
	static final double max_value = 50.0;

	static PrintStream out = System.out;

	final Random loadRandom = new Random(Long.getLong("hw3.seed", 123456L));
	final AtomicLong nextTxn = new AtomicLong();
	final AtomicBoolean accept = new AtomicBoolean(true);
	final AtomicInteger inFlight = new AtomicInteger(0);
	final AtomicInteger peakInFlight = new AtomicInteger(0);
	final CountDownLatch workersReady = new CountDownLatch(threads);
	final CountDownLatch startSignal = new CountDownLatch(1);
	final AtomicReference<Exception> workerFailure = new AtomicReference<>();
	/** Writes hold it shared; a transaction that aborted escalateAfter times holds it exclusively. */
	final ReentrantReadWriteLock writeGate = new ReentrantReadWriteLock(true);

	final AtomicLong readCommits = new AtomicLong();
	final AtomicLong insertCommits = new AtomicLong();
	final AtomicLong scanCommits = new AtomicLong();
	final AtomicLong writeCommits = new AtomicLong();
	final AtomicLong deadlockRetries = new AtomicLong();
	final AtomicLong escalations = new AtomicLong();
	final AtomicInteger maxAbortsOneTxn = new AtomicInteger();
	/** Wall time spent inside each kind (all attempts, until commit), for the bottleneck analysis. */
	final AtomicLong[] kindNanos = { new AtomicLong(), new AtomicLong(), new AtomicLong(), new AtomicLong() };

	enum Kind {
		READ_MULTI, INSERT, SCAN, WRITE
	}

	/**
	 * p in [0, 1). The point 1.0 has measure zero under a continuous uniform draw.
	 */
	static Kind kindOf(double p) {
		if (p < READ_CUTOFF) {
			return Kind.READ_MULTI;
		}
		if (p < INSERT_CUTOFF) {
			return Kind.INSERT;
		}
		if (p < SCAN_CUTOFF) {
			return Kind.SCAN;
		}
		return Kind.WRITE;
	}

	/** Transaction i of the stream. Depends only on (streamSeed, i). */
	static final class Txn {
		final long index;
		final Kind kind;
		final int[] ids;
		final double[] newValues;
		final int insertId;
		final double insertValue;

		Txn(long index) {
			this.index = index;
			SplittableRandom rng = new SplittableRandom(streamSeed + index * 0x9E3779B97F4A7C15L);
			this.kind = kindOf(rng.nextDouble());
			this.ids = (kind == Kind.READ_MULTI || kind == Kind.WRITE) ? distinctIds(rng, num_queries_per_task) : null;
			this.newValues = kind == Kind.WRITE ? newValues(rng, num_queries_per_task) : null;
			this.insertId = kind == Kind.INSERT ? Math.toIntExact(num_tuple + index) : -1;
			this.insertValue = kind == Kind.INSERT ? rng.nextDouble() * max_value : 0;
		}
	}

	static boolean isDeadlock(SQLException e) {
		for (SQLException cur = e; cur != null; cur = cur.getNextException()) {
			String state = cur.getSQLState();
			if ("40P01".equals(state) || "40001".equals(state)) {
				return true;
			}
			String msg = cur.getMessage();
			if (msg != null && msg.toLowerCase().contains("deadlock")) {
				return true;
			}
		}
		return false;
	}

	static Properties connectionProperties() {
		Properties p = new Properties();
		p.setProperty("sslmode", sslmode);
		return p;
	}

	void executeSQL(String sql, Connection con) throws SQLException {
		try (Statement stmt = con.createStatement()) {
			stmt.execute(sql);
		}
	}

	/**
	 * Drops the table before loading. Rows inserted by a previous run have
	 * data_id &gt;= num_tuple and disappear with the table.
	 */
	void createDB() throws SQLException {
		try (Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL,
				connectionProperties())) {
			int removedInserts = 0;
			boolean tableExisted = false;
			try (Statement stmt = con.createStatement();
					ResultSet rs = stmt.executeQuery(
							"SELECT COUNT(*) FROM blocking_data WHERE data_id >= " + num_tuple)) {
				tableExisted = true;
				if (rs.next()) {
					removedInserts = rs.getInt(1);
				}
			} catch (SQLException missing) {
				String state = missing.getSQLState();
				if (!"42P01".equals(state)) {
					throw missing;
				}
			}

			executeSQL("DROP TABLE IF EXISTS blocking_data;", con);
			executeSQL(
					"CREATE TABLE blocking_data ( data_id integer primary key, data_value float, modified_by integer);",
					con);
			con.setAutoCommit(false);
			try (PreparedStatement ps = con.prepareStatement(
					"INSERT INTO blocking_data (data_id, data_value, modified_by) VALUES (?, ?, ?)")) {
				for (int i = 0; i < num_tuple; i++) {
					ps.setInt(1, i);
					ps.setDouble(2, loadRandom.nextDouble() * max_value);
					ps.setInt(3, 0);
					ps.addBatch();
				}
				ps.executeBatch();
			}
			con.commit();
			con.setAutoCommit(true);
			// Fresh statistics and visibility map, so every run starts from the same table state.
			executeSQL("VACUUM ANALYZE blocking_data;", con);
			if (tableExisted) {
				out.println("Restart cleanup: removed " + removedInserts
						+ " inserted tuple(s) from the previous run (data_id >= " + num_tuple
						+ "), then reloaded " + num_tuple + " base rows.");
			} else {
				out.println("Restart cleanup: blocking_data did not exist yet. Loaded " + num_tuple
						+ " base rows. Later inserts use data_id >= " + num_tuple + " and are dropped on the next start.");
			}
		}
	}

	public static void main(String[] args) throws Exception {
		if (!"serial".equals(mode) && !"concurrent".equals(mode)) {
			throw new IllegalArgumentException("hw3.mode must be serial or concurrent");
		}
		if (!"optimized".equals(profile) && !"original".equals(profile)) {
			throw new IllegalArgumentException("hw3.profile must be optimized or original");
		}
		if (threads < 1) {
			throw new IllegalArgumentException("hw3.threads must be >= 1");
		}
		if (maxTransactions < 0) {
			throw new IllegalArgumentException("hw3.transactions must be >= 0 (0 = time window)");
		}
		if (maxTransactions == 0 && seconds < 1) {
			throw new IllegalArgumentException("hw3.seconds must be >= 1");
		}
		if (num_queries_per_task < 1) {
			throw new IllegalArgumentException("hw3.queries must be >= 1");
		}
		if (num_tuple < num_queries_per_task) {
			throw new IllegalArgumentException("hw3.tuples must be >= hw3.queries");
		}
		if (escalateAfter < 1) {
			throw new IllegalArgumentException("hw3.escalateAfter must be >= 1");
		}
		if (bufferedOutput) {
			out = new PrintStream(new BufferedOutputStream(new FileOutputStream(FileDescriptor.out), 1 << 16), false);
		}
		try {
			run();
		} finally {
			out.flush();
		}
	}

	static void run() throws Exception {
		WorkloadStream bench = new WorkloadStream();
		out.println("Homework 3 — mixed transaction stream");
		out.println("Execution: " + mode + ", workers=" + threads
				+ ", available processors=" + Runtime.getRuntime().availableProcessors());
		out.println("Profile: " + profile + " (sslmode=" + sslmode + ", bufferedOutput=" + bufferedOutput
				+ ", autocommitSingle=" + autocommitSingle + ", asyncCommit=" + asyncCommit
				+ ", verbose=" + verbose + ")");
		out.println("Mix: p<0.6 read " + num_queries_per_task
				+ " distinct tuples and print the sum; 0.6<=p<0.7 insert one tuple; "
				+ "0.7<=p<0.8 full-table sum; p>=0.8 " + num_queries_per_task + " single-tuple updates.");
		out.println("Stream: transaction i is generated from streamSeed=" + streamSeed
				+ " and i, so every run executes the same sequence.");
		out.println("Equilibrium: " + threads
				+ " workers, each starts its next transaction only after the previous one committed. "
				+ "In-flight stays at the worker count, so new transactions and commits stay matched. "
				+ "No extra generator thread and no queue.");
		if (maxTransactions > 0) {
			out.println("Run: exactly the first " + maxTransactions + " transactions of the stream, then stop.");
		} else {
			out.println("Run window: " + seconds + " s, then each worker finishes its current transaction and stops.");
		}

		bench.createDB();

		ExecutorService pool = Executors.newFixedThreadPool(threads);
		long started = 0;
		try {
			for (int id = 0; id < threads; id++) {
				pool.execute(bench.new Blocker(id));
			}
			pool.shutdown();
			bench.workersReady.await();
			if (bench.workerFailure.get() != null) {
				throw new IllegalStateException("Worker setup failed", bench.workerFailure.get());
			}
			started = System.nanoTime();
			long deadline = started + TimeUnit.SECONDS.toNanos(seconds);
			long heartbeat = TimeUnit.SECONDS.toNanos(5);
			bench.startSignal.countDown();
			while (true) {
				long wait = heartbeat;
				if (maxTransactions == 0 && bench.accept.get()) {
					long remaining = deadline - System.nanoTime();
					if (remaining <= 0) {
						bench.accept.set(false);
					} else {
						wait = Math.min(wait, remaining);
					}
				}
				if (pool.awaitTermination(wait, TimeUnit.NANOSECONDS)) {
					break;
				}
				if (bench.accept.get() && (maxTransactions > 0 || System.nanoTime() < deadline)) {
					bench.heartbeat();
				}
			}
		} finally {
			bench.accept.set(false);
			bench.startSignal.countDown();
			pool.shutdown();
			if (!pool.awaitTermination(1, TimeUnit.HOURS)) {
				pool.shutdownNow();
				throw new IllegalStateException("Workers did not finish their in-flight transactions");
			}
		}
		long wallNanos = System.nanoTime() - started;
		if (bench.workerFailure.get() != null) {
			throw new IllegalStateException("Benchmark failed", bench.workerFailure.get());
		}
		bench.report(wallNanos);
	}

	void report(long wallNanos) {
		long reads = readCommits.get();
		long inserts = insertCommits.get();
		long scans = scanCommits.get();
		long writes = writeCommits.get();
		long committed = reads + inserts + scans + writes;
		double wallSeconds = wallNanos / 1_000_000_000.0;
		double throughput = wallSeconds > 0 ? committed / wallSeconds : 0;
		if (maxTransactions > 0) {
			out.println("[DONE] committed " + committed + " of " + maxTransactions + " transactions in "
					+ String.format("%.3f", wallSeconds) + " s (makespan of the fixed stream).");
		} else {
			out.println("[DONE] committed " + committed + " transactions in " + String.format("%.3f", wallSeconds)
					+ " s (requested window " + seconds + " s, plus the in-flight tail).");
		}
		out.println("[DONE] throughput " + String.format("%.2f", throughput) + " txn/s"
				+ " | read=" + reads + " insert=" + inserts + " scan=" + scans + " write=" + writes
				+ " | deadlock retries=" + deadlockRetries.get()
				+ " escalations=" + escalations.get()
				+ " max aborts of one txn=" + maxAbortsOneTxn.get());
		if (committed > 0) {
			out.println("[DONE] observed mix"
					+ " read=" + String.format("%.1f", 100.0 * reads / committed) + "%"
					+ " insert=" + String.format("%.1f", 100.0 * inserts / committed) + "%"
					+ " scan=" + String.format("%.1f", 100.0 * scans / committed) + "%"
					+ " write=" + String.format("%.1f", 100.0 * writes / committed) + "%"
					+ " (target 60/10/10/20).");
		}
		long[] counts = { reads, inserts, scans, writes };
		long busy = 0;
		for (AtomicLong n : kindNanos) {
			busy += n.get();
		}
		StringBuilder lat = new StringBuilder("[DONE] latency per kind (mean ms, share of worker busy time):");
		for (Kind k : Kind.values()) {
			long n = kindNanos[k.ordinal()].get();
			long c = counts[k.ordinal()];
			lat.append(String.format(Locale.ROOT, " %s=%.3fms/%.1f%%", k.name().toLowerCase(Locale.ROOT),
					c == 0 ? 0.0 : n / 1e6 / c, busy == 0 ? 0.0 : 100.0 * n / busy));
		}
		out.println(lat);
		out.println("[DONE] equilibrium check: workers=" + threads
				+ ", peak in-flight=" + peakInFlight.get()
				+ ", in-flight after the tail=" + inFlight.get()
				+ ". Peak occupancy is the worker count: a commit is what frees a worker for the next transaction.");
		out.println("CSV_HEADER,mode,profile,threads,requested_seconds,transactions,elapsed_seconds,committed,throughput,"
				+ "reads,inserts,scans,writes,retries,escalations,peak_in_flight,in_flight,processors");
		out.printf(Locale.ROOT, "RESULT,%s,%s,%d,%d,%d,%.6f,%d,%.3f,%d,%d,%d,%d,%d,%d,%d,%d,%d%n",
				mode, profile, threads, maxTransactions > 0 ? 0 : seconds, maxTransactions, wallSeconds, committed,
				throughput, reads, inserts, scans, writes, deadlockRetries.get(), escalations.get(),
				peakInFlight.get(), inFlight.get(), Runtime.getRuntime().availableProcessors());
	}

	void heartbeat() {
		long committed = readCommits.get() + insertCommits.get() + scanCommits.get() + writeCommits.get();
		out.println("[stream] in-flight=" + inFlight.get() + "/" + threads
				+ " committed=" + committed
				+ " (read=" + readCommits.get()
				+ " insert=" + insertCommits.get()
				+ " scan=" + scanCommits.get()
				+ " write=" + writeCommits.get() + ")"
				+ " deadlock retries=" + deadlockRetries.get());
		out.flush();
	}

	void noteMax(AtomicInteger target, int current) {
		int prev;
		do {
			prev = target.get();
			if (current <= prev) {
				return;
			}
		} while (!target.compareAndSet(prev, current));
	}

	/**
	 * One worker. Each loop is one transaction: take the next one from the
	 * stream, run it until commit, then repeat while the benchmark is still
	 * accepting work.
	 */
	final class Blocker implements Runnable {
		final int workerId;

		Blocker(int workerId) {
			this.workerId = workerId;
		}

		@Override
		public void run() {
			boolean readyReported = false;
			try (Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL,
					connectionProperties())) {
				if (asyncCommit) {
					executeSQL("SET synchronous_commit TO off", con);
				}
				con.setAutoCommit(false);
				con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
				try (PreparedStatement readPs = con.prepareStatement(
						"SELECT data_value FROM blocking_data WHERE data_id = ?");
						PreparedStatement insertPs = con.prepareStatement(
								"INSERT INTO blocking_data (data_id, data_value, modified_by) VALUES (?, ?, ?)");
						PreparedStatement scanPs = con.prepareStatement(
								"SELECT sum(data_value) FROM blocking_data");
						PreparedStatement writePs = con.prepareStatement(
								"UPDATE blocking_data SET data_value = ?, modified_by = ? WHERE data_id = ?")) {
					workersReady.countDown();
					readyReported = true;
					startSignal.await();
					while (accept.get()) {
						long index = nextTxn.getAndIncrement();
						if (maxTransactions > 0 && index >= maxTransactions) {
							break;
						}
						runOne(con, new Txn(index), readPs, insertPs, scanPs, writePs);
					}
				}
			} catch (SQLException | RuntimeException | InterruptedException e) {
				if (e instanceof InterruptedException) {
					Thread.currentThread().interrupt();
				}
				workerFailure.compareAndSet(null, e);
				accept.set(false);
				Log.error("Worker " + workerId + " stopped: " + e.getMessage());
				Log.error(e);
			} finally {
				if (!readyReported) {
					workersReady.countDown();
				}
			}
		}

		/**
		 * Runs one transaction until it commits. A deadlock victim (40P01 /
		 * 40001) rolls back, sleeps a random time and repeats the same
		 * statements in the same order. From the escalateAfter-th retry on, a
		 * write runs with the gate held exclusively, so no other write is
		 * active and no deadlock cycle can include it.
		 */
		void runOne(Connection con, Txn t, PreparedStatement readPs, PreparedStatement insertPs,
				PreparedStatement scanPs, PreparedStatement writePs) throws SQLException, InterruptedException {
			noteMax(peakInFlight, inFlight.incrementAndGet());
			long t0 = System.nanoTime();
			int aborts = 0;
			try {
				while (true) {
					Lock gate = null;
					if (t.kind == Kind.WRITE) {
						if (aborts >= escalateAfter) {
							gate = writeGate.writeLock();
							escalations.incrementAndGet();
						} else {
							gate = writeGate.readLock();
						}
						gate.lock();
					}
					try {
						switch (t.kind) {
						case READ_MULTI:
							commitRead(con, readPs, t.ids);
							readCommits.incrementAndGet();
							break;
						case INSERT:
							commitInsert(con, insertPs, t.insertId, t.insertValue);
							insertCommits.incrementAndGet();
							break;
						case SCAN:
							commitScan(con, scanPs);
							scanCommits.incrementAndGet();
							break;
						case WRITE:
							commitWrite(con, writePs, t.ids, t.newValues);
							writeCommits.incrementAndGet();
							break;
						default:
							throw new IllegalStateException(String.valueOf(t.kind));
						}
						return;
					} catch (SQLException e) {
						con.rollback();
						if (!isDeadlock(e)) {
							throw e;
						}
						aborts++;
						deadlockRetries.incrementAndGet();
						noteMax(maxAbortsOneTxn, aborts);
					} finally {
						if (gate != null) {
							gate.unlock();
						}
					}
					// Randomized backoff outside the gate, so the losers do not wake up together.
					Thread.sleep(java.util.concurrent.ThreadLocalRandom.current().nextInt(1, 1 + (5 << Math.min(aborts, 3))));
				}
			} finally {
				kindNanos[t.kind.ordinal()].addAndGet(System.nanoTime() - t0);
				inFlight.decrementAndGet();
			}
		}

		void commitRead(Connection con, PreparedStatement readPs, int[] ids) throws SQLException {
			double sum = 0;
			for (int id : ids) {
				readPs.setInt(1, id);
				try (ResultSet rs = readPs.executeQuery()) {
					if (rs.next()) {
						sum += rs.getDouble(1);
					}
				}
			}
			con.commit();
			out.println("read worker " + workerId + " ids=" + formatIds(ids) + " sum=" + sum);
		}

		void commitInsert(Connection con, PreparedStatement insertPs, int insertId, double insertValue)
				throws SQLException {
			insertPs.setInt(1, insertId);
			insertPs.setDouble(2, insertValue);
			insertPs.setInt(3, workerId);
			if (autocommitSingle) {
				con.setAutoCommit(true);
				try {
					insertPs.executeUpdate();
				} finally {
					con.setAutoCommit(false);
				}
			} else {
				insertPs.executeUpdate();
				con.commit();
			}
			if (verbose) {
				out.println("insert worker " + workerId + " data_id=" + insertId + " data_value=" + insertValue);
			}
		}

		void commitScan(Connection con, PreparedStatement scanPs) throws SQLException {
			double sum = 0;
			if (autocommitSingle) {
				con.setAutoCommit(true);
			}
			try (ResultSet rs = scanPs.executeQuery()) {
				if (rs.next()) {
					sum = rs.getDouble(1);
				}
			} finally {
				if (autocommitSingle) {
					con.setAutoCommit(false);
				}
			}
			if (!autocommitSingle) {
				con.commit();
			}
			out.println("scan worker " + workerId + " sum=" + sum);
		}

		/**
		 * Updates stay in the drawn id order. Sorting them would change the
		 * lock order and is left alone on purpose.
		 */
		void commitWrite(Connection con, PreparedStatement writePs, int[] ids, double[] newValues)
				throws SQLException {
			for (int q = 0; q < ids.length; q++) {
				writePs.setDouble(1, newValues[q]);
				writePs.setInt(2, workerId);
				writePs.setInt(3, ids[q]);
				int updated = writePs.executeUpdate();
				if (updated != 1) {
					throw new SQLException("write touched " + updated + " rows for data_id=" + ids[q]);
				}
			}
			con.commit();
			if (verbose) {
				out.println("write worker " + workerId + " ids=" + formatIds(ids));
			}
		}
	}

	static int[] distinctIds(SplittableRandom rng, int count) {
		int[] ids = new int[count];
		int filled = 0;
		while (filled < count) {
			int id = rng.nextInt(num_tuple);
			boolean seen = false;
			for (int i = 0; i < filled; i++) {
				if (ids[i] == id) {
					seen = true;
					break;
				}
			}
			if (!seen) {
				ids[filled++] = id;
			}
		}
		return ids;
	}

	static double[] newValues(SplittableRandom rng, int count) {
		double[] values = new double[count];
		for (int i = 0; i < count; i++) {
			values[i] = rng.nextDouble() * max_value;
		}
		return values;
	}

	static String formatIds(int[] ids) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < ids.length; i++) {
			if (i > 0) {
				sb.append(',');
			}
			sb.append(ids[i]);
		}
		return sb.toString();
	}
}
