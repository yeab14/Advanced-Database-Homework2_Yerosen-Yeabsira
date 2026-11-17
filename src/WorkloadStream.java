import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Random;

/**
 * Homework 3, task 1 (Yeabsira Dereje Alemayhu).
 *
 * Replaces the Homework 2 write-only deadlock workload with a mixed stream:
 * each transaction draws p ~ Uniform[0, 1) and becomes one of four kinds.
 * A fixed set of workers keeps the database busy by starting a new
 * transaction as soon as the previous one commits.
 *
 * Equilibrium (why arrival rate equals commit rate):
 * there is no separate generator and no queue. Worker i runs exactly one
 * transaction at a time, then dices the next one. A new transaction is
 * admitted only because another transaction just committed on that same
 * worker. With W workers the number of in-flight transactions therefore
 * stays at W, and over any interval the number of transactions started
 * equals the number committed (plus at most W still in flight at the end).
 * That balance is structural. It does not depend on tuning a sleep or a
 * target rate. The measured commit rate is the throughput of this mix.
 *
 * Inserted tuples use data_id &gt;= num_tuple. Every fresh start of this
 * program drops blocking_data first, so those rows do not survive into the
 * next run.
 */
public class WorkloadStream {
	static final double READ_CUTOFF = 0.6;
	static final double INSERT_CUTOFF = 0.7;
	static final double SCAN_CUTOFF = 0.8;

	static final boolean verbose = Boolean.getBoolean("hw3.verbose");

	static final boolean printScans = Boolean.parseBoolean(
    System.getProperty("hw3.printScans", "true"));

	static final int num_tuple = Integer.getInteger("hw3.tuples", 50000);
	static final int num_queries_per_task = Integer.getInteger("hw3.queries", 3);
	static final String mode = System.getProperty("hw3.mode", "concurrent");
	static final int threads = "serial".equals(mode) ? 1 : Integer.getInteger("hw3.threads", 4);
	static final int seconds = Integer.getInteger("hw3.seconds", 30);
	static final double max_value = 50.0;

	final Random loadRandom = new Random(Long.getLong("hw3.seed", 123456L));
	final AtomicInteger nextInsertId = new AtomicInteger(num_tuple);
	final AtomicBoolean accept = new AtomicBoolean(true);
	final AtomicInteger inFlight = new AtomicInteger(0);
	final AtomicInteger peakInFlight = new AtomicInteger(0);
	final CountDownLatch workersReady = new CountDownLatch(threads);
	final CountDownLatch startSignal = new CountDownLatch(1);
	final AtomicReference<Exception> workerFailure = new AtomicReference<>();

	final AtomicLong readCommits = new AtomicLong();
	final AtomicLong insertCommits = new AtomicLong();
	final AtomicLong scanCommits = new AtomicLong();
	final AtomicLong writeCommits = new AtomicLong();
	final AtomicLong deadlockRetries = new AtomicLong();

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
		try (Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL)) {
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
			if (tableExisted) {
				System.out.println("Restart cleanup: removed " + removedInserts
						+ " inserted tuple(s) from the previous run (data_id >= " + num_tuple
						+ "), then reloaded " + num_tuple + " base rows.");
			} else {
				System.out.println("Restart cleanup: blocking_data did not exist yet. Loaded " + num_tuple
						+ " base rows. Later inserts use data_id >= " + num_tuple + " and are dropped on the next start.");
			}
		}
	}

	public static void main(String[] args) throws Exception {
		if (!"serial".equals(mode) && !"concurrent".equals(mode)) {
			throw new IllegalArgumentException("hw3.mode must be serial or concurrent");
		}
		if (threads < 1) {
			throw new IllegalArgumentException("hw3.threads must be >= 1");
		}
		if (seconds < 1) {
			throw new IllegalArgumentException("hw3.seconds must be >= 1");
		}
		if (num_queries_per_task < 1) {
			throw new IllegalArgumentException("hw3.queries must be >= 1");
		}
		if (num_tuple < num_queries_per_task) {
			throw new IllegalArgumentException("hw3.tuples must be >= hw3.queries");
		}

		WorkloadStream bench = new WorkloadStream();
		System.out.println("Homework 3 — mixed transaction stream");
		System.out.println("Execution: " + mode + ", workers=" + threads
				+ ", available processors=" + Runtime.getRuntime().availableProcessors());
		System.out.println("Mix: p<0.6 read " + num_queries_per_task
				+ " distinct tuples and print the sum; 0.6<=p<0.7 insert one tuple; "
				+ "0.7<=p<0.8 full-table sum; p>=0.8 " + num_queries_per_task + " single-tuple updates.");
		System.out.println("Equilibrium: " + threads
				+ " workers, each restarts a finished transaction on the same thread. "
				+ "In-flight stays at the worker count, so new transactions and commits stay matched. "
				+ "No extra generator thread and no queue.");
		System.out.println("Run window: " + seconds + " s, then each worker finishes its current transaction and stops.");

		bench.createDB();

		ExecutorService pool = Executors.newFixedThreadPool(threads);
		long started;
		try {
			for (int id = 0; id < threads; id++) {
				pool.execute(bench.new Blocker(id));
			}
			bench.workersReady.await();
			if (bench.workerFailure.get() != null) {
				throw new IllegalStateException("Worker setup failed", bench.workerFailure.get());
			}
			started = System.nanoTime();
			long deadline = started + TimeUnit.SECONDS.toNanos(seconds);
			bench.startSignal.countDown();
			long remaining;
			while (bench.accept.get() && (remaining = deadline - System.nanoTime()) > 0) {
				TimeUnit.NANOSECONDS.sleep(Math.min(TimeUnit.SECONDS.toNanos(5), remaining));
				if (bench.accept.get() && System.nanoTime() < deadline) {
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
		System.out.println("[DONE] committed " + committed + " transactions in " + String.format("%.3f", wallSeconds)
				+ " s (requested window " + seconds + " s, plus the in-flight tail).");
		System.out.println("[DONE] throughput " + String.format("%.2f", throughput) + " txn/s"
				+ " | read=" + reads + " insert=" + inserts + " scan=" + scans + " write=" + writes
				+ " | deadlock retries=" + deadlockRetries.get());
		if (committed > 0) {
			System.out.println("[DONE] observed mix"
					+ " read=" + String.format("%.1f", 100.0 * reads / committed) + "%"
					+ " insert=" + String.format("%.1f", 100.0 * inserts / committed) + "%"
					+ " scan=" + String.format("%.1f", 100.0 * scans / committed) + "%"
					+ " write=" + String.format("%.1f", 100.0 * writes / committed) + "%"
					+ " (target 60/10/10/20).");
		}
		System.out.println("[DONE] equilibrium check: workers=" + threads
				+ ", peak in-flight=" + peakInFlight.get()
				+ ", in-flight after the tail=" + inFlight.get()
				+ ". Peak occupancy is the worker count: a commit is what frees a worker for the next transaction.");
		System.out.println("CSV_HEADER,mode,threads,requested_seconds,elapsed_seconds,committed,throughput,reads,inserts,scans,writes,retries,peak_in_flight,in_flight,processors");
		System.out.printf(Locale.ROOT, "RESULT,%s,%d,%d,%.6f,%d,%.3f,%d,%d,%d,%d,%d,%d,%d,%d%n",
				mode, threads, seconds, wallSeconds, committed, throughput, reads, inserts, scans, writes,
				deadlockRetries.get(), peakInFlight.get(), inFlight.get(), Runtime.getRuntime().availableProcessors());
	}

	void heartbeat() {
		long committed = readCommits.get() + insertCommits.get() + scanCommits.get() + writeCommits.get();
		System.out.println("[stream] in-flight=" + inFlight.get() + "/" + threads
				+ " committed=" + committed
				+ " (read=" + readCommits.get()
				+ " insert=" + insertCommits.get()
				+ " scan=" + scanCommits.get()
				+ " write=" + writeCommits.get() + ")");
	}

	void noteInFlight(int current) {
		int prev;
		do {
			prev = peakInFlight.get();
			if (current <= prev) {
				return;
			}
		} while (!peakInFlight.compareAndSet(prev, current));
	}

	/**
	 * One worker. Each loop is one transaction: dice p, run it until commit,
	 * then repeat while the benchmark is still accepting work.
	 * Deadlock victims (40P01 / 40001) roll back and repeat the same statements
	 * in the same order, so the transaction that was diced is the one that commits.
	 */
	final class Blocker implements Runnable {
		final int workerId;

		Blocker(int workerId) {
			this.workerId = workerId;
		}

		@Override
		public void run() {
			boolean readyReported = false;
			try (Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL)) {
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
						runOne(con, readPs, insertPs, scanPs, writePs);
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

		void runOne(Connection con, PreparedStatement readPs, PreparedStatement insertPs,
				PreparedStatement scanPs, PreparedStatement writePs) throws SQLException {
			ThreadLocalRandom rng = ThreadLocalRandom.current();
			double p = rng.nextDouble();
			Kind kind = kindOf(p);
			int[] ids = (kind == Kind.READ_MULTI || kind == Kind.WRITE)
					? distinctIds(rng, num_queries_per_task)
					: null;
			double[] newValues = kind == Kind.WRITE ? newValues(rng, num_queries_per_task) : null;
			int insertId = -1;
			double insertValue = 0;
			if (kind == Kind.INSERT) {
				insertId = nextInsertId.getAndIncrement();
				insertValue = rng.nextDouble() * max_value;
			}

			noteInFlight(inFlight.incrementAndGet());
			try {
				boolean committed = false;
				while (!committed) {
					try {
						switch (kind) {
						case READ_MULTI:
							commitRead(con, readPs, ids);
							readCommits.incrementAndGet();
							break;
						case INSERT:
							commitInsert(con, insertPs, insertId, insertValue);
							insertCommits.incrementAndGet();
							break;
						case SCAN:
							commitScan(con, scanPs);
							scanCommits.incrementAndGet();
							break;
						case WRITE:
							commitWrite(con, writePs, ids, newValues);
							writeCommits.incrementAndGet();
							break;
						default:
							throw new IllegalStateException(String.valueOf(kind));
						}
						committed = true;
					} catch (SQLException e) {
						con.rollback();
						if (isDeadlock(e)) {
							deadlockRetries.incrementAndGet();
							continue;
						}
						throw e;
					}
				}
			} finally {
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
			if (verbose) {
                System.out.println("read worker " + workerId + " ids=" + formatIds(ids) + " sum=" + sum);
            }
		}

		void commitInsert(Connection con, PreparedStatement insertPs, int insertId, double insertValue)
				throws SQLException {
			insertPs.setInt(1, insertId);
			insertPs.setDouble(2, insertValue);
			insertPs.setInt(3, workerId);
			insertPs.executeUpdate();
			con.commit();
			if (verbose) {
                System.out.println("insert worker " + workerId + " data_id=" + insertId + " data_value=" + insertValue);
            }
		}

		void commitScan(Connection con, PreparedStatement scanPs) throws SQLException {
			double sum;
			try (ResultSet rs = scanPs.executeQuery()) {
				if (!rs.next()) {
					sum = 0;
				} else {
					sum = rs.getDouble(1);
				}
			}
			con.commit();
			if (printScans) {
               System.out.println("scan worker " + workerId + " sum=" + sum);
            }
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
               System.out.println("write worker " + workerId + " ids=" + formatIds(ids));
            }
		}
	}

	static int[] distinctIds(ThreadLocalRandom rng, int count) {
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

	static double[] newValues(ThreadLocalRandom rng, int count) {
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
