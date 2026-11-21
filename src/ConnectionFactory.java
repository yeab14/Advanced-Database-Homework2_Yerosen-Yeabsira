import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Builds JDBC connections for the Homework 2 demos.
 *
 * Values come from the process environment, then from a .env file in the
 * working directory, then from the defaults below. Copy .env.example to .env.
 */
public final class ConnectionFactory {

	public static final String POSTGRESQL = "postgresql";

	private static final Map<String, String> DOTENV = loadDotEnv();

	private ConnectionFactory() {
	}

	public static Connection getDefaultParameterConnection(String vendor) {
		return getDefaultParameterConnection(vendor, null);
	}

	/**
	 * Same as above, plus extra pgJDBC properties (for example sslmode). Homework 2
	 * calls the one-argument form and is unchanged.
	 */
	public static Connection getDefaultParameterConnection(String vendor, Properties extra) {
		if (!POSTGRESQL.equals(vendor)) {
			throw new IllegalArgumentException("unsupported vendor: " + vendor);
		}
		String host = envOr("HW2_DB_HOST", "127.0.0.1");
		String port = envOr("HW2_DB_PORT", "5432");
		String db = envOr("HW2_DB_NAME", "adb_hw2");
		String url = envOr("HW2_JDBC_URL", "jdbc:postgresql://" + host + ":" + port + "/" + db);
		Properties props = new Properties();
		props.setProperty("user", envOr("HW2_DB_USER", "hw2"));
		props.setProperty("password", envOr("HW2_DB_PASSWORD", "hw2"));
		if (extra != null) {
			props.putAll(extra);
		}
		try {
			return DriverManager.getConnection(url, props);
		} catch (SQLException e) {
			throw new RuntimeException("cannot connect to " + url, e);
		}
	}

	private static String envOr(String key, String fallback) {
		String v = System.getenv(key);
		if (v == null || v.isEmpty()) {
			v = DOTENV.get(key);
		}
		if (v == null || v.isEmpty()) {
			return fallback;
		}
		return stripEnvValue(v);
	}

	/** CRLF .env or shell export leaves \\r in values and breaks the JDBC URL. */
	private static String stripEnvValue(String value) {
		if (value.indexOf('\r') < 0) {
			return value;
		}
		return value.replace("\r", "").trim();
	}

	/**
	 * Reads KEY=VALUE lines. Ignores blanks and comments. Does not override real
	 * env vars.
	 */
	private static Map<String, String> loadDotEnv() {
		Map<String, String> map = new HashMap<String, String>();
		Path path = Path.of(System.getProperty("user.dir", "."), ".env");
		if (!Files.isRegularFile(path)) {
			return map;
		}
		try {
			for (String raw : Files.readAllLines(path, StandardCharsets.UTF_8)) {
				String line = raw.trim();
				if (line.isEmpty() || line.startsWith("#")) {
					continue;
				}
				if (line.startsWith("export ")) {
					line = line.substring("export ".length()).trim();
				}
				int eq = line.indexOf('=');
				if (eq <= 0) {
					continue;
				}
				String key = line.substring(0, eq).trim();
				String value = line.substring(eq + 1).trim();
				if (value.length() >= 2
						&& ((value.startsWith("\"") && value.endsWith("\""))
								|| (value.startsWith("'") && value.endsWith("'")))) {
					value = value.substring(1, value.length() - 1);
				}
				map.put(key, stripEnvValue(value));
			}
		} catch (IOException e) {
			Log.error("Could not read " + path + ": " + e.getMessage());
		}
		return map;
	}
}