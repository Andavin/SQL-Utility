package me.andavin.sql;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.bukkit.Bukkit;
import org.bukkit.plugin.Plugin;
import org.bukkit.scheduler.BukkitRunnable;

public class SQLUtil {

	private static Connection connect = null;
	
	private static Plugin plugin;
	private static DatabaseType type = DatabaseType.SQLite;
	private static String sqlUser = null;
	private static String sqlPass = null;
	
	/** 
	 * Get the SQL connection using the credentials already
	 * defined in this class.
	 * 
	 * @return An SQL connection to the SQL database.
	 * @throws SQLException 
	 */
	private synchronized static Connection getConnection() throws SQLException {
		
		if(plugin == null) {
			throw new SQLException("You need an instance of the plugin to use SQLUtil!");
		}
			
		if(connect != null && (connect.isClosed() || !connect.isValid(1))) {
			try {
				connect.close();
			} catch (SQLException e) {}
			connect = null;
		}
		
		if(connect == null) {
			connect = DriverManager.getConnection(type.address(), sqlUser, sqlPass);
		}
		
		if(connect == null) {
			throw new SQLException("Connection was null!");
		}
		
		return connect;
	}
	
	/**
	 * Set the instance of your plugin main class for use 
	 * in the SQLUtil.
	 * 
	 * @param plugin The plugin class instance.
	 */
	public static void addPlugin(Plugin plugin) {
		SQLUtil.plugin = plugin;
	}
	
	/**
	 * Set the type of database to either SQLite or MySQL.
	 * Note that the default setting is SQLite.
	 * 
	 * @param type The type of database to set to.
	 */
	public static void setType(DatabaseType type) {
		SQLUtil.type = type;
	}
	
	/**
	 * Set any of the attributes required to connect
	 * to an SQL database. Options for the name are:
	 * <ul>
	 * <li><b>port</b> - The port to the SQL server <i>default 3306</i>.
	 * <li><b>address</b> - The address to the SQL server <i>default localhost</i>.
	 * <li><b>name</b> - The name of the SQL database <i>required</i>.
	 * <li><b>username</b> - The username to access the SQL database <i>required for MySQL</i>.
	 * <li><b>password</b> - The password to access the SQL database <i>required for MySQL</i>.
	 * </ul>
	 * Note that with SQLite only the database name is required
	 * as it is the name of the file. MySQL can be used by changing
	 * the setting using {@link SQLUtil#setType(DatabaseType)};
	 * <p>
	 * 
	 * @param name The name of the attribute listed above.
	 * @param value The value to input into the attribute.
	 */
	public static void setAttribute(String name, String value) {
		
		switch(name) {
		case "port": type.port = value;
		break;
		case "address": type.address = value;
		break;
		case "name": type.name = value;
		break;
		case "username": sqlUser = value;
		break;
		case "password": sqlPass = value;
		}
	}
	
	/**
	 * Create a new SQL Statement object for use in executing
	 * non-bind variable SQL statements such as create, alter,
	 * grant, etc.
	 * <br>
	 * This is the same effect as calling {@link Connection#createStatement()}
	 * since that is what this method is doing.
	 * 
	 * @return A newly created Statement
	 */
	public static synchronized Statement createStatement() {
		
		try {
			return SQLUtil.getConnection().createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * Prepare an SQL statement for the database.
	 * 
	 * @param statement The string SQL syntax.
	 * @param args The arguments to insert into the given statement.
	 * Replacing '?' with the correct value in the correct way.
	 * @return The prepared statement.
	 */
	public static synchronized PreparedStatement prepareStatement(String statement, Object... args) {
		
		try {

			Bukkit.getLogger().info("Preparing statement: " + statement);
			PreparedStatement st = SQLUtil.getConnection().prepareStatement(statement);
			for(int i = 0; i < args.length; ++i) {
				
				if(statement.indexOf('?') == -1) { // Safety
					break;
				}
				
				boolean executed = false;
				for(Method m : st.getClass().getMethods()) {
					
					if(!m.getName().startsWith("set")) {
						continue;
					}
					
					Class<?>[] types = m.getParameterTypes();
					if(types.length == 2 && types[0] == int.class && types[1] == args[i].getClass()) {
						executed = true;
						m.invoke(st, i + 1, args[i]);
					}
				}
				
				if(!executed) {
					st.setString(i + 1, args[i].toString());
				}
			}
			
			return st;
			
		} catch (SQLException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * Execute a statement from a string giving all arguments to
	 * set into the string along with it.
	 * 
	 * @param statement The statement SQL syntax string.
	 * @param args The arguments to set into the statement.
	 */
	public static synchronized void execute(String statement, Object... args) {
		PreparedStatement ps = SQLUtil.prepareStatement(statement, args);
		SQLUtil.execute(ps);
	}
	
	/**
	 * Execute an operation on the database using an 
	 * already prepared SQL statement.
	 * 
	 * @param st The statement to use.
	 */
	public static synchronized void execute(final PreparedStatement st) {
		
		new BukkitRunnable() {
			
			@Override
			public void run() {
				
				try {
					st.execute();
				} catch (SQLException e) {
					e.printStackTrace();
				} finally {
					try {
						st.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
		}.runTaskAsynchronously(SQLUtil.plugin);
	}
	
	/**
	 * Execute an update statement from a string giving all 
	 * arguments to set into the string along with it.
	 * 
	 * @param statement The statement SQL syntax string.
	 * @param args The arguments to set into the statement.
	 */
	public static synchronized void update(String statement, Object... args) {
		PreparedStatement ps = SQLUtil.prepareStatement(statement, args);
		SQLUtil.update(ps);
	}
	
	/**
	 * Update the database using an already prepared SQL statement.
	 * 
	 * @param st The statement to use.
	 */
	public static synchronized void update(final PreparedStatement st) {
		
		new BukkitRunnable() {
			
			@Override
			public void run() {
				
				try {
					st.executeUpdate();
				} catch (SQLException e) {
					e.printStackTrace();
				} finally {
					try {
						st.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
		}.runTaskAsynchronously(SQLUtil.plugin);
	}
	
	/**
	 * Insert a set of values into a table if that value does not
	 * already exist. This is an alternative to 'ON DUPLICATE KEY'
	 * as the table does not have to have a primary key. This will
	 * insert every value into the statement regardless of what type
	 * it is. If the type is not found in SQL set methods it will be
	 * set into the statement as a string.
	 * <p>
	 * <b>Important</b>: The columnNames should be the same length as the args. 
	 * If this amount is different if would be bad!
	 * <br>
	 * Arguments will be inserted in the order they come in.
	 * <br>
	 * For example:<pre>
	 * 	INSERT INTO tableName(`columnName1`, `columName2`, `etc.`) VALUES(arg1, args2, etc.)</pre>
	 * This method then will return the ResultSet used to check it 
	 * with the cursor already at the first position.
	 * If this is not wished to be used the use parameter must be set
	 * to false and the ResultSet and PreparedStatement will be closed.
	 * If the use parameter is true then neither object will be closed
	 * and to avoid memory leaks the ResultSet <b>must</b> be closed
	 * using the {@link ResultSet#getStatement()} and from there the
	 * {@link PreparedStatement#close()} method. Null will be returned
	 * if the table did not exist and was placed or an exception was
	 * thrown during the process.
	 * <p>
	 * 
	 * @param use Whether it is wished to use the ResultSet returned.<br>
	 * If set to false the ResultSet will be initially closed before being returned.
	 * @param tableName The name of the SQL table to insert into.
	 * @param select The select part of this statement (e.g. 'SELECT * FROM tableName')
	 * @param where The where statement (e.g. 'WHERE `yourWhere` = yourValue')
	 * @param value The value of the where statement.
	 * @param columnNames The name of all the columns to insert into.
	 * @param args The argument objects to insert into the column.
	 * @return The ResultSet of corresponds to the table.
	 */
	public static synchronized ResultSet insertIfNotExists(boolean use, String tableName, String select, String where, Object value, String[] columnNames, Object... args) {
		
		ResultSet rs = null;
		PreparedStatement st = SQLUtil.prepareStatement("SELECT " + select + " FROM " + tableName + " WHERE `" + where + "` = ?", value);
		
		try {
					
			rs = st.executeQuery();
			if(!rs.next()) {
				
				StringBuilder sb = new StringBuilder("INSERT INTO " + tableName + "(`");
				for(int i = 0; i < columnNames.length; ++i) {
					sb.append(columnNames[i]).append("`, `");
				}
				
				StringBuilder sb1 = new StringBuilder(sb.substring(0, sb.length() - 2));
				sb1.append(") VALUES(");
				for(int i = 0; i < columnNames.length; ++i) {
					sb1.append("?, ");
				}
				
				String insert = sb1.substring(0, sb1.length() - 2) + ')';
				SQLUtil.update(insert, args);
				return null;
			}
			
			return rs;
			
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		} finally {
			
			try {
				
				if(!use) {
					
					if(rs != null) {
						rs.close();
					}
					
					st.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public enum DatabaseType {
		
		SQLite(false, "jdbc:sqlite:"),
		MySQL(true, "jdbc:mysql://address:port/name");

		private boolean rPort;
		private String prefix, address, port, name;
		private DatabaseType(boolean rPort, String prefix) {
			this.rPort = rPort;
			this.prefix = prefix;
		}
		
		/**
		 * Constructs a JDBC SQL address and returns 
		 * it for use in getting an SQL connection.
		 * 
		 * @return An SQL address.
		 */
		private String address() {
			return rPort ? prefix + address + ':' + 
					port + '/' + name : prefix + name;
		}
	}
}
