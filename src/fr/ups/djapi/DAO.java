/*
 * Licensed to the Apache Software Foundation (ASF) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright
 * ownership. The ASF licenses this file to you under the Apache
 * License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package fr.ups.djapi;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import fr.ups.djapi.DJAPIConfiguration;

/**
 * Contains the lower level of database connection.
 */
public abstract class DAO {

	protected Logger logger = Logger.getLogger("DAO");
	/*
	 * Stores the connection informations.
	 */
	protected Connection con = null;
	/*
	 * A statement is the object used for executing static SQL statements and
	 * returning the objects it produces.
	 */
	protected PreparedStatement stmt = null;

	protected ResultSet rs = null;

	/**
	 * Sets the connection to the database.
	 */
	protected DAO() {
		// TODO: global launching logger activation
		if (logger.getLevel() == null) { // Setting logger
			logger.setLevel(Level.ALL);
		}
		DJAPIConfiguration conf = DJAPIConfiguration.getInstance(); // Getting conf
		try {
			Class.forName(conf.getDriver());
			con = DriverManager.getConnection(conf.getUrl(), conf.getLogin(),
					conf.getPassword());
			logger.finer("Connection set up on " + conf.getUrl());
		} catch (SQLException e) {
			logger.severe("Unable set the connection @Url: " + conf.getUrl());
		} catch (ClassNotFoundException e) {
			logger.severe("Unable to load the following driver class:"
					+ conf.getDriver());
		}
	}

	protected void closeAll() {
		close(this.rs);
		close(this.stmt);
	}

	/**
	 * Closes a ResultSet.
	 */
	protected void close(ResultSet rs) {
		try {
			rs.close();
		} catch (SQLException ex) {
			logger.severe("Unable to close ResultSet");
		}
	}

	/**
	 * Closes a statement.
	 */
	protected void close(PreparedStatement pstmt) {
		try {
			if (pstmt != null) {
				pstmt.close();
				pstmt = null;
			}
		} catch (SQLException ex) {
			logger.warning("Unable to close Statement !");
			logger.fine("Current Statement: " + pstmt);
			logger.fine("Returned error: " + ex);
		}
	}

	/**
	 * Executes the prepared SQL statement.
	 * 
	 * NOTE: It controls if the result of the query contains anything and
	 * returns null to allow different processing for that answer. It is anyway
	 * recommended to raise a program exception.
	 * 
	 */
	protected boolean executeQuery() {
		boolean b = true;
		try {
			rs = stmt.executeQuery();
			// places the ResultSet to the first row, returns false if row is
			// empty and sets the ResultSet to null.
			if (!next()) {
				rs = null;
				b = false;
			}
		} catch (SQLException ex) {
			logger.severe("Unable to execute update.");
			logger.fine("Current Statement: " + stmt);
			logger.finer("Returned error: " + ex);
			b = false;
		}
		return b;
	}

	/**
	 * Executes the UPDATE, INSERT or DELETE query in the prepared SQL
	 * statement.
	 * 
	 * @return boolean telling if the update was executed.
	 */
	protected boolean executeUpdate() {
		boolean b;
		try {
			stmt.executeUpdate();
			b = true;
		} catch (SQLException ex) {
			logger.severe("Unable to execute update.");
			logger.fine("Current Statement: " + stmt);
			logger.finer("Returned error: " + ex);
			b = false;
		}
		return b;
	}

	/**
	 * Retrieves any auto-generated keys created as a result of executing this
	 * Statement object. If this Statement object did not generate any keys, an
	 * empty ResultSet object is returned.
	 * 
	 */
	protected void getGeneratedKeys() {
		try {
			rs = stmt.getGeneratedKeys();
			if (!next()) // nothing to retrieve
				rs = null;
		} catch (SQLException ex) {
			logger.severe("Unable to get generated keys !");
			logger.fine("Current Statement: " + stmt);
			logger.finer("Returned error: " + ex);
		}
	}

	/**
	 * Prepares the query.
	 */
	protected void set(String request) {
		try {
			stmt = con.prepareStatement(request);
		} catch (SQLException ex) {
			logger.severe("Unable to prepare querry.");
			logger.fine("Current request: " + request);
			logger.finer("Returned error: " + ex);
		}
	}

	/**
	 * Update the statement object to get the capability to retrieve
	 * auto-generated keys and prepares the query.
	 */
	protected void set(String request, int autoGeneratedKey) {
		try {
			stmt = con.prepareStatement(request, autoGeneratedKey);
		} catch (SQLException ex) {
			logger.severe("Unable to prepare querry.");
			logger.fine("Current request: " + request);
			logger.finer("Returned error: " + ex);
		}
	}

	/**
	 * Set given int at given index
	 * 
	 * @param index
	 *            the index
	 * @param i
	 *            the int to set
	 */
	protected void setInt(int index, int i) {
		try {
			stmt.setInt(index, i);
		} catch (SQLException ex) {
			logger.severe("Unable to set int: " + i);
			logger.fine("Current Statement: " + stmt);
			logger.finer("Returned error: " + ex);
		}
	}

	/**
	 * Set given string at given index
	 * 
	 * @param index
	 *            the index
	 * @param s
	 *            the String to set
	 */
	protected void setString(int index, String s) {
		try {
			stmt.setString(index, s);
		} catch (SQLException ex) {
			logger.severe("Unable to set string: " + s);
			logger.fine("Current Statement: " + stmt);
			logger.finer("Returned error: " + ex);
		}
	}

	/**
	 * Set given date at given index
	 * 
	 * @param index
	 *            the index
	 * @param dt
	 *            the date to set
	 */
	protected void setTimestamp(int index, Timestamp dt) {
		try {
			stmt.setTimestamp(index, dt);
		} catch (SQLException ex) {
			logger.severe("Unable to set date: " + dt);
			logger.fine("Current Statement: " + stmt);
			logger.finer("Returned error: " + ex);
		}
	}

	/**
	 * Set given boolean at given index
	 * 
	 * @param index
	 *            the index
	 * @param b
	 *            the boolean to set
	 */
	protected void setBoolean(int index, Boolean b) {
		try {
			stmt.setBoolean(index, b);
		} catch (SQLException ex) {
			logger.severe("Unable to set boolean: " + b);
			logger.fine("Current Statement: " + stmt);
			logger.finer("Returned error: " + ex);
		}
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * ResultSet object as a integer in the Java programming language.
	 * 
	 * @param s
	 *            the SQL name of the column
	 * @return the column value; if the value is SQL NULL, the value returned is
	 *         0
	 */
	protected int getInt(String s) {
		int ret = -1;
		try {
			ret = rs.getInt(s);
		} catch (SQLException ex) {
			logger.severe("Unable to get integer from resultset");
			logger.fine("Current statement: " + stmt);
			logger.finer("Returned error: " + ex);
		}
		return ret;
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * ResultSet object as a string in the Java programming language.
	 * 
	 * @param s
	 *            the SQL name of the column
	 * @return the column value; if the value is SQL NULL, the value returned is
	 *         null
	 */
	protected String getString(String s) {
		String ret = "";
		try {
			ret = rs.getString(s);
		} catch (SQLException ex) {
			logger.severe("Unable to get integer from resultset");
			logger.fine("Current statement: " + stmt);
			logger.finer("Returned error: " + ex);
		}
		return ret;
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * ResultSet object as a boolean in the Java programming language.
	 * 
	 * @param s
	 *            the SQL name of the column
	 * @return the column value; if the value is SQL NULL, the value returned is
	 *         false
	 */
	protected boolean getBoolean(String s) {
		boolean ret = false;
		try {
			ret = rs.getBoolean(s);
		} catch (SQLException ex) {
			logger.severe("Unable to get integer from resultset");
			logger.fine("Current statement: " + stmt);
			logger.finer("Returned error: " + ex);
		}
		return ret;
	}

	/**
	 * Retrieves the value of the designated column in the current row of this
	 * ResultSet object as a java.sql.Timestamp object.
	 * 
	 * @param s
	 *            the SQL name of the column
	 * @return the column value; if the value is SQL NULL, the value returned is
	 *         null
	 */
	protected Timestamp getTimestamp(String s) {
		Timestamp ret = null;
		try {
			ret = rs.getTimestamp(s);
		} catch (SQLException ex) {
			logger.severe("Unable to get integer from resultset");
			logger.fine("Current statement: " + stmt);
			logger.finer("Returned error: " + ex);
		}
		return ret;
	}

	/**
	 * Moves the cursor down one row from its current position. If an input
	 * stream is open for the current row, a call to the method next will
	 * implicitly close it. A ResultSet object's warning chain is cleared when a
	 * new row is read.
	 * 
	 * @return true if the new current row is valid; false if there are no more
	 *         rows
	 */
	protected boolean next() {
		boolean b = false;
		try {
			b = rs.next();
		} catch (SQLException ex) {
			logger.severe("ResultSet error. ");
			logger.fine("Current Statement " + stmt);
			logger.finer("Returned error: " + ex);
		}
		return b;
	}

}
