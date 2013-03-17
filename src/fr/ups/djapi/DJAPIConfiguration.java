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

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Configuration Singleton used to set up a connection to the database specified
 * in the configuration file or given trough properties.<br>
 * 
 * 
 * @author Benjamin Babic
 */
public final class DJAPIConfiguration {

	/**
	 * Error logger
	 */
	private static Logger logger = Logger.getLogger("DJAPIConfiguration");

	/**
	 * Unique instance of DJAPIConfiguration
	 */
	private static volatile DJAPIConfiguration instance = null;

	/**
	 * Url of the database
	 */
	private String url = "";

	/**
	 * Login to database
	 */
	private String login = "";

	/**
	 * Database password
	 */
	private String password = "";

	/**
	 * Driver used for database connection
	 */
	private String driver = "";

	/**
	 * Path of the configuration file if used. Default value: "db.connect"
	 */
	private String filePath = "db.connect";

	/*
	 * Creates the singleton Configuration using a formated file described by
	 * the file path.
	 */
	private DJAPIConfiguration(String path) {
		this.filePath = path;
		getConfigurationFromFile();
	}

	/*
	 * Creates the singleton Configuration using the default file name
	 * djapi_connect
	 */
	private DJAPIConfiguration() {
		this.filePath = "djapi_connect";
		getConfigurationFromFile();
	}

	/*
	 * Creates the singleton Configuration using function parameters. Allows not
	 * to store passwords and logins in a readable file
	 */
	private DJAPIConfiguration(String driver, String url, String login,
			String password) {
		this.driver = driver;
		this.url = url;
		this.login = login;
		this.password = password;
	}

	/*
	 * Creates the singleton Configuration using properties object.
	 */
	private DJAPIConfiguration(Properties properties) {
		this.url = properties.getProperty("Url");
		this.login = properties.getProperty("Login");
		this.driver = properties.getProperty("Driver");
		this.password = properties.getProperty("Password");
	}

	/*
	 * Logger initialization
	 */
	private static final void initiateLogger() {
		if (logger.getLevel() == null) {
			logger.setLevel(Level.ALL);
		}
	}

	/*
	 * Loads a configuration from a file.
	 */
	private final void getConfigurationFromFile() {
		Properties prop = new Properties();
		try {
			FileInputStream in = new FileInputStream(filePath);
			prop.load(in);

			this.url = prop.getProperty("Url");
			this.login = prop.getProperty("Login");
			this.driver = prop.getProperty("Driver");
			this.password = prop.getProperty("Password");
			in.close();
		} catch (Exception e) { // if the file was not found
			logger.warning("File not found: "
					+ filePath
					+ " If default path was used be sure to create the djapi_connect file");
		}
	}

	/**
	 * Creates the unique instance of Configuration using the file specified in
	 * the parameter path.
	 * 
	 * @param path
	 *            the path where the configuration file is located
	 * @return DJAPIConfiguration instance using Singleton Pattern
	 */
	public final static DJAPIConfiguration getInstance(String path) {
		if (instance == null) // we try to avoid using synchronized
		{
			synchronized (DJAPIConfiguration.class) {
				if (instance == null) {
					initiateLogger();
					instance = new DJAPIConfiguration(path);
				}
			}
		}
		return instance;
	}

	/**
	 * Creates the unique instance of Configuration using the default file path
	 * ./djapi_connect.
	 * 
	 * @return DJAPIConfiguration instance using Singleton Pattern
	 */
	public final static DJAPIConfiguration getInstance() {
		if (instance == null) // we try to avoid using synchronized
		{
			synchronized (DJAPIConfiguration.class) {
				if (instance == null) {
					initiateLogger();
					instance = new DJAPIConfiguration();
				}
			}
		}
		return instance;
	}

	/**
	 * Creates the unique instance of Configuration using parameters of the
	 * function. Used for raising the security by coding parameters in a class
	 * instead of reading them from a file.
	 * 
	 * @param driver
	 *            the driver to use for connecting to the database
	 * @param url
	 *            the url of the database (e.g: for
	 *            postgresqljdbc:postgresql://distant_ip
	 *            :defaultport/databasename
	 * @param login
	 *            the login name used for setting the connection.
	 * @param password
	 *            the password for connecting to the database.
	 * @return DJAPIConfiguration instance using Singleton Pattern
	 */
	public final static DJAPIConfiguration getInstance(String driver,
			String url, String login, String password) {
		initiateLogger();
		if (instance == null) // we try to avoid using synchronized
		{
			synchronized (DJAPIConfiguration.class) {
				if (instance == null)
					instance = new DJAPIConfiguration(driver, url, login,
							password);
			}
		}
		return instance;
	}

	/**
	 * Creates the unique instance of Configuration using a java.util.Properties
	 * object.
	 * 
	 * Warning: the properties object must contain following properties: - Url -
	 * Driver - Password - login
	 * 
	 * @param properties
	 *            the properties file.
	 * @return DJAPIConfiguration instance using Singleton Pattern
	 */
	public final static DJAPIConfiguration getInstance(Properties properties) {
		initiateLogger();
		if (instance == null) // we try to avoid using synchronized
		{
			synchronized (DJAPIConfiguration.class) {
				if (instance == null)
					instance = new DJAPIConfiguration(properties);
			}
		}
		return instance;
	}

	/**
	 * Returns the URL within the configuration
	 * 
	 * @return the URL
	 */
	protected String getUrl() {
		return url;
	}

	/**
	 * Returns the login within the configuration
	 * 
	 * @return the login
	 */
	protected String getLogin() {
		return login;
	}

	/**
	 * Returns the password within the configuration
	 * 
	 * @return the password
	 */
	protected String getPassword() {
		return password;
	}

	/**
	 * Returns the driver name within the configuration
	 * 
	 * @return the driver
	 */
	protected String getDriver() {
		return driver;
	}

	/**
	 * Gets the java.sql.Connection object representing the actual connection to
	 * the database.
	 * 
	 * @return java.sql.Connection; the connection object
	 */
	public final Connection connect() {
		DJAPIConfiguration conf = DJAPIConfiguration.getInstance(); // Getting
																	// conf
		Connection con = null;
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
		return con;
	}

}
