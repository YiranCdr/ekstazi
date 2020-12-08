/*
 * Copyright 2014-present Milos Gligoric
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ekstazi.data;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.sql.*;
import java.util.*;

import com.mongodb.client.*;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import static com.mongodb.client.model.Filters.*;
import org.ekstazi.Config;
import org.ekstazi.hash.Hasher;
import org.ekstazi.log.Log;

/**
 * IO API for storing/reading dependencies. Currently there is no
 * enforcement, but it is expected that all subclasses write/read
 * magic/version sequence as the first several bytes/characters.
 */
public abstract class Storer {

    /**
     * Storing mode.
     */
    public enum Mode {
        TXT("# 1"), PREFIX_TXT("# 4"), BIN("# 2"), TIME_TXT("# 3");

        /** Magic/version sequence */
        private final String mMagicSequence;

        /**
         * Constructor.
         *
         * @param magicSequence
         *            Magic/version sequence associated with mode.
         */
        private Mode(String magicSequence) {
            this.mMagicSequence = magicSequence;
        }

        /**
         * Returns magic/version sequence for this mode.
         *
         * @return Magic/version sequence.
         */
        public String getMagicSequence() {
            return mMagicSequence;
        }

        /**
         * This method guarantees to return a correct mode if one is not cannot
         * be parsed.
         *
         * @param text
         *            Mode as provides in configuration.
         *
         * @return Storing mode.
         */
        public static Mode fromString(String text) {
            if (text != null) {
                for (Mode b : Mode.values()) {
                    if (text.equalsIgnoreCase(b.name())) {
                        return b;
                    }
                }
            }
            return TXT;
        }
    }

    /** Mode of this storer */
    protected final Mode mMode;

    /**
     * Constructor.
     */
    protected Storer(Mode mode) {
        this.mMode = mode;
    }

    /**
     * Loads regression data.
     */
    public final Set<RegData> load(String dirName, String fullName) {
        return load(openFileRead(dirName, fullName, fullName, null));
    }

    /**
     * Loads regression data.
     */
    public final Set<RegData> load(String dirName, String className, String methodName) {
        String fullName = className + '.' + methodName;
        return load(openFileRead(dirName, fullName, className, methodName));
    }


    private Set<RegData> query(Connection connection, String tableName, String colName, String value) throws SQLException {
        String selectString = "select * from " + tableName + " where " + colName + " = " + "\'" + value + "\'";
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(selectString);

        Set<RegData> hashes = new HashSet<RegData>();

        while (resultSet.next()) {
//            String fullName = resultSet.getString(1);
            String fileUrl = resultSet.getString(2);
            String hash = resultSet.getString(3);
            hashes.add(new RegData(fileUrl, hash));
        }
        resultSet.close();
        statement.close();

        return hashes;
    }

    public final Set<RegData> myload(String dirName, String fullName) {
        Set<RegData> hashes = null;

        if (Config.USE_POSTGRESQL) {
            // my local postgresql database
            String url = "jdbc:postgresql://localhost/postgres";
            Properties props = new Properties();
            props.setProperty("user","apple");
            props.setProperty("password","ranpengFEI123");
            props.setProperty("reWriteBatchedInserts=true", "true");
            Connection connection = null;

            try {
                connection = DriverManager.getConnection(url, props);
                hashes = query(connection, "SE", "fullname", fullName);
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
//            // MongoDB
            MongoClient mongoClient = MongoClients.create();
            MongoDatabase database = mongoClient.getDatabase("mydb");
            MongoCollection<Document> collection = database.getCollection("SE");

            MongoCursor<Document> cursor = collection.find(eq("fullname", fullName)).iterator();
            hashes = new HashSet<RegData>();
            try {
                while (cursor.hasNext()) {
                    Document doc = cursor.next();
                    List<String> urls = (ArrayList<String>)doc.get("url");
                    List<String> hashvalues = (ArrayList<String>)doc.get("hashvalue");
                    for (int i = 0; i < urls.size(); ++i) {
                        hashes.add(new RegData(urls.get(i), hashvalues.get(i)));
                    }
                }
            } finally {
                cursor.close();
            }
        }
        return hashes;
    }

    // my load
    public final Set<RegData> myload(String dirName, String className, String methodName) {
        String fullName = className + '.' + methodName;
        return myload(dirName, fullName);
    }

    /**
     * Saves regression data.
     */
    public final void save(String dirName, String fullName, Set<RegData> hashes) {
        // @Research(if statement).
        if (!Config.X_DEPENDENCIES_SAVE_V) {
            return;
        }
        // Ensure that the directory for coverage exists.
        new File(dirName).mkdirs();
        save(openFileWrite(dirName, fullName, fullName, null), hashes);
    }

    /**
     * Saves regression data.
     */
    public final void save(String dirName, String className, String methodName, Set<RegData> regData) {
        // @Research(if statement).
        if (!Config.X_DEPENDENCIES_SAVE_V) {
            return;
        }
        // Ensure that the directory for coverage exists.
        new File(dirName).mkdir();
        String fullName = className + '.' + methodName;
        save(openFileWrite(dirName, fullName, className, methodName), regData);
    }

    public static void insertRow(Connection connection, String tableName, String values) throws SQLException {
        // use batch insertion without autocommit to insert more rows at a time
//        System.out.println("inserting row");
        String insertString =
                "insert into "+ tableName +
                        " values " + values;
        Statement statement = connection.createStatement();
        statement.executeUpdate(insertString);
        statement.close();
    }

    public static void deleteRows(Connection connection, String tableName, String ColName, String value) throws SQLException {
        String deleteString =
                "delete from "+ tableName +
                        " where " + ColName + " = " + "\'" + value + "\'";
        Statement statement = connection.createStatement();
        statement.executeUpdate(deleteString);
        statement.close();
    }

    /**
     * my DB storer
     */
    public final void mysave(String dirName, String fullName, Set<RegData> hashes) {
        // @Research(if statement).
        if (!Config.X_DEPENDENCIES_SAVE_V) {
            return;
        }

        if (Config.USE_POSTGRESQL) {
            // my local postgresql database
            String url = "jdbc:postgresql://localhost/postgres";
            Properties props = new Properties();
            props.setProperty("user","apple");
            props.setProperty("password","ranpengFEI123");
            props.setProperty("reWriteBatchedInserts=true", "true");

            Connection connection = null;

            try {
                connection = DriverManager.getConnection(url, props);
                deleteRows(connection, "SE", "fullname", fullName);
                for (RegData regDatum : hashes) {
                    if (regDatum.getURLExternalForm().startsWith("file")) {
                        insertRow(connection, "SE", "(" +
                                "\'" + fullName + "\'" + ", " +
                                "\'" + regDatum.getURLExternalForm() + "\'" + ", " +
                                "\'" + regDatum.getHash() + "\'" +
                                ")");
                    }
                }
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
//            // MongoDB
            MongoClient mongoClient = MongoClients.create();
            MongoDatabase database = mongoClient.getDatabase("mydb");
            MongoCollection<Document> collection = database.getCollection("SE");

            DeleteResult deleteResult = collection.deleteMany(eq("fullname", fullName));
            List<String> urls = new ArrayList<String>();
            List<String> hashvalues = new ArrayList<String>();
            for (RegData regDatum : hashes) {
                if (regDatum.getURLExternalForm().startsWith("file")) {
                    urls.add(regDatum.getURLExternalForm());
                    hashvalues.add(regDatum.getHash());
                }
            }
            Document doc = new Document("fullname", fullName)
                    .append("url", urls)
                    .append("hashvalue", hashvalues);
            collection.insertOne(doc);
        }
    }


    public final void mysave(String dirName, String className, String methodName, Set<RegData> regData) {
        String fullName = className + '.' + methodName;
        mysave(dirName, fullName, regData);
    }

    /**
     * Loading actual data from the given stream. Implementation in subclasses
     * should have matching load and save methods.
     *
     * @param fis
     *            Stream that contains regression information.
     * @return Regression data.
     */
    protected abstract Set<RegData> extendedLoad(FileInputStream fis);

    /**
     * Saving regression data to the given stream. Implementation in subclasses
     * should have matching load and save methods.
     *
     * @param fos
     *            Stream that stores regression information.
     * @param hashes
     *            Regression info as mapping URL(ExternalForm)->hash.
     */
    protected abstract void extendedSave(FileOutputStream fos, Set<RegData> hashes);

    // INTERNAL

    private final Set<RegData> load(FileInputStream fis) {
        if (fis != null) {
            return extendedLoad(fis);
        } else {
            return Collections.emptySet();
        }
    }

    private final void save(FileOutputStream fos, Set<RegData> hashes) {
        if (fos != null) {
            extendedSave(fos, hashes);
        }
    }

    /**
     * Opens {@link FileInputStream}. This method checks if file name is too
     * long and hashes the file name. If there are some other problems, the
     * method gives up and returns null.
     *
     * @param dirName
     *            Destination directory.
     * @param fullName
     *            Name of the file.
     * @return Opened {@link FileInputStream} or null if operation was not
     *         successful.
     */
    private static FileInputStream openFileRead(String dirName, String fullName, String firstPart, String secondPart) {
        try {
            return new FileInputStream(new File(dirName, fullName));
        } catch (FileNotFoundException ex1) {
            // If file name is too long hash it and try again.
            String message = ex1.getMessage();
            if (message != null && message.contains("File name too long")) {
                if (secondPart != null) {
                    long hashedSecondPart = Hasher.hashString(secondPart);
                    fullName = firstPart + "." + Long.toString(hashedSecondPart);
                    // Note that we pass fullName as the second argument too.
                    return openFileRead(dirName, fullName, fullName, null);
                } else if (firstPart != null) {
                    long hashedFirstPart = Hasher.hashString(firstPart);
                    fullName = Long.toString(hashedFirstPart);
                    return openFileRead(dirName, fullName, null, null);
                } else {
                    // No hope.
                    Log.w("Could not open file for reading (name too long) " + fullName);
                }
            }
            return null;
        }
    }

    /**
     * Opens {@link FileOutputStream}. This method checks if file name is too
     * long and hashes the name of the file. If there are other problems, this
     * method gives up and returns null.
     *
     * @param dirName
     *            Destination directory.
     * @param fullName
     *            File name.
     * @return {@link FileOutputStream} or null if operation was not successful.
     */
    private static FileOutputStream openFileWrite(String dirName, String fullName, String firstPart, String secondPart) {
        try {
            return new FileOutputStream(new File(dirName, fullName));
        } catch (FileNotFoundException ex1) {
            // If file name is too long hash it and try again.
            String message = ex1.getMessage();
            if (message != null && message.contains("File name too long")) {
                if (secondPart != null) {
                    long hashedSecondPart = Hasher.hashString(secondPart);
                    fullName = firstPart + "." + Long.toString(hashedSecondPart);
                    // Invoke again with firstPart.HASH(secondPart).
                    return openFileWrite(dirName, fullName, fullName, null);
                } else if (firstPart != null) {
                    long hashedFirstPart = Hasher.hashString(firstPart);
                    fullName = Long.toString(hashedFirstPart);
                    return openFileWrite(dirName, fullName, null, null);
                } else {
                    // No hope.
                    Log.w("Could not open file for writing (name too long) " + fullName);
                }
            } else {
                Log.w("Could not open file for writing " + fullName + " " + ex1.getMessage());
            }
            return null;
        }
    }
}
