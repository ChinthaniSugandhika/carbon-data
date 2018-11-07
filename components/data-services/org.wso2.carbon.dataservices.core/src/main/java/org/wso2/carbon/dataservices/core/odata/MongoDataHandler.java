

package org.wso2.carbon.dataservices.core.odata;

//from mongoquery
import com.datastax.driver.core.*;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.apache.jasper.tagplugins.jstl.core.Set;
import org.jongo.Jongo;
import org.wso2.carbon.dataservices.common.DBConstants;
import org.wso2.carbon.dataservices.core.boxcarring.RequestBox;
import org.wso2.carbon.dataservices.core.custom.datasource.DataRow;
import org.wso2.carbon.dataservices.core.engine.DataEntry;

import java.util.*;

import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.jongo.ResultHandler;
import org.jongo.Update;

//import com.mongodb.client.MongoCollection;


import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//import jongo.rest.xstream.Row;

import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




public class MongoDataHandler implements ODataDataHandler{
    /**
     * Config ID.
     */

    private final String configID;

    /**
     * MongoDB session.
     */
    //private Session session;

    /**
     * MongoDB Client
     */
    //private final MongoClient mongoClient;

    /**
     * MongoDB Database
     */
    //private final MongoDatabase database;


    /**
     * List of Collections in the Database.
     */
    //private List<String> collectionList;

    /**
     * ObjectID s of the Collections (Map<Table Name, List>).
     */
    private Map<String, List<String>> primaryKeys;

    /**
     * List of Tables in the Database.
     */
    private List<String> tableList;

    /**
     * Table metadata.
     */
    private Map<String, Map<String, DataColumn>> tableMetaData;


    private Jongo jongo;

    //private RequestBox session;

    public Jongo getJongo() {
        return jongo;
    }

    public MongoDataHandler(String configID, Jongo jongo){
        this.configID = configID;
        this.jongo=jongo;
        this.tableList=generateTableList();
        this.tableMetaData=generateMetaData();
        this.primaryKeys=generatePrimaryKeyList();
    }



    public List<ODataEntry> readTable(String tableName) throws ODataServiceFault {
        List<ODataEntry> entryList = new ArrayList<>();
        //getJongo().getDatabase().getCollection(tableName).
        return  entryList;
    }

    /*
    public List<ODataEntry> readTable(String tableName) throws ODataServiceFault {

        Statement statement = new SimpleStatement("db." + tableName + ".find()");
        ResultSet resultSet = this.session.execute(statement);
        Iterator<Row> iterator = resultSet.iterator();
        List<ODataEntry> entryList = new ArrayList<>();
        DataRow currentRow;
        String tmpVal;
        while (iterator.hasNext()) {
            currentRow= (DataRow) iterator.next();
            tmpVal = currentRow.getValueAt(DBConstants.MongoDB.RESULT_COLUMN_NAME);
            ODataEntry dataEntry = new ODataEntry();
            dataEntry.addValue(DBConstants.MongoDB.RESULT_COLUMN_NAME, tmpVal);
            entryList.add(dataEntry);
        }
        return entryList;
    }


    private List<ODataEntry> createDataEntryCollectionFromDocument(String tableName, ResultSet resultSet)throws ODataServiceFault {
        List<ODataEntry> entitySet = new ArrayList<>();
        Iterator<Row> iterator = resultSet.iterator();
        String tmpVal;
        DataRow currentRow;
        DataColumn currentField;
        while (iterator.hasNext()) {
            ODataEntry entry = new ODataEntry();
            currentRow = (DataRow) iterator.next();
            tmpVal = currentRow.getValueAt(DBConstants.MongoDB.RESULT_COLUMN_NAME);
            entry.addValue(DBConstants.MongoDB.RESULT_COLUMN_NAME, tmpVal);
            //Set Etag to the entity
            entry.addValue("ETag", ODataUtils.generateETag(this.configID, tableName, entry));
            entitySet.add(entry);
        }
        return entitySet;
    }

    */
    /**
     * This method read the table with Keys and return.
     * Return a list of DataEntry object which has been wrapped the entity.
     *
     * @param tableName Name of the table
     * @param keys      Keys to check
     * @return EntityCollection
     * @throws ODataServiceFault
     * @see DataEntry
     */
    public List<ODataEntry> readTableWithKeys(String tableName, ODataEntry keys) throws ODataServiceFault{
        List<ODataEntry> entitySet = new ArrayList<>();
        return entitySet;
    }

    /**
     * This method inserts entity to table.
     *
     * @param tableName Name of the table
     * @param entity    Entity
     * @throws ODataServiceFault
     */
    public ODataEntry insertEntityToTable(String tableName, ODataEntry entity) throws ODataServiceFault{
        ODataEntry entitySet=new ODataEntry();
        return entitySet;
    }

    /**
     * This method deletes entity from table.
     *
     * @param tableName Name of the table
     * @param entity    Entity
     * @throws ODataServiceFault
     */
    public boolean deleteEntityInTable(String tableName, ODataEntry entity) throws ODataServiceFault{
        boolean x=true;
        return true;
    }

    /**
     * This method updates entity in table.
     *
     * @param tableName     Name of the table
     * @param newProperties New Properties
     * @throws ODataServiceFault
     */
    public boolean updateEntityInTable(String tableName, ODataEntry newProperties) throws ODataServiceFault{
        boolean x=true;
        return true;
    }

    /**
     * This method updates the entity in table when transactional update is necessary.
     *
     * @param tableName     Table Name
     * @param oldProperties Old Properties
     * @param newProperties New Properties
     * @throws ODataServiceFault
     */
    public boolean updateEntityInTableTransactional(String tableName, ODataEntry oldProperties, ODataEntry newProperties)
            throws ODataServiceFault{
        boolean x=true;
        return true;
    }

    /**
     * This method return database table metadata.
     * Return a map with table name as the key, and the values contains maps with column name as the map key,
     * and the values of the column name map will DataColumn object, which represents the column.
     *
     * @return Database Metadata
     * @see org.wso2.carbon.dataservices.core.odata.DataColumn
     */
    @Override
    public Map<String, Map<String, DataColumn>> getTableMetadata() {
        return this.getTableMetadata();
    }

    private Map<String, Map<String, DataColumn>> generateMetaData() {
        List<String> tableNames=this.tableList;
        Map<String, Map<String, DataColumn>> meta = new HashMap<>();
        Map<String, DataColumn> dataColumnMap = new HashMap<>();
        for(String tName :tableNames) {
            meta.put(tName, dataColumnMap);
        }
        return meta;
    }


    @Override
    public List<String> getTableList() {
        return this.tableList;

    }

    /**
     * This method creates a list of tables available in the DB.
     * @return Table List of the DB
     * @throws ODataServiceFault
     */
    private List<String> generateTableList() {
        List<String> list = new ArrayList<String>();
        list.addAll(getJongo().getDatabase().getCollectionNames());
        return list;

    }

    @Override
    public Map<String, List<String>> getPrimaryKeys() {
        return this.getPrimaryKeys();
    }

    private Map<String, List<String>> generatePrimaryKeyList() {
        Map<String, List<String>> primaryKeyList=new HashMap<>();
        List<String> tableNames=this.tableList;
        List<String> primaryKey = new ArrayList<>();
        // count =getJongo().getDatabase().getCollectionNames().size();
        //for(int x=1; x<=1; x++){
         //   primaryKey.add("_id");
        //}
        primaryKey.add("_id");
        for(String tname : tableNames){
            primaryKeyList.put(tname,primaryKey);
        }

        return primaryKeyList;
    }

    @Override
    public Map<String, NavigationTable> getNavigationProperties() {
        return null;
    }

    /**
     * This method opens the transaction.
     *
     * @throws ODataServiceFault
     */
    public void openTransaction() throws ODataServiceFault{}

    /**
     * This method commits the transaction.
     *
     * @throws ODataServiceFault
     */
    public void commitTransaction() throws ODataServiceFault{}

    /**
     * This method rollbacks the transaction.
     *
     * @throws ODataServiceFault
     */
    public void rollbackTransaction() throws ODataServiceFault{}

    /**
     * This method updates the references of the table where the keys were imported.
     *
     * @param rootTableName       Root - Table Name
     * @param rootTableKeys       Root - Entity keys (Primary Keys)
     * @param navigationTable     Navigation - Table Name
     * @param navigationTableKeys Navigation - Entity Name (Primary Keys)
     * @throws ODataServiceFault
     */
    public void updateReference(String rootTableName, ODataEntry rootTableKeys, String navigationTable,
                                ODataEntry navigationTableKeys) throws ODataServiceFault{}

    /**
     * This method deletes the references of the table where the keys were imported.
     *
     * @param rootTableName       Root - Table Name
     * @param rootTableKeys       Root - Entity keys (Primary Keys)
     * @param navigationTable     Navigation - Table Name
     * @param navigationTableKeys Navigation - Entity Name (Primary Keys)
     * @throws ODataServiceFault
     */
    public void deleteReference(String rootTableName, ODataEntry rootTableKeys, String navigationTable,
                                ODataEntry navigationTableKeys) throws ODataServiceFault{}







/*

    /**
     * This method creates Mongo query to delete data.
     *
     * @param tableName Name of the table
     * @return sql Query
     */

/*
    private String createDeleteMongo(String tableName) {
        StringBuilder sql = new StringBuilder();
        sql.append("db.").append(tableName).append(".deleteMany").append(" ({ ");
        List<String> pKeys = this.primaryKeys.get(tableName);
        boolean propertyMatch = false;
        for (String key : pKeys) {
            if (propertyMatch) {
                sql.append(" AND ");
            }
            sql.append(key).append(" = ").append(" ? ");
            propertyMatch = true;
        }
        sql.append(" }) ");
        return sql.toString();
    }

*/

/*
    /**
     * This method creates a Mongo query to update data(single document).
     *
     * @param tableName  Name of the table
     * @param properties Properties
     * @return sql Query
     */
/*
    private String createUpdateEntityMongo(String tableName, ODataEntry properties) {
        List<String> pKeys = primaryKeys.get(tableName);    //not sure. How to retrive the ObjectID/PK of a document in mongodb
        StringBuilder sql = new StringBuilder();
        sql.append("db.").append(tableName).append(".updateOne").append("(").append("{");
        boolean propertyMatch = false;
        //Handling keys
        for(String key : pKeys){  //keys? or properties?
            if(propertyMatch){
                sql.append(",");
            }
            sql.append(key).append(":").append(" ? ");
            propertyMatch=true;
        }
        sql.append("}");
        sql.append("{").append("$set").append(":").append("{");
        for (String column : properties.getNames()) {
            if (!pKeys.contains(column)) {
                if (propertyMatch) {
                    sql.append(",");
                }
                sql.append(column).append(" : ").append(" ? ");
                propertyMatch = true;
            }
            sql.append("}").append(")");

        }
        return sql.toString();


    }

    /**
     * This method creates Mongo query to read data with keys.
     *
     * @param tableName Name of the table
     * @param keys      Keys
     * @return sql Query
     */
/*
    private String createReadMongoWithKeys(String tableName, ODataEntry keys) {
        StringBuilder sql = new StringBuilder();
        sql.append("db ").append(tableName).append(".find ").append("(");
        boolean propertyMatch = false;
        for (String column : this.rdbmsDataTypes.get(tableName).keySet()) {     //rdbmsDataTypes or tableMetaData thing????
            if (keys.getNames().contains(column)) {
                if (propertyMatch) {
                    sql.append(" , ");
                }
                sql.append(column).append(" : ").append(" ? ");
                propertyMatch = true;
            }
        }
        return sql.toString();
    }



    private String createReadSqlWithKeys(String tableName, ODataEntry keys) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM ").append(tableName).append(" WHERE ");
        boolean propertyMatch = false;
        for (DataColumn column : this.tableMetaData.get(tableName).values()) {
            if (keys.getValue(column.getColumnName()) != null) {
                if (propertyMatch) {
                    sql.append(" AND ");
                }
                sql.append(column.getColumnName()).append(" = ").append(" ? ");
                propertyMatch = true;
            }
        }
        return sql.toString();
    }



    /**
     * This method creates a Mongo query to insert data in table.
     *
     * @param tableName Name of the table
     * @return mongoQuery
     */
/*
    private String createInsertSQL(String tableName, ODataEntry entry) {
        StringBuilder sql = new StringBuilder();
        sql.append("db. ").append(tableName).append("insertOne").append("(").append("{");
        boolean propertyMatch = false;
        for (String column : entry.getNames()) {
            if (this.rdbmsDataTypes.get(tableName).keySet().contains(column)) {     //rdbmsDayaTypes ??? how to retrive column names???
                if (propertyMatch) {
                    sql.append(",");
                }
                sql.append(column).append(":").append("?");
                propertyMatch = true;
            }
        }
        sql.append(")").append("}");
        return sql.toString();
    }


    private String createInsertCQL(String tableName, ODataEntry entry) {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(tableName).append(" (");
        boolean propertyMatch = false;
        for (DataColumn column : this.tableMetaData.get(tableName).values()) {
            if (entry.getValue(column.getColumnName()) != null) {
                if (propertyMatch) {
                    sql.append(",");
                }
                sql.append(column.getColumnName());
                propertyMatch = true;
            }
        }
        sql.append(" ) VALUES ( ");
        propertyMatch = false;
        for (DataColumn column : this.tableMetaData.get(tableName).values()) {
            if (entry.getValue(column.getColumnName()) != null) {
                if (propertyMatch) {
                    sql.append(",");
                }
                sql.append(" ? ");
                propertyMatch = true;
            }
        }
        sql.append(" ) ");
        return sql.toString();
    }
    */

}


