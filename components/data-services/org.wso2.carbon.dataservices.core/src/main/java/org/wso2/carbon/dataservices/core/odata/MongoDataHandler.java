

package org.wso2.carbon.dataservices.core.odata;

//from mongoquery
import com.datastax.driver.core.*;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.jongo.*;
import org.json.JSONObject;
import org.wso2.carbon.dataservices.common.DBConstants;
import org.wso2.carbon.dataservices.core.DBUtils;
import org.wso2.carbon.dataservices.core.DataServiceFault;
import org.wso2.carbon.dataservices.core.custom.datasource.DataRow;
import org.wso2.carbon.dataservices.core.custom.datasource.QueryResult;
import org.wso2.carbon.dataservices.core.engine.DataEntry;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.*;

import org.jongo.Jongo;

//import com.mongodb.client.MongoCollection;


import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//import jongo.rest.xstream.Row;


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
    //private Iterator ;


    //private RequestBox session;

    public Jongo getJongo() {
        return jongo;
    }

    public MongoDataHandler(String configID, Jongo jongo){
        this.configID = configID;
        this.jongo=jongo;
        this.tableList=generateTableList();
        this.tableMetaData=generateTableMetaData();
        this.primaryKeys=getPrimaryKeys();
    }


    @Override
    public List<ODataEntry> readTable(String tableName) throws ODataServiceFault {;
        String tmpVal;
        List<ODataEntry> entryList = new ArrayList<>();
        DBCollection result = getJongo().getDatabase().getCollection(tableName);
        Iterator<DBObject> cursor = result.find();
        this.tableMetaData = new HashMap<>();
        ODataEntry dataEntry = new ODataEntry();

//        while (cursor.hasNext()) {
//            final DBObject current = cursor.next();
//            tmpVal = current.toString();
//            //ODataEntry dataEntry = new ODataEntry();
//            //dataEntry.addValue(DBConstants.MongoDB.RESULT_COLUMN_NAME, tmpVal);
//            entryList.add(dataEntry);


        // while (cursor.hasNext()) {
        final DBObject current = cursor.next();
        tmpVal = current.toString();
        //ODataEntry dataEntry = new ODataEntry();
        //dataEntry.addValue(DBConstants.MongoDB.RESULT_COLUMN_NAME, tmpVal);
        //entryList.add(dataEntry);
        Iterator<?> keys = new JSONObject(tmpVal).keys();
        //int i = 1;
        while (keys.hasNext()) {
            String columnName = (String) keys.next();
            String columnValue = new JSONObject(tmpVal).get(columnName).toString();
            //String colName=new JSONObject(tmpVal).
            dataEntry.addValue(columnName, columnValue);
            // i++;
        }
        entryList.add(dataEntry);

        //}
        return entryList;
    }


//            Iterator<?> keys = new JSONObject(tmpVal).keys();
//            int i = 1;
//            while( keys.hasNext() ) {
//                String columnName = (String) keys.next();
//                DataColumn dataColumn = new DataColumn(columnName, DataColumn.ODataDataType.STRING, i, true, 100,
//                        columnName.equals("_id") ? true : false);
//                HashMap<String, DataColumn> column = new HashMap();
//                column.put(columnName, dataColumn);
//                tableMetaData.put(tableName, column);
//                i++;
//            }


    //initializeMetaData(tableName, new JSONObject(tmpVal));   //
    // }
//        QueryResult queryResult = (QueryResult) result;
//        try {
//            while (queryResult.hasNext()) {
//                currentRow = queryResult.next();
//                tmpVal = currentRow.getValueAt(DBConstants.MongoDB.RESULT_COLUMN_NAME);
//                ODataEntry dataEntry = new ODataEntry();
//                dataEntry.addValue(DBConstants.MongoDB.RESULT_COLUMN_NAME, tmpVal);
//                entryList.add(dataEntry);
//            }
//        } catch (DataServiceFault dataServiceFault) {
//            ODataServiceFault fault = new ODataServiceFault("reason");
//            fault.initCause(dataServiceFault);
//            throw fault;
//        }
    // return entryList;

    //}

/*

public List<ODataEntry> readTable(String tableName) throws ODataServiceFault {
        //DBCursor cursor=getJongo().getDatabase().getCollection(tableName).find();
        DataRow currentRow;
        String tmpVal;
        List<ODataEntry> entryList = new ArrayList<>();
       // Iterator<DBObject> iterator =cursor.iterator();
        Object result=getJongo().getDatabase().getCollection(tableName).find();
        QueryResult queryResult = (QueryResult) result;
        try {
            while (queryResult.hasNext()) {
                currentRow = queryResult.next();
                tmpVal = currentRow.getValueAt(DBConstants.MongoDB.RESULT_COLUMN_NAME);
                ODataEntry dataEntry = new ODataEntry();
                dataEntry.addValue(DBConstants.MongoDB.RESULT_COLUMN_NAME, tmpVal);
                entryList.add(dataEntry);
            }
        } catch (DataServiceFault dataServiceFault) {
            ODataServiceFault fault = new ODataServiceFault("reason");
            fault.initCause(dataServiceFault);
            throw fault;
        }
        return entryList;

    }


    public List<ODataEntry> readTable(String tableName) throws ODataServiceFault {

        Statement statement = new SimpleStatement("db." + tableName + ".find()");
        Command command=getJongo().runCommand("db." + tableName + ".find()");

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
//        List<String> tableNames=this.tableList;
//        Map<String, Map<String, DataColumn>> meta = new HashMap<>();
//        Map<String, DataColumn> dataColumnMap = new HashMap<>();
//        //dataColumnMap.put("");
//        //DataColumn a=new DataColumn();
//        for(String tName :tableNames) {
//            //meta.put(tName, dataColumnMap);
//            generateTableMetaData(tName,)
//        }
//        //return tableMetaData;
//        return meta;
        return this.tableMetaData;
    }

    private Map<String, Map<String, DataColumn>> generateTableMetaData() {
        DataRow currentRow;
        //Iterator<?> keys = document.keys();
        int i = 1;
        Map<String, Map<String, DataColumn>> metaData = new HashMap<>();
        for (String tName : this.tableList) {
            DBCollection result = getJongo().getDatabase().getCollection(tName);
            Iterator<DBObject> cursor = result.find();
            //while (cursor.hasNext()) {
            final DBObject current = cursor.next();
            String tmpVal = current.toString();
            JSONObject document = new JSONObject(tmpVal);
            Iterator<?> keys = document.keys();
            //Iterable<?> value=
            HashMap<String, DataColumn> column = new HashMap();
            while (keys.hasNext()) {
                String columnName = (String) keys.next();
                DataColumn dataColumn = new DataColumn(columnName, DataColumn.ODataDataType.STRING, i, true, 100,
                        columnName.equals("_id") ? true : false);
                column.put(columnName, dataColumn);
                i++;
            }
            metaData.put(tName, column);
        }

        // }
        return metaData;
    }

    /**
     * This method initializes metadata.
     *
     * @throws ODataServiceFault
     */
//    private void initializeMetaData(String tableName, JSONObject document) throws ODataServiceFault {
//        this.tableMetaData = new HashMap<>();
//            Iterator<?> keys = document.keys();
//            int i = 1;
//            while( keys.hasNext() ) {
//                String columnName = (String) keys.next();
//                DataColumn dataColumn = new DataColumn(columnName, DataColumn.ODataDataType.STRING, i, true, 100,
//                        columnName.equals("_id") ? true : false);
//                HashMap<String, DataColumn> column = new HashMap();
//                column.put(columnName, dataColumn);
//                tableMetaData.put(tableName, column);
//                i++;
//            }
//    }


    @Override
    public List<String> getTableList() {
        return this.tableList;

    }

    /**
     * This method creates a list of tables available in the DB.
     *
     * @return Table List of the DB
     * @throws ODataServiceFault
     */
    private List<String> generateTableList(){
        List<String> list = new ArrayList<String>();
        list.addAll(getJongo().getDatabase().getCollectionNames());
        return list;

    }



    @Override
    public Map<String, List<String>> getPrimaryKeys() {
        Map<String, List<String>> primaryKeyList=new HashMap<>();
        List<String> tableNames=this.tableList;
        List<String> primaryKey = new ArrayList<>();
        // count =getJongo().getDatabase().getCollectionNames().size();
        for(int x=1; x<=1; x++){
            primaryKey.add("_id");
        }
        for(String tname : tableNames){
            primaryKeyList.put(tname,primaryKey);
        }

        return primaryKeyList;

    }

//    private Map<String, List<String>> generatePrimaryKeyList() {
//        Map<String, List<String>> primaryKeyMap = new HashMap<>();
//        for (String tableName : this.tableList) {
//           List<String> primaryKey = new ArrayList<>();
//           //DBCursor indexes = getJongo().getDatabase().getCollection(tableName).find().returnKey();
//            for (DBObject columnMetadata : getJongo().getDatabase().getCollection(tableName).find().returnKey()) {
//                primaryKey.add(columnMetadata.toString());
//            }
//            primaryKeyMap.put(tableName, primaryKey);
//        }
//       return primaryKeyMap;
//    }


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
     * @param tableName Name of the tableet
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


