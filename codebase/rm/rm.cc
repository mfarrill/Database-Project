
#include "rm.h"

#include <algorithm>
#include <cstring>

RelationManager *RelationManager::_rm = 0;

RelationManager *RelationManager::instance()
{
    if (!_rm)
        _rm = new RelationManager();
    return _rm;
}

RelationManager::RelationManager()
    : tableDescriptor(createTableDescriptor()), columnDescriptor(createColumnDescriptor()), indexDescriptor(createIndexDescriptor())
{
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    // Create both tables and columns tables, return error if either fails
    RC rc;
    rc = rbfm->createFile(getFileName(TABLES_TABLE_NAME));
    if (rc)
        return rc;
    rc = rbfm->createFile(getFileName(COLUMNS_TABLE_NAME));
    if (rc)
        return rc;
    // Create index table, return error if fails
    rc = rbfm->createFile(getFileName(INDEXES_TABLE_NAME));
    if (rc)
        return rc;

    // Add table entries for both Tables and Columns
    rc = insertTable(TABLES_TABLE_ID, 1, TABLES_TABLE_NAME);
    if (rc)
        return rc;
    rc = insertTable(COLUMNS_TABLE_ID, 1, COLUMNS_TABLE_NAME);
    if (rc)
        return rc;
    // Add table entries for Indexes
    rc = insertTable(INDEXES_TABLE_ID, 1, INDEXES_TABLE_NAME);
    if (rc)
        return rc;

    // Add entries for tables and columns to Columns table
    rc = insertColumns(TABLES_TABLE_ID, tableDescriptor);
    if (rc)
        return rc;
    rc = insertColumns(COLUMNS_TABLE_ID, columnDescriptor);
    if (rc)
        return rc;
    // Add entries for indexes to Indexes table
    rc = insertColumns(INDEXES_TABLE_ID, indexDescriptor);
    if (rc)
        return rc;

    return SUCCESS;
}

// Just delete the the two catalog files
RC RelationManager::deleteCatalog()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    RC rc;

    rc = rbfm->destroyFile(getFileName(TABLES_TABLE_NAME));
    if (rc)
        return rc;

    rc = rbfm->destroyFile(getFileName(COLUMNS_TABLE_NAME));
    if (rc)
        return rc;
    // delete the index catalog file
    rc = rbfm->destroyFile(getFileName(INDEXES_TABLE_NAME));
    if (rc)
        return rc;

    return SUCCESS;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    RC rc;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    // Create the rbfm file to store the table
    if ((rc = rbfm->createFile(getFileName(tableName))))
        return rc;

    // Get the table's ID
    int32_t id;
    rc = getNextTableID(id);
    if (rc)
        return rc;

    // Insert the table into the Tables table (0 means this is not a system table)
    rc = insertTable(id, 0, tableName);
    if (rc)
        return rc;

    // Insert the table's columns into the Columns table
    rc = insertColumns(id, attrs);
    if (rc)
        return rc;

    return SUCCESS;
}

RC RelationManager::deleteTable(const string &tableName)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // If this is a system table, we cannot delete it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Delete the rbfm file holding this table's entries
    rc = rbfm->destroyFile(getFileName(tableName));
    if (rc)
        return rc;

    // Grab the table ID
    int32_t id;
    rc = getTableID(tableName, id);
    if (rc)
        return rc;

    // Open tables file
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Find entry with same table ID
    // Use empty projection because we only care about RID
    RBFM_ScanIterator rbfm_si;
    vector<string> projection; // Empty
    void *value = &id;

    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);

    RID rid;
    rc = rbfm_si.getNextRecord(rid, NULL);
    if (rc)
        return rc;

    // Delete RID from table and close file
    rbfm->deleteRecord(fileHandle, tableDescriptor, rid);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();

    // Delete from Columns table
    rc = rbfm->openFile(getFileName(COLUMNS_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Find all of the entries whose table-id equal this table's ID
    rbfm->scan(fileHandle, columnDescriptor, COLUMNS_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);

    while ((rc = rbfm_si.getNextRecord(rid, NULL)) == SUCCESS)
    {
        // Delete each result with the returned RID
        rc = rbfm->deleteRecord(fileHandle, columnDescriptor, rid);
        if (rc)
            return rc;
    }
    if (rc != RBFM_EOF)
        return rc;
    rbfm->closeFile(fileHandle);

    rc = rbfm->openFile(getFileName(INDEXES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;
    rbfm->closeFile(fileHandle);
    rbfm_si.close();

    value = malloc(TABLES_COL_TABLE_NAME_SIZE);
    void *columnIndexFileName = malloc(INDEXES_COL_FILE_NAME_SIZE);
    toAPI(tableName, value);

    projection.clear();
    projection.push_back(INDEXES_COL_FILE_NAME);

    rbfm->scan(fileHandle, indexDescriptor, INDEXES_COL_TABLE_NAME, EQ_OP, value, projection, rbfm_si);

    //Loop over index file catalog and delete entries where tableName == tableToDelete
    //also delete the undelying index files
    while ((rc = rbfm_si.getNextRecord(rid, columnIndexFileName)) == SUCCESS)
    {
        string _columnIndexFileName;
        fromAPI(_columnIndexFileName, columnIndexFileName);
        rc = rbfm->destroyFile(_columnIndexFileName);
        if (rc)
            return rc;
        rc = rbfm->deleteRecord(fileHandle, indexDescriptor, rid);
        if (rc)
            return rc;
        memset(columnIndexFileName, 0, INDEXES_COL_COLUMN_NAME_SIZE);
    }
    free(columnIndexFileName);
    free(value);
    if (rc != RBFM_EOF)
        return rc;
    rbfm->closeFile(fileHandle);
    rbfm_si.close();

    return SUCCESS;
}

// Fills the given attribute vector with the recordDescriptor of tableName
RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    // Clear out any old values
    attrs.clear();
    RC rc;

    int32_t id;
    rc = getTableID(tableName, id);
    if (rc)
        return rc;

    void *value = &id;

    // We need to get the three values that make up an Attribute: name, type, length
    // We also need the position of each attribute in the row
    RBFM_ScanIterator rbfm_si;
    vector<string> projection;
    projection.push_back(COLUMNS_COL_COLUMN_NAME);
    projection.push_back(COLUMNS_COL_COLUMN_TYPE);
    projection.push_back(COLUMNS_COL_COLUMN_LENGTH);
    projection.push_back(COLUMNS_COL_COLUMN_POSITION);

    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(COLUMNS_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Scan through the Column table for all entries whose table-id equals tableName's table id.
    rc = rbfm->scan(fileHandle, columnDescriptor, COLUMNS_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);
    if (rc)
        return rc;

    RID rid;
    void *data = malloc(COLUMNS_RECORD_DATA_SIZE);

    // IndexedAttr is an attr with a position. The position will be used to sort the vector
    vector<IndexedAttr> iattrs;
    while ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
    {
        // For each entry, create an IndexedAttr, and fill it with the 4 results
        IndexedAttr attr;
        unsigned offset = 0;

        // For the Columns table, there should never be a null column
        char null;
        memcpy(&null, data, 1);
        if (null)
            rc = RM_NULL_COLUMN;

        // Read in name
        offset = 1;
        int32_t nameLen;
        memcpy(&nameLen, (char *)data + offset, VARCHAR_LENGTH_SIZE);
        offset += VARCHAR_LENGTH_SIZE;
        char name[nameLen + 1];
        name[nameLen] = '\0';
        memcpy(name, (char *)data + offset, nameLen);
        offset += nameLen;
        attr.attr.name = string(name);

        // read in type
        int32_t type;
        memcpy(&type, (char *)data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.attr.type = (AttrType)type;

        // Read in length
        int32_t length;
        memcpy(&length, (char *)data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.attr.length = length;

        // Read in position
        int32_t pos;
        memcpy(&pos, (char *)data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.pos = pos;

        iattrs.push_back(attr);
    }
    // Do cleanup
    rbfm_si.close();
    rbfm->closeFile(fileHandle);
    free(data);
    // If we ended on an error, return that error
    if (rc != RBFM_EOF)
        return rc;

    // Sort attributes by position ascending
    auto comp = [](IndexedAttr first, IndexedAttr second) { return first.pos < second.pos; };
    sort(iattrs.begin(), iattrs.end(), comp);

    // Fill up our result with the Attributes in sorted order
    for (auto attr : iattrs)
    {
        attrs.push_back(attr.attr);
    }

    return SUCCESS;
}
RC RelationManager::getIndexes(const string &tableName, vector<string> &indexes)
{
    RC rc = SUCCESS;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle filehandle;
    vector<string> projection;
    RBFM_ScanIterator rbfmsi;
    RID rid;

    void *value = malloc(sizeof(uint32_t) + tableName.length());
    *(int *)value = tableName.length();
    memcpy((char *)value + sizeof(uint32_t), tableName.c_str(), tableName.length());

    void *indexColumnName = malloc(PAGE_SIZE);
    //toAPI(tableName, value);
    projection.push_back(INDEXES_COL_COLUMN_NAME);
    rc = rbfm->openFile(getFileName(INDEXES_TABLE_NAME), filehandle);
    if (rc)
    {
        free(indexColumnName);
        free(value);
        return rc;
    }
    rc = rbfm->scan(filehandle, indexDescriptor, INDEXES_COL_TABLE_NAME, EQ_OP, value, projection, rbfmsi);
    if (rc)
    {
        free(indexColumnName);
        free(value);
        return rc;
    }
    while ((rc = rbfmsi.getNextRecord(rid, indexColumnName)) == SUCCESS)
    {
        string tmp;
        fromAPI(tmp, indexColumnName);
        indexes.push_back(tmp);
        memset(indexColumnName, 0, INDEXES_COL_COLUMN_NAME_SIZE);
    }
    free(indexColumnName);
    free(value);
    if (rc != RBFM_EOF)
        return rc;
    //rmsi.close();
    rbfmsi.close();
    rbfm->closeFile(filehandle);
    return SUCCESS;
}
RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // If this is a system table, we cannot modify it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Get recordDescriptor
    vector<Attribute> recordDescriptor;
    vector<string> columnsWithIndexes;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;
    rc = getIndexes(tableName, columnsWithIndexes);
    if (rc)
        return rc;

    // And get fileHandle
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    // Let rbfm do all the work
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, data, rid);
    rbfm->closeFile(fileHandle);

    // Insert corresponding record into the index
    IndexManager *im = IndexManager::instance();
    IXFileHandle ixFileHandle;

    // Iterate over attributes in recordDescriptor
    void *value; // This is malloc'd in getColumnFromTuple()
    for (Attribute attr : recordDescriptor)
    {
        if (std::find(columnsWithIndexes.begin(), columnsWithIndexes.end(), attr.name) != columnsWithIndexes.end())
        {
            rc = im->openFile(getIndexFileName(tableName, attr.name), ixFileHandle);
            if (rc)
                return rc;
            rc = RecordBasedFileManager::getColumnFromTuple(data, recordDescriptor, attr.name, value);
            if (rc)
                return rc;
            rc = im->insertEntry(ixFileHandle, attr, value, rid);
            if (rc)
                return rc;
            free(value);
            im->closeFile(ixFileHandle);
        }
    }
    return rc;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // If this is a system table, we cannot modify it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Get recordDescriptor
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // And get fileHandle
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;
    void *data = malloc(PAGE_SIZE);
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, data);
    if (rc)
        return rc;

    IXFileHandle ixFileHandle;
    vector<string> columnsWithIndexes;
    rc = getIndexes(tableName, columnsWithIndexes);
    if (rc)
        return rc;
    IndexManager *im = IndexManager::instance();
    // Iterate over attributes in recordDescriptor
    void *value; // This is malloc'd in getColumnFromTuple()
    for (Attribute attr : recordDescriptor)
    {
        if (std::find(columnsWithIndexes.begin(), columnsWithIndexes.end(), attr.name) != columnsWithIndexes.end())
        {
            rc = im->openFile(getIndexFileName(tableName, attr.name), ixFileHandle);
            if (rc)
                return rc;
            rc = RecordBasedFileManager::getColumnFromTuple(data, recordDescriptor, attr.name, value);
            if (rc)
                return rc;
            rc = im->deleteEntry(ixFileHandle, attr, value, rid);
            if (rc)
                return rc;
            free(value);
            im->closeFile(ixFileHandle);
        }
    }
    free(data);
    // Let rbfm do all the work
    rc = rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
    if (rc)
        return rc;
    rc = rbfm->closeFile(fileHandle);

    return rc;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // If this is a system table, we cannot modify it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Get recordDescriptor
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // And get fileHandle
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;
    void *currentData = malloc(PAGE_SIZE);
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, currentData);
    if (rc)
        return rc;

    IXFileHandle ixFileHandle;
    vector<string> columnsWithIndexes;
    rc = getIndexes(tableName, columnsWithIndexes);
    if (rc)
        return rc;
    IndexManager *im = IndexManager::instance();
    // Iterate over attributes in recordDescriptor
    void *value; // This is malloc'd in getColumnFromTuple()
    for (Attribute attr : recordDescriptor)
    {
        if (std::find(columnsWithIndexes.begin(), columnsWithIndexes.end(), attr.name) != columnsWithIndexes.end())
        {
            rc = im->openFile(getIndexFileName(tableName, attr.name), ixFileHandle);
            if (rc)
                return rc;
            rc = RecordBasedFileManager::getColumnFromTuple(currentData, recordDescriptor, attr.name, value);
            if (rc)
                return rc;
            rc = im->deleteEntry(ixFileHandle, attr, value, rid);
            free(value);
            rc = RecordBasedFileManager::getColumnFromTuple(data, recordDescriptor, attr.name, value);
            if (rc)
                return rc;
            rc = im->insertEntry(ixFileHandle, attr, value, rid);
            free(value);
            if (rc)
                return rc;
            im->closeFile(ixFileHandle);
        }
    }
    free(currentData);

    // Let rbfm do all the work
    rc = rbfm->updateRecord(fileHandle, recordDescriptor, data, rid);
    rbfm->closeFile(fileHandle);

    return rc;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // Get record descriptor
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // And get fileHandle
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    // Let rbfm do all the work
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, data);
    rbfm->closeFile(fileHandle);
    return rc;
}

// Let rbfm do all the work
RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    return rbfm->printRecord(attrs, data);
}

unsigned RelationManager::getTupleSize(const vector<Attribute> &attrs, const void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    return rbfm->getRecordSize(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    rc = rbfm->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);
    rbfm->closeFile(fileHandle);
    return rc;
}

string RelationManager::getIndexFileName(const char *tableName, const char *attributeName)
{
    return string(tableName) + '.' + string(attributeName) + string(INDEX_FILE_EXTENSION);
}

string RelationManager::getIndexFileName(const string &tableName, const string &attributeName)
{
    return tableName + '.' + attributeName + string(INDEX_FILE_EXTENSION);
}

string RelationManager::getFileName(const char *tableName)
{
    return string(tableName) + string(TABLE_FILE_EXTENSION);
}

string RelationManager::getFileName(const string &tableName)
{
    return tableName + string(TABLE_FILE_EXTENSION);
}

vector<Attribute> RelationManager::createTableDescriptor()
{
    vector<Attribute> td;

    Attribute attr;
    attr.name = TABLES_COL_TABLE_ID;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    td.push_back(attr);

    attr.name = TABLES_COL_TABLE_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)TABLES_COL_TABLE_NAME_SIZE;
    td.push_back(attr);

    attr.name = TABLES_COL_FILE_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)TABLES_COL_FILE_NAME_SIZE;
    td.push_back(attr);

    attr.name = TABLES_COL_SYSTEM;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    td.push_back(attr);

    return td;
}

vector<Attribute> RelationManager::createColumnDescriptor()
{
    vector<Attribute> cd;

    Attribute attr;
    attr.name = COLUMNS_COL_TABLE_ID;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)COLUMNS_COL_COLUMN_NAME_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_TYPE;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_LENGTH;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_POSITION;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    return cd;
}

// Create index descriptor
vector<Attribute> RelationManager::createIndexDescriptor()
{
    vector<Attribute> id;

    Attribute attr;
    attr.name = INDEXES_COL_TABLE_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)INDEXES_COL_TABLE_NAME_SIZE;
    id.push_back(attr);

    attr.name = INDEXES_COL_COLUMN_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)INDEXES_COL_COLUMN_NAME_SIZE;
    id.push_back(attr);

    attr.name = INDEXES_COL_FILE_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)INDEXES_COL_FILE_NAME_SIZE;
    id.push_back(attr);

    return id;
}

// Creates the Tables table entry for the given id and tableName
// Assumes fileName is just tableName + file extension
void RelationManager::prepareTablesRecordData(int32_t id, bool system, const string &tableName, void *data)
{
    unsigned offset = 0;

    int32_t name_len = tableName.length();

    string table_file_name = getFileName(tableName);
    int32_t file_name_len = table_file_name.length();

    int32_t is_system = system ? 1 : 0;

    // All fields non-null
    char null = 0;
    // Copy in null indicator
    memcpy((char *)data + offset, &null, 1);
    offset += 1;
    // Copy in table id
    memcpy((char *)data + offset, &id, INT_SIZE);
    offset += INT_SIZE;
    // Copy in varchar table name
    memcpy((char *)data + offset, &name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char *)data + offset, tableName.c_str(), name_len);
    offset += name_len;
    // Copy in varchar file name
    memcpy((char *)data + offset, &file_name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char *)data + offset, table_file_name.c_str(), file_name_len);
    offset += file_name_len;
    // Copy in system indicator
    memcpy((char *)data + offset, &is_system, INT_SIZE);
    offset += INT_SIZE; // not necessary because we return here, but what if we didn't?
}

// Prepares the Columns table entry for the given id and attribute list
void RelationManager::prepareColumnsRecordData(int32_t id, int32_t pos, Attribute attr, void *data)
{
    unsigned offset = 0;
    int32_t name_len = attr.name.length();

    // None will ever be null
    char null = 0;

    memcpy((char *)data + offset, &null, 1);
    offset += 1;

    memcpy((char *)data + offset, &id, INT_SIZE);
    offset += INT_SIZE;

    memcpy((char *)data + offset, &name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char *)data + offset, attr.name.c_str(), name_len);
    offset += name_len;

    int32_t type = attr.type;
    memcpy((char *)data + offset, &type, INT_SIZE);
    offset += INT_SIZE;

    int32_t len = attr.length;
    memcpy((char *)data + offset, &len, INT_SIZE);
    offset += INT_SIZE;

    memcpy((char *)data + offset, &pos, INT_SIZE);
    offset += INT_SIZE;
}

// Prepares the Index table entry for the given name and attribute
void RelationManager::prepareIndexesRecordData(const string &tableName, const string &attrName, void *data)
{
    unsigned offset = 0;

    int32_t name_len = tableName.length();
    int32_t attr_len = attrName.length();
    string table_file_name = getFileName(tableName);
    int32_t file_name_len = table_file_name.length();

    // All fields non-null
    char null = 0;
    // Copy in null indicator
    memcpy((char *)data + offset, &null, 1);
    offset += 1;
    // Copy in varchar table name
    memcpy((char *)data + offset, &name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char *)data + offset, tableName.c_str(), name_len);
    offset += name_len;
    // Copy in varchar attribue name
    memcpy((char *)data + offset, &attr_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char *)data + offset, attrName.c_str(), attr_len);
    offset += attr_len;
    // Copy in varchar file name
    memcpy((char *)data + offset, &file_name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char *)data + offset, table_file_name.c_str(), file_name_len);
    offset += file_name_len;
}
// Insert the given columns into the Columns table
RC RelationManager::insertColumns(int32_t id, const vector<Attribute> &recordDescriptor)
{
    RC rc;

    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(COLUMNS_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    void *columnData = malloc(COLUMNS_RECORD_DATA_SIZE);
    RID rid;
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        int32_t pos = i + 1;
        prepareColumnsRecordData(id, pos, recordDescriptor[i], columnData);
        rc = rbfm->insertRecord(fileHandle, columnDescriptor, columnData, rid);
        if (rc)
            return rc;
    }

    rbfm->closeFile(fileHandle);
    free(columnData);
    return SUCCESS;
}

RC RelationManager::insertTable(int32_t id, int32_t system, const string &tableName)
{
    FileHandle fileHandle;
    RID rid;
    RC rc;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    void *tableData = malloc(TABLES_RECORD_DATA_SIZE);
    prepareTablesRecordData(id, system, tableName, tableData);
    rc = rbfm->insertRecord(fileHandle, tableDescriptor, tableData, rid);
    rbfm->closeFile(fileHandle);
    free(tableData);
    return rc;
}

RC RelationManager::insertIndex(const string &tableName, const string &attrName)
{
    FileHandle fileHandle;
    RID rid;
    RC rc;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    rc = rbfm->openFile(getFileName(INDEXES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    void *indexData = malloc(INDEXES_RECORD_DATA_SIZE);
    prepareIndexesRecordData(tableName, attrName, indexData);
    rc = rbfm->insertRecord(fileHandle, indexDescriptor, indexData, rid);

    rbfm->closeFile(fileHandle);
    free(indexData);
    return rc;
}

// Get the next table ID for creating a table
RC RelationManager::getNextTableID(int32_t &table_id)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc;

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Grab only the table ID
    vector<string> projection;
    projection.push_back(TABLES_COL_TABLE_ID);

    // Scan through all tables to get largest ID value
    RBFM_ScanIterator rbfm_si;
    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_ID, NO_OP, NULL, projection, rbfm_si);

    RID rid;
    void *data = malloc(1 + INT_SIZE);
    int32_t max_table_id = 0;
    while ((rc = rbfm_si.getNextRecord(rid, data)) == (SUCCESS))
    {
        // Parse out the table id, compare it with the current max
        int32_t tid;
        fromAPI(tid, data);
        if (tid > max_table_id)
            max_table_id = tid;
    }
    // If we ended on eof, then we were successful
    if (rc == RM_EOF)
        rc = SUCCESS;

    free(data);
    // Next table ID is 1 more than largest table id
    table_id = max_table_id + 1;
    rbfm->closeFile(fileHandle);
    rbfm_si.close();
    return SUCCESS;
}

// Gets the table ID of the given tableName
RC RelationManager::getTableID(const string &tableName, int32_t &tableID)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc;

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // We only care about the table ID
    vector<string> projection;
    projection.push_back(TABLES_COL_TABLE_ID);

    // Fill value with the string tablename in api format (without null indicator)
    void *value = malloc(4 + TABLES_COL_TABLE_NAME_SIZE);
    int32_t name_len = tableName.length();
    memcpy(value, &name_len, INT_SIZE);
    memcpy((char *)value + INT_SIZE, tableName.c_str(), name_len);

    // Find the table entries whose table-name field matches tableName
    RBFM_ScanIterator rbfm_si;
    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_NAME, EQ_OP, value, projection, rbfm_si);

    // There will only be one such entry, so we use if rather than while
    RID rid;
    void *data = malloc(1 + INT_SIZE);
    if ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
    {
        int32_t tid;
        fromAPI(tid, data);
        tableID = tid;
    }

    free(data);
    free(value);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();
    return rc;
}

// Determine if table tableName is a system table. Set the boolean argument as the result
RC RelationManager::isSystemTable(bool &system, const string &tableName)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc;

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // We only care about system column
    vector<string> projection;
    projection.push_back(TABLES_COL_SYSTEM);

    // Set up value to be tableName in API format (without null indicator)
    void *value = malloc(5 + TABLES_COL_TABLE_NAME_SIZE);
    int32_t name_len = tableName.length();
    memcpy(value, &name_len, INT_SIZE);
    memcpy((char *)value + INT_SIZE, tableName.c_str(), name_len);

    // Find table whose table-name is equal to tableName
    RBFM_ScanIterator rbfm_si;
    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_NAME, EQ_OP, value, projection, rbfm_si);

    RID rid;
    void *data = malloc(1 + INT_SIZE);
    if ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
    {
        // Parse the system field from that table entry
        int32_t tmp;
        fromAPI(tmp, data);
        system = tmp == 1;
    }
    if (rc == RBFM_EOF)
        rc = SUCCESS;

    free(data);
    free(value);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();
    return rc;
}

void RelationManager::toAPI(const string &str, void *data)
{
    int32_t len = str.length();
    char null = 0;

    memcpy(data, &null, 1);
    memcpy((char *)data + 1, &len, INT_SIZE);
    memcpy((char *)data + 1 + INT_SIZE, str.c_str(), len);
}

void RelationManager::toAPI(const int32_t integer, void *data)
{
    char null = 0;

    memcpy(data, &null, 1);
    memcpy((char *)data + 1, &integer, INT_SIZE);
}

void RelationManager::toAPI(const float real, void *data)
{
    char null = 0;

    memcpy(data, &null, 1);
    memcpy((char *)data + 1, &real, REAL_SIZE);
}

void RelationManager::fromAPI(string &str, void *data)
{
    char null = 0;
    int32_t len;

    memcpy(&null, data, 1);
    if (null)
        return;

    memcpy(&len, (char *)data + 1, INT_SIZE);

    char tmp[len + 1];
    tmp[len] = '\0';
    memcpy(tmp, (char *)data + 5, len);

    str = string(tmp);
}

void RelationManager::fromAPI(int32_t &integer, void *data)
{
    char null = 0;

    memcpy(&null, data, 1);
    if (null)
        return;

    int32_t tmp;
    memcpy(&tmp, (char *)data + 1, INT_SIZE);

    integer = tmp;
}

void RelationManager::fromAPI(float &real, void *data)
{
    char null = 0;

    memcpy(&null, data, 1);
    if (null)
        return;

    float tmp;
    memcpy(&tmp, (char *)data + 1, REAL_SIZE);

    real = tmp;
}

// RM_ScanIterator ///////////////

// Makes use of underlying rbfm_scaniterator
RC RelationManager::scan(const string &tableName,
                         const string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const vector<string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator)
{
    // Open the file for the given tableName
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc = rbfm->openFile(getFileName(tableName), rm_ScanIterator.fileHandle);
    if (rc)
        return rc;

    // grab the record descriptor for the given tableName
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // Use the underlying rbfm_scaniterator to do all the work
    rc = rbfm->scan(rm_ScanIterator.fileHandle, recordDescriptor, conditionAttribute,
                    compOp, value, attributeNames, rm_ScanIterator.rbfm_iter);
    if (rc)
        return rc;
    return SUCCESS;
}

// Let rbfm do all the work
RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
    return rbfm_iter.getNextRecord(rid, data);
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
    RC rc;

    // Ensure table exists before creating index.
    int tableID_tmp;
    rc = getTableID(tableName, tableID_tmp);
    if (rc != SUCCESS)
        return rc;

    // Ensure index on attribute is attribute of table.
    vector<Attribute> tableAttrs;
    rc = getAttributes(tableName, tableAttrs);
    if (rc != SUCCESS)
        return rc;

    int attrIndex = -1;
    for (auto it = tableAttrs.begin(); it != tableAttrs.end(); it++)
    {
        auto index = std::distance(tableAttrs.begin(), it);
        Attribute attr = *it;
        if (attr.name.compare(attributeName) == 0)
        {
            attrIndex = index;
            break;
        }
    }

    bool hasTargetAttr = attrIndex >= 0;
    if (!hasTargetAttr)
        return RM_ATTR_DOES_NOT_EXIST;

    // Create index file.
    IndexManager *ixm = IndexManager::instance();
    rc = ixm->createFile(getIndexFileName(tableName, attributeName));
    if (rc != SUCCESS) // This also fails when index file already exists.
        return rc;

    // Insert into index catalog.
    rc = insertIndex(tableName, attributeName);
    if (rc != SUCCESS)
    {
        ixm->destroyFile(getIndexFileName(tableName, attributeName)); // Try to cleanup index file from before.
        return rc;
    }

    // For each tuple in the table:
    //     For each index on the table:
    //         get new key by projecting only the attribute of the index
    //         insert <key, rid> into index.

    RM_ScanIterator rmsi;
    vector<string> tableAttrNames;
    for (auto a : tableAttrs)
    {
        tableAttrNames.push_back(a.name);
    }

    rc = scan(tableName, "", NO_OP, nullptr, tableAttrNames, rmsi); // Scan every tuple.
    if (rc != SUCCESS)
    {
        rmsi.close();
        ixm->destroyFile(getIndexFileName(tableName, attributeName)); // Try to cleanup index file from before.
        return rc;
    }

    IXFileHandle ixFileHandle;
    rc = ixm->openFile(getIndexFileName(tableName, attributeName), ixFileHandle);
    if (rc != SUCCESS)
    {
        rmsi.close();
        return rc;
    }

    RID rid;
    void *data = calloc(PAGE_SIZE, sizeof(uint32_t));
    //void *value = calloc(PAGE_SIZE, sizeof(uint32_t));

    // For each tuple in the table, insert into our index.
    while ((rc = rmsi.getNextTuple(rid, data)) == SUCCESS)
    {
        void *value = nullptr; // This is alloc'd in getColumnFromTuple.
        rc = RecordBasedFileManager::getColumnFromTuple(data, tableAttrs, tableAttrs[attrIndex].name, value);
        if (rc)
        {
            if (rc == RBFM_READ_FAILED) // NULL value in column.
            {
                if (value != nullptr)
                    free(value);
                continue; // Don't insert NULLs into our index.
            }
            else
            {
                free(data);
                if (value != nullptr)
                    free(value);
                rmsi.close();
                return rc; // Some other error means something broke.
            }
        }
        rc = ixm->insertEntry(ixFileHandle, tableAttrs[attrIndex], value, rid);
        free(value);
        if (rc)
        {
            free(data);
            rmsi.close();
            return rc;
        }
    }
    ixm->closeFile(ixFileHandle);
    free(data);
    rmsi.close();
    return SUCCESS;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
    RC rc;

    // Ensure table exists before deleting index.
    int tableID_tmp;
    rc = getTableID(tableName, tableID_tmp);
    if (rc != SUCCESS)
        return rc;

    // Ensure index on attribute is attribute of table.
    vector<Attribute> tableAttrs;
    rc = getAttributes(tableName, tableAttrs);
    if (rc != SUCCESS)
        return rc;
    bool hasTargetAttr = false;
    for (auto attr : tableAttrs)
    {
        if (attr.name.compare(attributeName) == 0)
        {
            hasTargetAttr = true;
            break;
        }
    }
    if (!hasTargetAttr)
        return RM_ATTR_DOES_NOT_EXIST;

    // Destroy index file.
    IndexManager *ixm = IndexManager::instance();
    rc = ixm->destroyFile(getIndexFileName(tableName, attributeName));
    if (rc != SUCCESS)
        return rc;

    // Scan the index catalog for entries on table, get attribute name.
    RM_ScanIterator rmsi;

    uint32_t varcharLength = tableName.length();
    void *compValue = calloc(sizeof(uint32_t) + varcharLength, sizeof(uint8_t));
    memcpy(compValue, &varcharLength, sizeof(uint32_t));
    memcpy((char *)compValue + sizeof(uint32_t), tableName.c_str(), varcharLength);

    vector<string> projectedAttributes{INDEXES_COL_COLUMN_NAME};
    rc = scan(INDEXES_TABLE_NAME, INDEXES_COL_TABLE_NAME, EQ_OP, compValue, projectedAttributes, rmsi);
    if (rc != SUCCESS)
        return rc;

    // For all indexes on table, try to find index on attribute.
    RID rid; // After this loop, if we found our matching attribute, this will be the RID of that tuple.
    bool attributeFound = false;
    void *data = malloc(PAGE_SIZE);
    while (rmsi.getNextTuple(rid, data) == SUCCESS)
    {
        uint8_t SIZEOF_NULL_INDICATOR = 1;
        uint32_t SIZEOF_VARCHAR_LENGTH = sizeof(uint32_t);

        int tupleLength = *(int *)((char *)data + SIZEOF_NULL_INDICATOR);

        char tupleString[tupleLength + 1];
        memcpy(tupleString, (char *)data + SIZEOF_NULL_INDICATOR + SIZEOF_VARCHAR_LENGTH, tupleLength),
            tupleString[tupleLength] = '\0';

        if (strcmp(tupleString, attributeName.c_str()) == 0)
        {
            attributeFound = true;
            break;
        }
    }
    free(compValue);
    free(data);
    rmsi.close();
    if (!attributeFound)
        return RM_ATTR_DOES_NOT_EXIST;

    // Delete from index catalog (using RBFM because it's a system table).
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle indexCatalogHandle;
    rc = rbfm->openFile(getFileName(INDEXES_TABLE_NAME), indexCatalogHandle);
    if (rc != SUCCESS)
        return rc;
    rc = rbfm->deleteRecord(indexCatalogHandle, indexDescriptor, rid);
    if (rc != SUCCESS)
        return rc;
    rc = rbfm->closeFile(indexCatalogHandle);
    if (rc != SUCCESS)
        return rc;

    return SUCCESS;
}

RC RelationManager::indexScan(const string &tableName,
                              const string &attributeName,
                              const void *lowKey,
                              const void *highKey,
                              bool lowKeyInclusive,
                              bool highKeyInclusive,
                              RM_IndexScanIterator &rm_IndexScanIterator)
{
    RC rc;

    // Ensure table exists before creating index.
    int tableID_tmp;
    rc = getTableID(tableName, tableID_tmp);
    if (rc != SUCCESS)
        return rc;

    // Ensure index on attribute is attribute of table.
    Attribute targetAttr; // Get the matching attribute.
    vector<Attribute> tableAttrs;
    rc = getAttributes(tableName, tableAttrs);
    if (rc != SUCCESS)
        return rc;
    bool hasTargetAttr = false;
    for (auto attr : tableAttrs)
    {
        if (attr.name.compare(attributeName) == 0)
        {
            hasTargetAttr = true;
            targetAttr = attr;
            break;
        }
    }
    if (!hasTargetAttr)
        return RM_ATTR_DOES_NOT_EXIST;

    IndexManager *ixm = IndexManager::instance();

    rc = ixm->openFile(getIndexFileName(tableName, attributeName), rm_IndexScanIterator.indexFileHandle);
    if (rc != SUCCESS)
        return rc;

    rc = ixm->scan(
        rm_IndexScanIterator.indexFileHandle,
        targetAttr,
        lowKey,
        highKey,
        lowKeyInclusive,
        highKeyInclusive,
        rm_IndexScanIterator.indexScanIterator);
    if (rc != SUCCESS)
        return rc;

    rm_IndexScanIterator.closed = false;

    return SUCCESS;
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key)
{
    if (closed)
        return -1;
    return indexScanIterator.getNextEntry(rid, key);
}

RC RM_IndexScanIterator::close()
{
    if (closed)
        return -1;

    RC rc;

    rc = indexScanIterator.close();
    if (rc != SUCCESS)
        return rc;

    IndexManager *ixm = IndexManager::instance();
    return ixm->closeFile(indexFileHandle);
}

// Close our file handle, rbfm_scaniterator
RC RM_ScanIterator::close()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    rbfm_iter.close();
    rbfm->closeFile(fileHandle);
    return SUCCESS;
}
