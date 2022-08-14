
#ifndef _rm_h_
#define _rm_h_

#include <iostream>
#include <string>
#include <string.h>
#include <math.h>
#include <vector>

#include "../rbf/rbfm.h"

#define TABLE_DNE 8
#define CATALOG_DNE 9
#define INVALID_PERMISSIONS 10

using namespace std;

#define RM_EOF (-1) // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator
{
public:
  RM_ScanIterator(){};
  RM_ScanIterator(RBFM_ScanIterator ittr) : underlyingIterator(ittr){};
  ~RM_ScanIterator(){};
  //TODO Make RBFM

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data);
  RC close();
  RC reset();

private:
  RBFM_ScanIterator underlyingIterator;
};

class Table
{
public:
  Table(int tableId, string tableName, string fileName) : tableId(tableId), tableName(tableName), fileName(fileName){};
  Table() : Table(-1, "", ""){};
  int tableId;
  string tableName;
  string fileName;
  //50 + 50 + sizeof(int)
  size_t size = 104;
};
// Relation Manager
class RelationManager
{
public:
  static RelationManager *instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Advanced requirement.
  RC addAttribute(const string &tableName, const Attribute &attr);

  // Advanced requirement.
  RC dropAttribute(const string &tableName, const string &attributeName);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
          const string &conditionAttribute,
          const CompOp compOp,                  // comparison type such as "<" and "="
          const void *value,                    // used in the comparison
          const vector<string> &attributeNames, // a list of projected attributes
          RM_ScanIterator &rm_ScanIterator);

protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;
  static RecordBasedFileManager *_rbfm;
  Table *tableTable;
  Table *columnTable;
  const string tableCatalogName = "Tables";
  const string columnCatalogName = "Columns";
  const string fileSuffix = ".t";
  int catalogCreated; //-1 for unsure, 0 for no, 1 for yes

  vector<Attribute> tableCatalogAttributes;
  vector<Attribute> columnCatalogAttributes;
  void addTableToCatalog(Table *table, const vector<Attribute> &attrs);
  void addColumnsToCatalog(const vector<Attribute> &attrs, int tableId);
  void addColumnToCatalog(const Attribute attr, const int tableId, const int columnPosition, FileHandle &columnCatalogFile);
  bool catalogExists();
  Table *getTableFromCatalog(const string &tableName, RID &rid);

  int _tableIndex;
  int getCurrentIndex();
  int getNextIndex();
  FILE *_fIndex;
  const string _fIndexName = "current_highest_table_index";
  RC writeTableIndex(int newIndex);
};


#endif
