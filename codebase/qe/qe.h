#ifndef _qe_h_
#define _qe_h_

#include <algorithm>
#include <vector>

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"
#define QE_EOF (-1) // end of the index scan
#define QE_NO_SUCH_ATTR (-2)
#define QE_MISMATCHED_ATTR_TYPES (-3)
#define QE_NO_SUCH_ATTR_TYPE (-4)

using namespace std;

typedef enum
{
    MIN = 0,
    MAX,
    COUNT,
    SUM,
    AVG
} AggregateOp;

struct AttrData
{
    Attribute attr;
    int size;
    bool isNull;
    void *data;
};

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters
//

struct Value
{
    AttrType type; // type of value
    void *data;    // value
    int compare(const Value *rhs);
    bool compare(const Value *rhs, const CompOp op);
    static int compare(const void *key, const void *value, const AttrType attrType);
    static int compare(const int key, const int value);
    static int compare(const float key, const float value);
    static int compare(const char *key, const char *value);
};

struct Condition
{
    string lhsAttr;  // left-hand side attribute
    CompOp op;       // comparison operator
    bool bRhsIsAttr; // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string rhsAttr;  // right-hand side attribute if bRhsIsAttr = TRUE
    Value rhsValue;  // right-hand side value if bRhsIsAttr = FALSE
};

class Iterator
{
    // All the relational operators and access methods are iterators.
public:
    virtual RC getNextTuple(void *data) = 0;
    virtual void getAttributes(vector<Attribute> &attrs) const = 0;
    virtual ~Iterator(){};
};

class TableScan : public Iterator
{
    // A wrapper inheriting Iterator over RM_ScanIterator
public:
    RelationManager &rm;
    RM_ScanIterator *iter = nullptr;
    string tableName;
    vector<Attribute> attrs;
    vector<string> attrNames;
    RID rid;

    TableScan(RelationManager &rm, const string &tableName, const char *alias = NULL) : rm(rm)
    {
        //Set members
        this->tableName = tableName;

        // Get Attributes from RM
        rm.getAttributes(tableName, attrs);

        // Get Attribute Names from RM
        unsigned i;
        for (i = 0; i < attrs.size(); ++i)
        {
            // convert to char *
            attrNames.push_back(attrs.at(i).name);
        }

        // Call RM scan to get an iterator
        iter = new RM_ScanIterator();
        rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

        // Set alias
        if (alias)
            this->tableName = alias;
    };

    // Start a new iterator given the new compOp and value
    void setIterator()
    {
        iter->close();
        delete iter;
        iter = new RM_ScanIterator();
        rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
    };

    RC getNextTuple(void *data)
    {
        return iter->getNextTuple(rid, data);
    };

    void getAttributes(vector<Attribute> &attrs) const
    {
        attrs.clear();
        attrs = this->attrs;
        unsigned i;

        // For attribute in vector<Attribute>, name it as rel.attr
        for (i = 0; i < attrs.size(); ++i)
        {
            string tmp = tableName;
            tmp += ".";
            tmp += attrs.at(i).name;
            attrs.at(i).name = tmp;
        }
    };

    ~TableScan()
    {
        if (iter != nullptr)
        {
            iter->close();
            delete iter;
        }
    };
};

class IndexScan : public Iterator
{
    // A wrapper inheriting Iterator over IX_IndexScan
public:
    RelationManager &rm;
    RM_IndexScanIterator *iter = nullptr;
    string tableName;
    string attrName;
    vector<Attribute> attrs;
    char key[PAGE_SIZE];
    RID rid;

    IndexScan(RelationManager &rm, const string &tableName, const string &attrName, const char *alias = NULL) : rm(rm)
    {
        // Set members
        this->tableName = tableName;
        this->attrName = attrName;

        // Get Attributes from RM
        rm.getAttributes(tableName, attrs);

        // Call rm indexScan to get iterator
        iter = new RM_IndexScanIterator();
        rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

        // Set alias
        if (alias)
            this->tableName = alias;
    };

    // Start a new iterator given the new key range
    void setIterator(void *lowKey,
                     void *highKey,
                     bool lowKeyInclusive,
                     bool highKeyInclusive)
    {
        iter->close();
        delete iter;
        iter = new RM_IndexScanIterator();
        rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
                     highKeyInclusive, *iter);
    };

    RC getNextTuple(void *data)
    {
        int rc = iter->getNextEntry(rid, key);
        if (rc == 0)
        {
            rc = rm.readTuple(tableName.c_str(), rid, data);
        }
        return rc;
    };

    void getAttributes(vector<Attribute> &attrs) const
    {
        attrs.clear();
        attrs = this->attrs;
        unsigned i;

        // For attribute in vector<Attribute>, name it as rel.attr
        for (i = 0; i < attrs.size(); ++i)
        {
            string tmp = tableName;
            tmp += ".";
            tmp += attrs.at(i).name;
            attrs.at(i).name = tmp;
        }
    };

    ~IndexScan()
    {
        if (iter != nullptr)
        {
            iter->close();
            delete iter;
        }
    };
};

class Filter : public Iterator
{
    // Filter operator
public:
    Filter(Iterator *input,           // Iterator of input R
           const Condition &condition // Selection condition
           ) : iter_{input}, cond_{condition} {};
    ~Filter(){};

    RC getNextTuple(void *data);
    // For attribute in vector<Attribute>, name it as rel.attr
    void getAttributes(vector<Attribute> &attrs) const;

private:
    Iterator *iter_;
    const Condition cond_;
};

class Project : public Iterator
{
    // Projection operator
public:
    // Assumes that attrNames to project are all valid attributes of tuples from the underlying input Iterator.
    Project(Iterator *input,
            const vector<string> &attrNames) : iter_{input}, attrNames_{attrNames}
    {
        iter_->getAttributes(attrsBeforeProjection_);
        for (auto aname : attrNames_)
        {
            auto matchingAttr = [aname](Attribute a) { return aname == a.name; };
            auto match = std::find_if(attrsBeforeProjection_.begin(), attrsBeforeProjection_.end(), matchingAttr);
            if (match != attrsBeforeProjection_.end())
            {
                attrs_.push_back(*match);
            }
        }
    };
    ~Project(){};

    RC getNextTuple(void *data);
    // For attribute in vector<Attribute>, name it as rel.attr
    void getAttributes(vector<Attribute> &attrs) const;

private:
    Iterator *iter_;
    vector<Attribute> attrsBeforeProjection_;
    vector<Attribute> attrs_;
    vector<string> attrNames_;
};

class INLJoin : public Iterator
{
    // Index nested-loop join operator
public:
    INLJoin(Iterator *leftIn,          // Iterator of input R
            IndexScan *rightIn,        // IndexScan Iterator of input S
            const Condition &condition // Join condition
            ) : left(leftIn), right(rightIn), condition(condition)
    {
        left->getAttributes(leftDescriptor);
        right->getAttributes(rightDescriptor);
        for (Attribute attr : leftDescriptor)
        {
            if (attr.name.compare(condition.lhsAttr) == 0)
            {
                leftJoinAttr = attr;
                break;
            }
        }
        for (Attribute attr : rightDescriptor)
        {
            if (attr.name.compare(condition.rhsAttr) == 0)
            {
                rightJoinAttr = attr;
                break;
            }
        }
    };
    ~INLJoin(){};

    RC getNextTuple(void *data);
    // For attribute in vector<Attribute>, name it as rel.attr
    void getAttributes(vector<Attribute> &attrs) const
    {
        attrs.clear();
        attrs = this->leftDescriptor;
        for (unsigned int i = 0; i < (unsigned int)rightDescriptor.size(); ++i)
        {
            attrs.push_back(rightDescriptor.at(i));
        }
    };

private:
    Iterator *left;
    IndexScan *right;
    vector<Attribute> leftDescriptor;
    vector<Attribute> rightDescriptor;
    Attribute leftJoinAttr;
    Attribute rightJoinAttr;
    const Condition condition;
    void concat(const void *left, const void *right, void *data);
};

RC evalPredicate(bool &result,
                 const void *leftTuple, Condition condition, const void *rightTuple,
                 const vector<Attribute> leftAttrs,
                 const vector<Attribute> rightAttrs);

#endif
