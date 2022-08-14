#include <iostream>
#include <string>
#include <cassert>
#include <sys/stat.h>
#include <stdlib.h> 
#include <string.h>
#include <stdexcept>
#include <stdio.h> 
#include <math.h>

#include "pfm.h"
#include "rbfm.h"

using namespace std;

const int success = 0;


// Check whether a file exists
bool FileExists(string &fileName)
{
    struct stat stFileInfo;

    if(stat(fileName.c_str(), &stFileInfo) == 0) return true;
    else return false;
}


// Calculate actual bytes for nulls-indicator for the given field counts
int getActualByteForNullsIndicator(int fieldCount) {

    return ceil((double) fieldCount / CHAR_BIT);
}


// After createFile() check
int createFileShouldSucceed(string &fileName) 
{
    if(FileExists(fileName))
    {
        cout << "File " << fileName << " has been created properly." << endl << endl;
        return 0;
    }
    else
    {
        cout << "[Fail] Failed to create the file: " << fileName << endl;
        cout << "[Fail] Test Case Failed!" << endl << endl;
        return -1;
    }
}

// After destroyFile() check
int destroyFileShouldSucceed(string &fileName) 
{
    if(FileExists(fileName))
    {
        cout << "[Fail] Failed to destory the file: " << fileName << endl;
        cout << "[Fail] Test Case Failed!" << endl << endl;
        return -1;
    }
    else
    {
        cout << "File " << fileName << " has been destroyed properly." << endl << endl;
        return 0;
    }
}
    
// Function to prepare the data in the correct form to be inserted/read
void prepareRecord(int fieldCount, unsigned char *nullFieldsIndicator, const int nameLength, const string &name, const int age, const float height, const int salary, void *buffer, int *recordSize)
{
    int offset = 0;

    // Null-indicators
    bool nullBit = false;
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(fieldCount);

    // Null-indicator for the fields
    memcpy((char *)buffer + offset, nullFieldsIndicator, nullFieldsIndicatorActualSize);
    offset += nullFieldsIndicatorActualSize;

    // Beginning of the actual data    
    // Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
    // e.g., if a record consists of four fields and they are all nulls, then the bit representation will be: [11110000]

    // Is the name field not-NULL?
    nullBit = nullFieldsIndicator[0] & (1 << 7);

    if (!nullBit) {
        memcpy((char *)buffer + offset, &nameLength, sizeof(int));
        offset += sizeof(int);
        memcpy((char *)buffer + offset, name.c_str(), nameLength);
        offset += nameLength;
    }

    // Is the age field not-NULL?
    nullBit = nullFieldsIndicator[0] & (1 << 6);
    if (!nullBit) {
        memcpy((char *)buffer + offset, &age, sizeof(int));
        offset += sizeof(int);
    }

    // Is the height field not-NULL?
    nullBit = nullFieldsIndicator[0] & (1 << 5);
    if (!nullBit) {
        memcpy((char *)buffer + offset, &height, sizeof(float));
        offset += sizeof(float);
    }

    // Is the height field not-NULL?
    nullBit = nullFieldsIndicator[0] & (1 << 4);
    if (!nullBit) {
        memcpy((char *)buffer + offset, &salary, sizeof(int));
        offset += sizeof(int);
    }

    *recordSize = offset;
}

void prepareLargeRecord(int fieldCount, unsigned char *nullFieldsIndicator, const int index, void *buffer, int *size)
{
    int offset = 0;

    // compute the count
    int count = (index + 2) % 50 + 1;

    // compute the letter
    char text = (index + 2) % 26 + 97;

    // Null-indicators
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(fieldCount);

    // Null-indicators
    memcpy((char *)buffer + offset, nullFieldsIndicator, nullFieldsIndicatorActualSize);
    offset += nullFieldsIndicatorActualSize;

    // Actual data
    for(int i = 0; i < 10; i++)
    {
        memcpy((char *)buffer + offset, &count, sizeof(int));
        offset += sizeof(int);

        for(int j = 0; j < count; j++)
        {
            memcpy((char *)buffer + offset, &text, 1);
            offset += 1;
        }

        // compute the integer
        memcpy((char *)buffer + offset, &index, sizeof(int));
        offset += sizeof(int);

        // compute the floating number
        float real = (float)(index + 1);
        memcpy((char *)buffer + offset, &real, sizeof(float));
        offset += sizeof(float);
    }
    *size = offset;
}

// Increments all integers within a prepared large record.
void prepareLargeRecord_incrementIntegers(int index, vector<Attribute> recordDescriptor, void *buffer, RecordBasedFileManager *rbfm)
{
    int offset = 0;

    // compute the count
    int count = (index + 2) % 50 + 1;

    // Null-indicators
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());

    // Null-indicators
    offset += nullFieldsIndicatorActualSize;

    // Actual data
    for(int i = 0; i < 10; i++)
    {
        // Skip over VARCHAR length and value.
        offset += sizeof(int) + count;

        // compute the integer
        auto newIndex = index + 1;
        memcpy((char *)buffer + offset, &newIndex, sizeof(int));
        offset += sizeof(int);

        // Skip over the float.
        offset += sizeof(float);
    }
}

void createRecordDescriptor(vector<Attribute> &recordDescriptor) {

    Attribute attr;
    attr.name = "EmpName";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)30;
    recordDescriptor.push_back(attr);

    attr.name = "Age";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);

    attr.name = "Height";
    attr.type = TypeReal;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);

    attr.name = "Salary";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);

}

void createRecordDescriptor_varchar2048(vector<Attribute> &recordDescriptor)
{
    recordDescriptor.clear();
    Attribute attr;
    attr.name = "Char";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)2048;
    recordDescriptor.push_back(attr);
}

void prepareRecord_varchar2048(string varchar, vector<Attribute> &recordDescriptor, void *buffer, int *size)
{
    int offset = 0;
    createRecordDescriptor_varchar2048(recordDescriptor);

    // Null-indicators
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    void *nullFieldsIndicator = calloc(nullFieldsIndicatorActualSize, sizeof(uint8_t));
    memcpy((char *)buffer + offset, nullFieldsIndicator, nullFieldsIndicatorActualSize);
    free(nullFieldsIndicator);
    offset += nullFieldsIndicatorActualSize;

    const int nchars = varchar.size();
    memcpy((char *)buffer + offset, &nchars, sizeof(int));
    offset += sizeof(int);

    memcpy((char *)buffer + offset, varchar.c_str(), nchars);
    offset += nchars;

    *size = offset;
}

void createRecordDescriptor_int_varchar2048_real(vector<Attribute> &recordDescriptor)
{
    recordDescriptor.clear();

    Attribute attr;
    attr.name = "Int";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);

    attr.name = "Char";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)2048;
    recordDescriptor.push_back(attr);

    attr.name = "Real";
    attr.type = TypeReal;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);
}

void prepareRecord_int_varchar2048_real(int intVal, string varchar, float realVal, vector<Attribute> &recordDescriptor, void *buffer, int *size)
{
    int offset = 0;
    createRecordDescriptor_int_varchar2048_real(recordDescriptor);

    // Null-indicators
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    void *nullFieldsIndicator = calloc(nullFieldsIndicatorActualSize, sizeof(uint8_t));
    memcpy((char *)buffer + offset, nullFieldsIndicator, nullFieldsIndicatorActualSize);
    free(nullFieldsIndicator);
    offset += nullFieldsIndicatorActualSize;

    // Write int value.
    memcpy((char *)buffer + offset, &intVal, sizeof(int));
    offset += sizeof(int);

    // Write length of varchar.
    const int nchars = varchar.size();
    memcpy((char *)buffer + offset, &nchars, sizeof(int));
    offset += sizeof(int);

    // Write varchar value.
    memcpy((char *)buffer + offset, varchar.c_str(), nchars);
    offset += nchars;

    // Write real value.
    memcpy((char *)buffer + offset, &realVal, sizeof(float));
    offset += sizeof(float);

    *size = offset;
}

void createRecordDescriptor_real(vector<Attribute> &recordDescriptor)
{
    recordDescriptor.clear();

    Attribute attr;
    attr.name = "Real";
    attr.type = TypeReal;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);
}

void prepareRecord_real(float realVal, vector<Attribute> &recordDescriptor, void *buffer, int *size)
{
    int offset = 0;
    createRecordDescriptor_real(recordDescriptor);

    // Null-indicators
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    void *nullFieldsIndicator = calloc(nullFieldsIndicatorActualSize, sizeof(uint8_t));
    memcpy((char *)buffer + offset, nullFieldsIndicator, nullFieldsIndicatorActualSize);
    free(nullFieldsIndicator);
    offset += nullFieldsIndicatorActualSize;

    // Write real value.
    memcpy((char *)buffer + offset, &realVal, sizeof(float));
    offset += sizeof(float);

    *size = offset;
}

void createLargeRecordDescriptor(vector<Attribute> &recordDescriptor)
{
    char *suffix = (char *)malloc(10);
    for(int i = 0; i < 10; i++)
    {
        Attribute attr;
        sprintf(suffix, "%d", i);
        attr.name = "Char";
        attr.name += suffix;
        attr.type = TypeVarChar;
        attr.length = (AttrLength)50;
        recordDescriptor.push_back(attr);

        sprintf(suffix, "%d", i);
        attr.name = "Int";
        attr.name += suffix;
        attr.type = TypeInt;
        attr.length = (AttrLength)4;
        recordDescriptor.push_back(attr);

        sprintf(suffix, "%d", i);
        attr.name = "Real";
        attr.name += suffix;
        attr.type = TypeReal;
        attr.length = (AttrLength)4;
        recordDescriptor.push_back(attr);
    }
    free(suffix);
}

void prepareLargeRecord2(int fieldCount, unsigned char *nullFieldsIndicator, const int index, void *buffer, int *size) {
    int offset = 0;

    // compute the count
    int count = (index + 2) % 60 + 1;

    // compute the letter
    char text = (index + 2) % 26 + 65;

    // Null-indicators
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(fieldCount);

    // Null-indicators
    memcpy((char *)buffer + offset, nullFieldsIndicator, nullFieldsIndicatorActualSize);
    offset += nullFieldsIndicatorActualSize;

    for (int i = 0; i < 10; i++) {
        // compute the integer
        memcpy((char *) buffer + offset, &index, sizeof(int));
        offset += sizeof(int);

        // compute the floating number
        float real = (float) (index + 1);
        memcpy((char *) buffer + offset, &real, sizeof(float));
        offset += sizeof(float);

        // compute the varchar field
        memcpy((char *) buffer + offset, &count, sizeof(int));
        offset += sizeof(int);

        for (int j = 0; j < count; j++) {
            memcpy((char *) buffer + offset, &text, 1);
            offset += 1;
        }

    }
    *size = offset;
}

void createLargeRecordDescriptor2(vector<Attribute> &recordDescriptor) {
    char *suffix = (char *) malloc(10);
    for (int i = 0; i < 10; i++) {
        Attribute attr;
        sprintf(suffix, "%d", i);
        attr.name = "Int";
        attr.name += suffix;
        attr.type = TypeInt;
        attr.length = (AttrLength) 4;
        recordDescriptor.push_back(attr);

        sprintf(suffix, "%d", i);
        attr.name = "Real";
        attr.name += suffix;
        attr.type = TypeReal;
        attr.length = (AttrLength) 4;
        recordDescriptor.push_back(attr);

        sprintf(suffix, "%d", i);
        attr.name = "Char";
        attr.name += suffix;
        attr.type = TypeVarChar;
        attr.length = (AttrLength) 60;
        recordDescriptor.push_back(attr);

    }
    free(suffix);
}

/* Create a page with all unforwarded records.
 * Assumes that we're starting on a fresh page.
 *
 * On return, index will be set such that inserting a prepared large record,
 * called with that index, will be placed on the next page.
 */
void *createUnforwardedPage(RecordBasedFileManager *rbfm, FileHandle &fileHandle, int &index, vector<Attribute> &recordDescriptor, vector<RID> &rids, vector<int> &sizes)
{
    createLargeRecordDescriptor(recordDescriptor);

    // NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    RID rid;
    PageNum initialPageNum;
    bool isFirstIteration = true;

    // Insert just enough records to fill up one page.
    while (true)
    {
        int size = 0;

        void *record = calloc(1000, sizeof(uint8_t));
        prepareLargeRecord(recordDescriptor.size(), nullsIndicator, index, record, &size);
        auto rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
        free(record);

        assert(rc == success && "Inserting a record should not fail.");

        if (isFirstIteration)
        {
            initialPageNum = rid.pageNum;
            isFirstIteration = false;
        }
        else if (initialPageNum != rid.pageNum)
        {
            rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
            break;
        }

        index++;
        rids.push_back(rid);
        sizes.push_back(size);
    }

    void *pageData = malloc(PAGE_SIZE);
    auto rc = fileHandle.readPage(rids.front().pageNum, pageData);
    assert(rc == SUCCESS && "Reading page should not fail.");

    free(nullsIndicator);
    return pageData;
}

void *createUnforwardedPage_varchar2048(RecordBasedFileManager *rbfm, FileHandle &fileHandle, int &index, vector<Attribute> &recordDescriptor, vector<RID> &rids, vector<int> &sizes)
{
    createRecordDescriptor_varchar2048(recordDescriptor);

    RID rid;
    PageNum initialPageNum;
    bool isFirstIteration = true;

    // Insert just enough records to fill up one page.
    while (true)
    {
        int size = 0;

        void *record = calloc(PAGE_SIZE, sizeof(uint8_t));
        prepareRecord_varchar2048("a", recordDescriptor, record, &size); 
        auto rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
        free(record);

        assert(rc == success && "Inserting a record should not fail.");

        if (isFirstIteration)
        {
            initialPageNum = rid.pageNum;
            isFirstIteration = false;
        }
        else if (initialPageNum != rid.pageNum)
        {
            rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
            break;
        }

        index++;
        rids.push_back(rid);
        sizes.push_back(size);
    }

    void *pageData = malloc(PAGE_SIZE);
    auto rc = fileHandle.readPage(rids.front().pageNum, pageData);
    assert(rc == SUCCESS && "Reading page should not fail.");

    return pageData;
}


// Creates two pages, where a slot in the first page forwards to a slot in the second.
void createForwardingPages(RecordBasedFileManager *rbfm, FileHandle &fileHandle, RID &forwardingRID, vector<Attribute> &recordDescriptor, vector<RID> rids)
{
    int rc;
    RID lastRID;
    PageNum initialPageNum;
    bool firstIteration = true;

    RID rid;
    int size;

    // Insert records until the page is full.
    do
    {
        void *record = calloc(PAGE_SIZE, sizeof(uint8_t));
        prepareRecord_varchar2048("a", recordDescriptor, record, &size); 
        rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
        assert(rc == SUCCESS && "Insert record should not fail.");
        free(record);

        if (firstIteration)
        {
            initialPageNum = rid.pageNum;
            firstIteration = false;
        }
        else if (rid.pageNum != initialPageNum)
        {
            rc = rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
            assert(rc == SUCCESS && "Delete record should not fail.");
            break;
        }

        lastRID = rid;
        rids.push_back(rid);
    }
    while (true);

    forwardingRID = lastRID;

    // Allocate our huge record s.t. it's guaranteed to forward.
    string lengthyVarChar (2048, 'b');
    int newSize;
    void *newRecord = calloc(PAGE_SIZE, sizeof(uint8_t));
    prepareRecord_varchar2048(lengthyVarChar, recordDescriptor, newRecord, &newSize); 

    rc = rbfm->updateRecord(fileHandle, recordDescriptor, newRecord, forwardingRID);
    assert(rc == SUCCESS && "Updating record should not fail.");

    void *updatedRecord = calloc(newSize, sizeof(uint8_t));
    rc = rbfm->readRecord(fileHandle, recordDescriptor, forwardingRID, updatedRecord);
    assert(rc == SUCCESS && "Reading updated record should not fail.");

    bool updated_eq_new = memcmp(updatedRecord, newRecord, newSize) == 0 ? true : false;
    assert(updated_eq_new && "Updated record should be equal to the new record.");

    free(updatedRecord);
    free(newRecord);
}

