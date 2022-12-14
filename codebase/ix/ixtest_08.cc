#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "ix.h"
#include "ix_test_util.h"

IndexManager *indexManager;

int testCase_8(const string &indexFileName, const Attribute &attribute)
{
    // Functions tested
    // 1. Create Index File
    // 2. Open Index File
    // 3. Insert entry
    // 4. Scan entries using GE_OP operator and checking if the values returned are correct. **
    // 5. Scan close
    // 6. Close Index File
    // 7. Destroy Index File
    // NOTE: "**" signifies the new functions being tested in this test case.
	cerr << endl << "***** In IX Test Case 8 *****" << endl;

    RID rid;
    IXFileHandle ixfileHandle;
    IX_ScanIterator ix_ScanIterator;
    unsigned numOfTuples = 300;
    unsigned numOfMoreTuples = 100;
    unsigned key;
    int inRidPageNumSum = 0;
    int outRidPageNumSum = 0;
    int value = 5001;

    // create index file
    RC rc = indexManager->createFile(indexFileName);
    assert(rc == success && "indexManager::createFile() should not fail.");

    // open index file
    rc = indexManager->openFile(indexFileName, ixfileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // insert Entries
    for(unsigned i = 1; i <= numOfTuples; i++)
    {
        key = i;
        rid.pageNum = key;
        rid.slotNum = key+1;

        rc = indexManager->insertEntry(ixfileHandle, attribute, &key, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");
    }

    // Insert more entries
    for(unsigned i = value; i < value + numOfMoreTuples; i++)
    {
        key = i;
        rid.pageNum = key;
        rid.slotNum = key+1;

        rc = indexManager->insertEntry(ixfileHandle, attribute, &key, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");

        inRidPageNumSum += rid.pageNum;
    }

    // Scan
    rc = indexManager->scan(ixfileHandle, attribute, &value, NULL, true, true, ix_ScanIterator);
    assert(rc == success && "indexManager::scan() should not fail.");

    // IndexScan iterator
    unsigned count = 0;
    while(ix_ScanIterator.getNextEntry(rid, &key) == success)
    {
    	count++;

        if (rid.pageNum % 100 == 0) {
            cerr << count << " - Returned rid: " << rid.pageNum << " " << rid.slotNum << endl;
        }
        if (rid.pageNum < (unsigned) value || rid.slotNum < (unsigned) value + 1)
        {
            cerr << "Wrong entries output... The test failed" << endl;
            rc = ix_ScanIterator.close();
            rc = indexManager->closeFile(ixfileHandle);
            rc = indexManager->destroyFile(indexFileName);
            return fail;
        }
        outRidPageNumSum += rid.pageNum;
    }

    // Inconsistency check
    if (inRidPageNumSum != outRidPageNumSum)
    {
        cerr << "Wrong entries output... The test failed" << endl;
        rc = ix_ScanIterator.close();
        rc = indexManager->closeFile(ixfileHandle);
        return fail;
    }

    // Close Scan
    rc = ix_ScanIterator.close();
    assert(rc == success && "IX_ScanIterator::close() should not fail.");

    // Close Index
    rc = indexManager->closeFile(ixfileHandle);
    assert(rc == success && "indexManager::closeFile() should not fail.");

    // Destroy Index
    rc = indexManager->destroyFile(indexFileName);
    assert(rc == success && "indexManager::destroyFile() should not fail.");

    return success;

}

int main()
{
    // Global Initialization
    indexManager = IndexManager::instance();

    const string indexFileName = "age_idx";
    Attribute attrAge;
    attrAge.length = 4;
    attrAge.name = "age";
    attrAge.type = TypeInt;

    remove("age_idx");

    RC result = testCase_8(indexFileName, attrAge);
    if (result == success) {
        cerr << "***** IX Test Case 8 finished. The result will be examined. *****" << endl;
        return success;
    } else {
        cerr << "***** [FAIL] IX Test Case 8 failed. *****" << endl;
        return fail;
    }

}

