#include <iostream>
#include <string>
#include <cassert>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <stdexcept>
#include <stdio.h>
#include <fstream>
#include <unordered_map>

#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

using namespace std;

// Check if a file exists
bool FileExists(string fileName)
{
    struct stat stFileInfo;

    if (stat(fileName.c_str(), &stFileInfo) == 0)
        return true;
    else
        return false;
}

// Function to prepare the data in the correct form to be inserted/read
void prepareRecord(const int nameLength, const string &name, const int age, const float height, const int salary, void *buffer, int *recordSize)
{
    int offset = 0;

    memcpy((char *)buffer + offset, &nameLength, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)buffer + offset, name.c_str(), nameLength);
    offset += nameLength;

    memcpy((char *)buffer + offset, &age, sizeof(int));
    offset += sizeof(int);

    memcpy((char *)buffer + offset, &height, sizeof(float));
    offset += sizeof(float);

    memcpy((char *)buffer + offset, &salary, sizeof(int));
    offset += sizeof(int);

    *recordSize = offset;
}

void prepareLargeRecord(const int index, void *buffer, int *size)
{
    int offset = 0;

    // compute the count
    int count = index % 50 + 1;

    // compute the letter
    char text = index % 26 + 97;

    for (int i = 0; i < 10; i++)
    {
        memcpy((char *)buffer + offset, &count, sizeof(int));
        offset += sizeof(int);

        for (int j = 0; j < count; j++)
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

int RBFTest_1(PagedFileManager *pfm)
{
    // Functions Tested:
    // 1. Create File
    cout << "****In RBF Test Case 1****" << endl;

    RC rc;
    string fileName = "test";

    // Create a file named "test"
    rc = pfm->createFile(fileName.c_str());
    assert(rc == success);

    if (FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been created." << endl
             << endl;
    }
    else
    {
        cout << "Failed to create file!" << endl;
        cout << "Test Case 1 Failed!" << endl
             << endl;
        return -1;
    }

    // Create "test" again, should fail
    rc = pfm->createFile(fileName.c_str());
    assert(rc != success);

    cout << "Test Case 1 Passed!" << endl
         << endl;
    return 0;
}

int RBFTest_2(PagedFileManager *pfm)
{
    // Functions Tested:
    // 1. Destroy File
    cout << "****In RBF Test Case 2****" << endl;

    RC rc;
    string fileName = "test";

    rc = pfm->destroyFile(fileName.c_str());
    assert(rc == success);

    if (!FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been destroyed." << endl
             << endl;
        cout << "Test Case 2 Passed!" << endl
             << endl;
        return 0;
    }
    else
    {
        cout << "Failed to destroy file!" << endl;
        cout << "Test Case 2 Failed!" << endl
             << endl;
        return -1;
    }
}

int RBFTest_3(PagedFileManager *pfm)
{
    // Functions Tested:
    // 1. Create File
    // 2. Open File
    // 3. Get Number Of Pages
    // 4. Close File
    cout << "****In RBF Test Case 3****" << endl;
    cout << "test1" << endl;
    RC rc;
    string fileName = "test_1";

    // Create a file named "test_1"
    rc = pfm->createFile(fileName.c_str());
    assert(rc == success);

    if (FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been created." << endl;
    }
    else
    {
        cout << "Failed to create file!" << endl;
        cout << "Test Case 3 Failed!" << endl
             << endl;
        return -1;
    }

    // Open the file "test_1"
    FileHandle fileHandle;
    rc = pfm->openFile(fileName.c_str(), fileHandle);
    assert(rc == success);

    // Get the number of pages in the test file
    unsigned count = fileHandle.getNumberOfPages();
    assert(count == (unsigned)0);

    // Close the file "test_1"
    rc = pfm->closeFile(fileHandle);
    assert(rc == success);

    cout << "Test Case 3 Passed!" << endl
         << endl;

    return 0;
}

int RBFTest_4(PagedFileManager *pfm)
{
    // Functions Tested:
    // 1. Open File
    // 2. Append Page
    // 3. Get Number Of Pages
    // 3. Close File
    cout << "****In RBF Test Case 4****" << endl;

    RC rc;
    string fileName = "test_1";

    // Open the file "test_1"
    FileHandle fileHandle;
    rc = pfm->openFile(fileName.c_str(), fileHandle);
    assert(rc == success);

    // Append the first page
    void *data = malloc(PAGE_SIZE);
    for (unsigned i = 0; i < PAGE_SIZE; i++)
    {
        *((char *)data + i) = i % 94 + 32;
    }
    rc = fileHandle.appendPage(data);
    assert(rc == success);

    // Get the number of pages
    unsigned count = fileHandle.getNumberOfPages();
    assert(count == (unsigned)1);

    // Close the file "test_1"
    rc = pfm->closeFile(fileHandle);
    assert(rc == success);

    free(data);

    cout << "Test Case 4 Passed!" << endl
         << endl;

    return 0;
}

int RBFTest_5(PagedFileManager *pfm)
{
    // Functions Tested:
    // 1. Open File
    // 2. Read Page
    // 3. Close File
    cout << "****In RBF Test Case 5****" << endl;

    RC rc;
    string fileName = "test_1";

    // Open the file "test_1"
    FileHandle fileHandle;
    rc = pfm->openFile(fileName.c_str(), fileHandle);
    assert(rc == success);

    // Read the first page
    void *buffer = malloc(PAGE_SIZE);
    rc = fileHandle.readPage(0, buffer);
    assert(rc == success);

    // Check the integrity of the page
    void *data = malloc(PAGE_SIZE);
    for (unsigned i = 0; i < PAGE_SIZE; i++)
    {
        *((char *)data + i) = i % 94 + 32;
    }
    rc = memcmp(data, buffer, PAGE_SIZE);
    assert(rc == success);

    // Close the file "test_1"
    rc = pfm->closeFile(fileHandle);
    assert(rc == success);

    free(data);
    free(buffer);

    cout << "Test Case 5 Passed!" << endl
         << endl;

    return 0;
}

int RBFTest_6(PagedFileManager *pfm)
{
    // Functions Tested:
    // 1. Open File
    // 2. Write Page
    // 3. Read Page
    // 4. Close File
    // 5. Destroy File
    cout << "****In RBF Test Case 6****" << endl;

    RC rc;
    string fileName = "test_1";

    // Open the file "test_1"
    FileHandle fileHandle;
    rc = pfm->openFile(fileName.c_str(), fileHandle);
    assert(rc == success);

    // Update the first page
    void *data = malloc(PAGE_SIZE);
    for (unsigned i = 0; i < PAGE_SIZE; i++)
    {
        *((char *)data + i) = i % 10 + 32;
    }
    rc = fileHandle.writePage(0, data);
    assert(rc == success);

    // Read the page
    void *buffer = malloc(PAGE_SIZE);
    rc = fileHandle.readPage(0, buffer);
    assert(rc == success);

    // Check the integrity
    rc = memcmp(data, buffer, PAGE_SIZE);
    assert(rc == success);

    // Close the file "test_1"
    rc = pfm->closeFile(fileHandle);
    assert(rc == success);

    free(data);
    free(buffer);

    // Destroy File
    rc = pfm->destroyFile(fileName.c_str());
    assert(rc == success);

    if (!FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been destroyed." << endl;
        cout << "Test Case 6 Passed!" << endl
             << endl;
        return 0;
    }
    else
    {
        cout << "Failed to destroy file!" << endl;
        cout << "Test Case 6 Failed!" << endl
             << endl;
        return -1;
    }
}

int RBFTest_7(PagedFileManager *pfm)
{
    // Functions Tested:
    // 1. Create File
    // 2. Open File
    // 3. Append Page
    // 4. Get Number Of Pages
    // 5. Read Page
    // 6. Write Page
    // 7. Close File
    // 8. Destroy File
    cout << "****In RBF Test Case 7****" << endl;

    RC rc;
    string fileName = "test_2";

    // Create the file named "test_2"
    rc = pfm->createFile(fileName.c_str());
    assert(rc == success);

    if (FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been created." << endl;
    }
    else
    {
        cout << "Failed to create file!" << endl;
        cout << "Test Case 7 Failed!" << endl
             << endl;
        return -1;
    }

    // Open the file "test_2"
    FileHandle fileHandle;
    rc = pfm->openFile(fileName.c_str(), fileHandle);
    assert(rc == success);

    // Append 50 pages
    void *data = malloc(PAGE_SIZE);
    for (unsigned j = 0; j < 50; j++)
    {
        for (unsigned i = 0; i < PAGE_SIZE; i++)
        {
            *((char *)data + i) = i % (j + 1) + 32;
        }
        rc = fileHandle.appendPage(data);
        assert(rc == success);
    }
    cout << "50 Pages have been successfully appended!" << endl;

    // Get the number of pages
    unsigned count = fileHandle.getNumberOfPages();
    assert(count == (unsigned)50);

    // Read the 25th page and check integrity
    void *buffer = malloc(PAGE_SIZE);
    rc = fileHandle.readPage(24, buffer);
    assert(rc == success);

    for (unsigned i = 0; i < PAGE_SIZE; i++)
    {
        *((char *)data + i) = i % 25 + 32;
    }
    rc = memcmp(buffer, data, PAGE_SIZE);
    assert(rc == success);
    cout << "The data in 25th page is correct!" << endl;

    // Update the 25th page
    for (unsigned i = 0; i < PAGE_SIZE; i++)
    {
        *((char *)data + i) = i % 60 + 32;
    }
    rc = fileHandle.writePage(24, data);
    assert(rc == success);

    // Read the 25th page and check integrity
    rc = fileHandle.readPage(24, buffer);
    assert(rc == success);

    rc = memcmp(buffer, data, PAGE_SIZE);
    assert(rc == success);

    // Close the file "test_2"
    rc = pfm->closeFile(fileHandle);
    assert(rc == success);

    // Destroy File
    rc = pfm->destroyFile(fileName.c_str());
    assert(rc == success);

    free(data);
    free(buffer);

    if (!FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been destroyed." << endl;
        cout << "Test Case 7 Passed!" << endl
             << endl;
        return 0;
    }
    else
    {
        cout << "Failed to destroy file!" << endl;
        cout << "Test Case 7 Failed!" << endl
             << endl;
        return -1;
    }
}

int RBFTest_8(RecordBasedFileManager *rbfm) 
{
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Record
    // 4. Read Record
    // 5. Close Record-Based File
    // 6. Destroy Record-Based File
    cout << endl << "***** In RBF Test Case 8 *****" << endl;
   
    RC rc;
    string fileName = "test8";

    // Create a file named "test8"
    rc = rbfm->createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file should not fail.");

    // Open the file "test8"
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");
      
    RID rid; 
    int recordSize = 0;
    void *record = malloc(100);
    void *returnedData = malloc(100);

    vector<Attribute> recordDescriptor;
    createRecordDescriptor(recordDescriptor);
    
    // Initialize a NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    // Insert a record into a file and print the record
    prepareRecord(recordDescriptor.size(), nullsIndicator, 8, "Anteater", 25, 177.8, 6200, record, &recordSize);
    cout << endl << "Inserting Data:" << endl;
    rbfm->printRecord(recordDescriptor, record);
    
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
    assert(rc == success && "Inserting a record should not fail.");
    
    // Given the rid, read the record from file
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData);
    assert(rc == success && "Reading a record should not fail.");

    cout << endl << "Returned Data:" << endl;
    rbfm->printRecord(recordDescriptor, returnedData);

    // Compare whether the two memory blocks are the same
    if(memcmp(record, returnedData, recordSize) != 0)
    {
        cout << "[FAIL] Test Case 8 Failed!" << endl << endl;
        free(record);
        free(returnedData);
        return -1;
    }
    
    cout << endl;

    // Close the file "test8"
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    // Destroy the file
    rc = rbfm->destroyFile(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    rc = destroyFileShouldSucceed(fileName);
    assert(rc == success  && "Destroying the file should not fail.");
    
    free(record);
    free(returnedData);

    cout << "RBF Test Case 8 Finished! The result will be examined." << endl << endl;
    
    free(nullsIndicator);
    return 0;
}

int RBFTest_9(RecordBasedFileManager *rbfm, vector<RID> &rids, vector<int> &sizes) {
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Multiple Records
    // 4. Close Record-Based File
    cout << endl << "***** In RBF Test Case 9 *****" << endl;
   
    RC rc;
    string fileName = "test9";

    // Create a file named "test9"
    rc = rbfm->createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file failed.");

    // Open the file "test9"
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");

    RID rid; 
    void *record = malloc(1000);
    int numRecords = 2000;

    vector<Attribute> recordDescriptor;
    createLargeRecordDescriptor(recordDescriptor);

    for(unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        cout << "Attr Name: " << recordDescriptor[i].name << " Attr Type: " << (AttrType)recordDescriptor[i].type << " Attr Len: " << recordDescriptor[i].length << endl;
    }
    cout << endl;
    
    // NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    // Insert 2000 records into file
    for(int i = 0; i < numRecords; i++)
    {
        // Test insert Record
        int size = 0;
        memset(record, 0, 1000);
        prepareLargeRecord(recordDescriptor.size(), nullsIndicator, i, record, &size);

        rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
        assert(rc == success && "Inserting a record should not fail.");

        rids.push_back(rid);
        sizes.push_back(size);        
    }
    // Close the file "test9"
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    free(record);
    
    
    // Write RIDs to the disk. Do not use this code in your codebase. This is not a PAGE-BASED operation - for the test purpose only.
    ofstream ridsFile("test9rids", ios::out | ios::trunc | ios::binary);

    if (ridsFile.is_open()) {
        ridsFile.seekp(0, ios::beg);
        for (int i = 0; i < numRecords; i++) {
            ridsFile.write(reinterpret_cast<const char*>(&rids[i].pageNum),
                    sizeof(unsigned));
            ridsFile.write(reinterpret_cast<const char*>(&rids[i].slotNum),
                    sizeof(unsigned));
            if (i % 1000 == 0) {
                cout << "RID #" << i << ": " << rids[i].pageNum << ", "
                        << rids[i].slotNum << endl;
            }
        }
        ridsFile.close();
    }

    // Write sizes vector to the disk. Do not use this code in your codebase. This is not a PAGE-BASED operation - for the test purpose only.
    ofstream sizesFile("test9sizes", ios::out | ios::trunc | ios::binary);

    if (sizesFile.is_open()) {
        sizesFile.seekp(0, ios::beg);
        for (int i = 0; i < numRecords; i++) {
            sizesFile.write(reinterpret_cast<const char*>(&sizes[i]),sizeof(int));
            if (i % 1000 == 0) {
                cout << "Sizes #" << i << ": " << sizes[i] << endl;
            }
        }
        sizesFile.close();
    }
        
    cout << "RBF Test Case 9 Finished! The result will be examined." << endl << endl;

    free(nullsIndicator);
    return 0;
}

int RBFTest_10(RecordBasedFileManager *rbfm) {
    // Functions tested
    // 1. Open Record-Based File
    // 2. Read Multiple Records
    // 3. Close Record-Based File
    // 4. Destroy Record-Based File
    cout << endl << "***** In RBF Test Case 10 *****" << endl;
   
    RC rc;
    string fileName = "test9";

    // Open the file "test9"
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");
    
    int numRecords = 2000;
    void *record = malloc(1000);
    void *returnedData = malloc(1000);

    vector<Attribute> recordDescriptor;
    createLargeRecordDescriptor(recordDescriptor);

    vector<RID> rids;
    vector<int> sizes;
    RID tempRID;

    // Read rids from the disk - do not use this code in your codebase. This is not a PAGE-BASED operation - for the test purpose only.
    ifstream ridsFileRead("test9rids", ios::in | ios::binary);

    unsigned pageNum;
    unsigned slotNum;

    if (ridsFileRead.is_open()) {
        ridsFileRead.seekg(0,ios::beg);
        for (int i = 0; i < numRecords; i++) {
            ridsFileRead.read(reinterpret_cast<char*>(&pageNum), sizeof(unsigned));
            ridsFileRead.read(reinterpret_cast<char*>(&slotNum), sizeof(unsigned));
            if (i % 1000 == 0) {
                cout << "loaded RID #" << i << ": " << pageNum << ", " << slotNum << endl;
            }
            tempRID.pageNum = pageNum;
            tempRID.slotNum = slotNum;
            rids.push_back(tempRID);
        }
        ridsFileRead.close();
    }

    assert(rids.size() == (unsigned) numRecords && "Reading records should not fail.");

    // Read sizes vector from the disk - do not use this code in your codebase. This is not a PAGE-BASED operation - for the test purpose only.
    ifstream sizesFileRead("test9sizes", ios::in | ios::binary);

    int tempSize;
    
    if (sizesFileRead.is_open()) {
        sizesFileRead.seekg(0,ios::beg);
        for (int i = 0; i < numRecords; i++) {
            sizesFileRead.read(reinterpret_cast<char*>(&tempSize), sizeof(int));
            if (i % 1000 == 0) {
                cout << "loaded Sizes #" << i << ": " << tempSize << endl;
            }
            sizes.push_back(tempSize);
        }
        sizesFileRead.close();
    }

    assert(sizes.size() == (unsigned) numRecords && "Reading records should not fail.");

    // NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    for(int i = 0; i < numRecords; i++)
    {
        memset(record, 0, 1000);
        memset(returnedData, 0, 1000);
        rc = rbfm->readRecord(fileHandle, recordDescriptor, rids[i], returnedData);
        assert(rc == success && "Reading a record should not fail.");
        
        if (i % 1000 == 0) {
            cout << endl << "Returned Data:" << endl;
            rbfm->printRecord(recordDescriptor, returnedData);
        }

        int size = 0;
        prepareLargeRecord(recordDescriptor.size(), nullsIndicator, i, record, &size);
        if(memcmp(returnedData, record, sizes[i]) != 0)
        {
            cout << "[FAIL] Test Case 10 Failed!" << endl << endl;
            free(record);
            free(returnedData);
            return -1;
        }
    }
    
    cout << endl;

    // Close the file "test9"
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");
    
    rc = rbfm->destroyFile(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    rc = destroyFileShouldSucceed(fileName);
    assert(rc == success  && "Destroying the file should not fail.");

    free(record);
    free(returnedData);

    cout << "RBF Test Case 10 Finished! The result will be examined." << endl << endl;

    remove("test9sizes");
    remove("test9rids");
    
    free(nullsIndicator);
    return 0;
}

int RBFTest_11() {
    cout << endl << "***** In RBF Test Case 11 *****" << endl;
    SlotDirectoryRecordEntry recordEntry;
    recordEntry.offset = 0;

    bool rc;

    recordEntry.length = 0;
    rc = isSlotForwarding(recordEntry);
    assert(rc == false && "Slots should not forward with a cleared MSB.");

    recordEntry.length = -1;
    rc = isSlotForwarding(recordEntry);
    assert(rc == true && "Slots should forward with a set MSB.");

    markSlotAsTerminal(recordEntry);
    rc = isSlotForwarding(recordEntry);
    assert(rc == false && "Failed to mark slot as terminal.");

    markSlotAsForwarding(recordEntry);
    rc = isSlotForwarding(recordEntry);
    assert(rc == true && "Failed to mark slot as forwarding.");

    cout << "RBF Test Case 11 Finished! Slot forwarding utilities are correct." << endl << endl;
    return 0;
}

int RBFTest_12(RecordBasedFileManager *rbfm, int recordToDelete) 
{
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Record
    // 4. Delete Record ***
    // 4. Read Record
    // 5. Close Record-Based File
    // 6. Destroy Record-Based File
    cout << endl << "***** In RBF Test Case 12 (deleting record " << recordToDelete << ") *****" << endl;
   
    RC rc;
    string fileName = "test12";

    // Create the file.
    remove("test12");
    rc = rbfm->createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file failed.");

    // Open the file.
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");

    vector<Attribute> recordDescriptor;
    createLargeRecordDescriptor(recordDescriptor);

    // NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    RID rid; 
    RID *deletedRID = nullptr;
    unordered_map<RID, void *, RIDHasher> ridsToRecord;
    unordered_map<RID, int, RIDHasher> ridsToSize;

    int numRecords = 3;
    for(int i = 0; i < numRecords; i++)
    {
        int size = 0;
        void *record = calloc(1000, sizeof(uint8_t));
        prepareLargeRecord(recordDescriptor.size(), nullsIndicator, i, record, &size);

        rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
        assert(rc == success && "Inserting a record should not fail.");

        ridsToRecord[rid] = record;
        ridsToSize[rid] = size;

        if (i == recordToDelete) {
            deletedRID = (RID *) malloc(sizeof(RID));
            *deletedRID = rid;
        }
    }

    assert(deletedRID != nullptr);

    rc = rbfm->deleteRecord(fileHandle, recordDescriptor, *deletedRID);
    if (rc == RBFM_SLOT_DN_EXIST && recordToDelete >= numRecords)
        return 0;
    assert(rc == success && "Deleting a record should not fail.");

    // Go through all of our records and try to find them in the file.
    // For each match, remove it from our map.
    // At the end, we should have a single record in our map (the one we deleted on the page!).
    for (auto it = ridsToRecord.cbegin(); it != ridsToRecord.cend();)
    {
        rid = it->first;
        void *record = calloc(1000, sizeof(char));

        if (rid == *deletedRID) {
            free(record);
            it++;
            continue;
        }

        rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, record);

        if (rid == *deletedRID)
        {
            free(record);
            assert(rc == RBFM_SLOT_DN_EXIST && "Failed to error on reading from empty slot of deleted record.");
            it++;
            continue;
        }

        assert(rc == SUCCESS && "Failed to read an inserted record.");

        bool recordsMatch;
        recordsMatch = memcmp(record, ridsToRecord[rid], ridsToSize[rid]) == 0;
        assert(recordsMatch && "Some record was modified after deletion.");

        free(record);
        free(ridsToRecord[rid]);
        it = ridsToRecord.erase(it);
        ridsToSize.erase(rid);
    }

    // We built up our map on inserting and then tore it down on reading records.
    // There should only be one record left.
    assert(!ridsToRecord.empty() && "No records were deleted."); // We tore down EVERY record.
    assert(ridsToRecord.size() == 1 && "More than one record was deleted."); // We didn't tear down enough.

    RID remainingRID = ridsToRecord.begin()->first;
    assert(remainingRID == *deletedRID && "Unexpected RID was deleted");

    // Close the file.
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");
    remove(fileName.c_str());

    free(deletedRID);
    free(ridsToRecord[remainingRID]);
    free(nullsIndicator);
    cout << "RBF Test Case 12 Finished (deleting record " << recordToDelete << ")" << endl << endl;
    return 0;
}

int RBFTest_12(RecordBasedFileManager *rbfm)
{
    RBFTest_12(rbfm, 0); // Adjacent to page boundary.
    RBFTest_12(rbfm, 1); // In between two records.
    RBFTest_12(rbfm, 2); // Adjacent to free space.
    return 0;
}

// Insert records, delete a record, then insert another record.
// The last record that was inserted should fill the slot from the deleted record.
int RBFTest_13(RecordBasedFileManager *rbfm, int recordToDelete)
{
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Record ***
    // 4. Delete Record ***
    // 4. Read Record
    // 5. Close Record-Based File
    // 6. Destroy Record-Based File
    cout << endl << "***** In RBF Test Case 13 (deleting record " << recordToDelete << ") *****" << endl;
   
    RC rc;
    string fileName = "test13";

    // Create the file.
    rc = rbfm->createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file failed.");

    // Open the file.
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");

    vector<Attribute> recordDescriptor;
    createLargeRecordDescriptor(recordDescriptor);

    // NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    RID rid; 
    RID *deletedRID = nullptr;
    unordered_map<RID, void *, RIDHasher> ridsToRecord;
    unordered_map<RID, int, RIDHasher> ridsToSize;

    int numRecords = 3;
    int i;
    for(i = 0; i < numRecords; i++)
    {
        int size = 0;
        void *record = calloc(1000, sizeof(uint8_t));
        prepareLargeRecord(recordDescriptor.size(), nullsIndicator, i, record, &size);

        rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
        assert(rc == success && "Inserting a record should not fail.");

        ridsToRecord[rid] = record;
        ridsToSize[rid] = size;

        if (i == recordToDelete)
        {
            deletedRID = (RID *) malloc(sizeof(RID));
            *deletedRID = rid;
        }
    }

    assert(deletedRID != nullptr);

    rc = rbfm->deleteRecord(fileHandle, recordDescriptor, *deletedRID);
    if (rc == RBFM_SLOT_DN_EXIST && recordToDelete >= numRecords)
        return 0;
    assert(rc == success && "Deleting a record should not fail.");

    // Insert a new record;
    int newSize = 0;
    void *newRecord = calloc(1000, sizeof(uint8_t));
    prepareLargeRecord(recordDescriptor.size(), nullsIndicator, i, newRecord, &newSize);

    RID newRID;
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, newRecord, newRID);
    assert(rc == success && "Inserting a record should not fail.");
    assert(newRID == *deletedRID && "New record should have same RID as the one that was deleted.");

    // Go through all of our records and try to find them in the file.
    // For each match, remove it from our map.
    // At the end, we should have two records in our map (the one we deleted on the page, and the new record).
    for (auto it = ridsToRecord.cbegin(); it != ridsToRecord.cend(); it = ridsToRecord.erase(it))
    {
        rid = it->first;
        void *record = calloc(1000, sizeof(char));

        rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, record);
        assert(rc == SUCCESS && "Failed to read an inserted record.");

        bool recordsMatch;
        recordsMatch = memcmp(record, ridsToRecord[rid], ridsToSize[rid]) == 0;
        if (rid == newRID)
        {
            assert(!recordsMatch && "Some record was modified after deletion.");
        }
        else
        {
            assert(recordsMatch && "Some record was modified after deletion.");
        }

        free(record);
        free(ridsToRecord[rid]);
        ridsToSize.erase(rid);
    }

    // We built up our map on inserting and then tore it down on reading records.
    // There should be no records left because our new record had the same RID as the deleted record.
    assert(ridsToRecord.empty() && "No records were deleted.");

    // Close the file.
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");
    remove(fileName.c_str());

    free(newRecord);
    free(deletedRID);
    free(nullsIndicator);
    cout << "RBF Test Case 13 Finished (deleting record " << recordToDelete << ")" << endl << endl;
    return 0;
    
}

int RBFTest_13(RecordBasedFileManager *rbfm)
{
    RBFTest_13(rbfm, 0); // Adjacent to page boundary.
    RBFTest_13(rbfm, 1); // In between two records.
    RBFTest_13(rbfm, 2); // Adjacent to free space.
    return 0;
}

// Updating a record.  Initial RID must be permanent while
// still retrieving the record, regardless of its location in storage.
// Note that a record should only be forwarded when it updates and gains length.
// If a record stays the same length or is shorter, it should stay on the same page.
namespace RBFTest_14
{
    string fileName = "test14";
    FileHandle fileHandle;

    // Record is initially unforwarded.
    namespace Unforwarded
    {
        void beforeEach(RecordBasedFileManager *rbfm)
        {
            remove(fileName.c_str());

            // Create the file.
            auto rc = rbfm->createFile(fileName);
            assert(rc == success && "Creating the file should not fail.");

            rc = createFileShouldSucceed(fileName);
            assert(rc == success && "Creating the file failed.");

            // Open the file.
            rc = rbfm->openFile(fileName, fileHandle);
            assert(rc == success && "Opening the file should not fail.");
        }

        void afterEach(RecordBasedFileManager *rbfm)
        {
            // Close the file.
            auto rc = rbfm->closeFile(fileHandle);
            assert(rc == success && "Closing the file should not fail.");
            remove(fileName.c_str());
        }

        // Updated record remains on same page (primary page).
        int toUnforwarded_shrinkSize(RecordBasedFileManager *rbfm)
        {
            cout << "**** In RBF Test Case 14 (unforwarded to unforwarded, shrink) ****" << endl;
            int index = 0;
            vector<Attribute> recordDescriptor;
            vector<RID> rids;
            vector<int> sizes;
            void *page = createUnforwardedPage(rbfm, fileHandle, index, recordDescriptor, rids, sizes);
            assert(page != nullptr && "Unforwarded page creation should not fail.");

            /* I'm not sure how prepareLargeRecord calculates the size. From inserting records,
             * it seems that the size is always increasing, but it may wrap around at some point.
             * To be safe, we just get the min and max records.
             */
            auto minSize = INT_MAX;
            auto maxSize = INT_MIN;
            auto argminSize = -1; // Index of min.
            auto argmaxSize = -1; // Index of max.
            for (auto it = sizes.begin(); it != sizes.end(); ++it) {
                    auto size = *it;
                    int index = distance(sizes.begin(), it);
                    if (size <= minSize)
                    {
                        minSize = size;
                        argminSize = index;
                    }
                    if (size >= maxSize)
                    {
                        maxSize = size;
                        argmaxSize = index;
                    }
            }
            assert(argminSize != -1);  // We must have found some min.
            assert(argmaxSize != -1);  // We must have found some max.
            assert(minSize != maxSize); // And we need a record that is larger than the min (to shrink it).
            
            // Load smallest record.
            void *minRecord = calloc(sizes[argminSize], sizeof(uint8_t));
            auto rc = rbfm->readRecord(fileHandle, recordDescriptor, rids[argminSize], minRecord);
            assert(rc == SUCCESS && "Reading the min record should not fail.");
            cout << "Min record found ("; rids[argminSize].print(); cout << "):" << endl;
            rbfm->printRecord(recordDescriptor, minRecord);

            // Load largest record.
            void *maxRecord = calloc(sizes[argmaxSize], sizeof(uint8_t));
            rc = rbfm->readRecord(fileHandle, recordDescriptor, rids[argmaxSize], maxRecord);
            assert(rc == SUCCESS && "Reading the max record should not fail.");
            cout << "Max record found ("; rids[argmaxSize].print(); cout << "):" << endl;
            rbfm->printRecord(recordDescriptor, maxRecord);

            // Replace the largest record with a copy of the smallest record.
            rc = rbfm->updateRecord(fileHandle, recordDescriptor, minRecord, rids[argmaxSize]);
            assert(rc == SUCCESS && "Update record should not fail when shrinking size.");

            // Load updated record.
            void *updatedRecord = calloc(sizes[argminSize], sizeof(uint8_t));
            rc = rbfm->readRecord(fileHandle, recordDescriptor, rids[argmaxSize], updatedRecord);
            assert(rc == SUCCESS && "Reading the updated record should not fail.");
            cout << "Updated record found ("; rids[argmaxSize].print(); cout << "):" << endl;
            rbfm->printRecord(recordDescriptor, updatedRecord);

            // Updated record SHOULD NOT BE MAX record.
            bool updated_eq_max = memcmp(updatedRecord, maxRecord, sizes[argminSize]) == 0 ? true : false;
            assert(!updated_eq_max && "Updated record should not be the max record.");

            // Updated record SHOULD BE MIN record.
            bool updated_eq_min = memcmp(updatedRecord, minRecord, sizes[argminSize]) == 0 ? true : false;
            assert(updated_eq_min && "Updated record should be the min record.");

            free(updatedRecord);
            free(maxRecord);
            free(minRecord);
            free(page);
            cout << "RBF Test Case 14 Finished (unforwarded to unforwarded, shrink)" << endl << endl;
            return success;
        }

        int toUnforwarded_constSize(RecordBasedFileManager *rbfm)
        {
            cout << "**** In RBF Test Case 14 (unforwarded to unforwarded, const) ****" << endl;
            int index = 0;
            vector<Attribute> recordDescriptor;
            vector<RID> rids;
            vector<int> sizes;
            void *page = createUnforwardedPage(rbfm, fileHandle, index, recordDescriptor, rids, sizes);
            assert(page != nullptr && "Unforwarded page creation should not fail.");

            // Load some record.
            int originalIndex = index - 1; // Index was setup for the next record.  We want the current record.
            RID originalRID = rids.back();
            int originalSize = sizes.back();
            void *originalRecord = calloc(originalSize, sizeof(uint8_t));

            auto rc = rbfm->readRecord(fileHandle, recordDescriptor, originalRID, originalRecord);
            assert(rc == SUCCESS && "Reading record should not fail.");
            cout << "Record found ("; originalRID.print(); cout << "):" << endl;
            rbfm->printRecord(recordDescriptor, originalRecord);

            // Prepare updated version of loaded record.
            void *newRecord = calloc(originalSize, sizeof(uint8_t));
            memcpy(newRecord, originalRecord, originalSize);
            prepareLargeRecord_incrementIntegers(originalIndex, recordDescriptor, newRecord, rbfm);
            cout << "Updating record ("; originalRID.print(); cout << "):" << endl;
            rbfm->printRecord(recordDescriptor, newRecord);

            rc = rbfm->updateRecord(fileHandle, recordDescriptor, newRecord, originalRID);
            assert(rc == SUCCESS && "Update record should not fail when shrinking size.");

            // Load updated record.
            void *updatedRecord = calloc(originalSize, sizeof(uint8_t));
            rc = rbfm->readRecord(fileHandle, recordDescriptor, originalRID, updatedRecord);
            assert(rc == SUCCESS && "Reading the updated record should not fail.");
            cout << "Updated record found ("; originalRID.print(); cout << "):" << endl;
            rbfm->printRecord(recordDescriptor, updatedRecord);

            // Updated record SHOULD BE new record.
            bool updated_eq_new = memcmp(updatedRecord, newRecord, originalSize) == 0 ? true : false;
            assert(updated_eq_new && "Updated record should be the new record.");

            free(updatedRecord);
            free(newRecord);
            free(originalRecord);
            free(page);
            cout << "RBF Test Case 14 Finished (unforwarded to unforwarded, const)" << endl << endl;
            return success;
        }

        // Updated record may increase in size but remain on same page (if there's space).
        int toUnforwarded_incrSize(RecordBasedFileManager *rbfm)
        {
            cout << "**** In RBF Test Case 14 (unforwarded to unforwarded, incr) ****" << endl;
            int index = 0;
            vector<Attribute> recordDescriptor;
            vector<RID> rids;
            vector<int> sizes;
            void *page = createUnforwardedPage(rbfm, fileHandle, index, recordDescriptor, rids, sizes);
            assert(page != nullptr && "Unforwarded page creation should not fail.");

            /* I'm not sure how prepareLargeRecord calculates the size. From inserting records,
             * it seems that the size is always increasing, but it may wrap around at some point.
             * To be safe, we just get the min and max records.
             */
            auto minSize = INT_MAX;
            auto maxSize = INT_MIN;
            auto argminSize = -1; // Index of min.
            auto argmaxSize = -1; // Index of max.
            for (auto it = sizes.begin(); it != sizes.end(); ++it) {
                    auto size = *it;
                    int index = distance(sizes.begin(), it);
                    if (size <= minSize)
                    {
                        minSize = size;
                        argminSize = index;
                    }
                    if (size >= maxSize)
                    {
                        maxSize = size;
                        argmaxSize = index;
                    }
            }
            assert(argminSize != -1);  // We must have found some min.
            assert(argmaxSize != -1);  // We must have found some max.
            assert(minSize != maxSize); // And we need a record that is larger than the min (to shrink it).
            
            // Load smallest record.
            void *minRecord = calloc(sizes[argminSize], sizeof(uint8_t));
            auto rc = rbfm->readRecord(fileHandle, recordDescriptor, rids[argminSize], minRecord);
            assert(rc == SUCCESS && "Reading the min record should not fail.");
            cout << "Min record found ("; rids[argminSize].print(); cout << "):" << endl;
            rbfm->printRecord(recordDescriptor, minRecord);

            // Load largest record.
            void *maxRecord = calloc(sizes[argmaxSize], sizeof(uint8_t));
            rc = rbfm->readRecord(fileHandle, recordDescriptor, rids[argmaxSize], maxRecord);
            assert(rc == SUCCESS && "Reading the max record should not fail.");
            cout << "Max record found ("; rids[argmaxSize].print(); cout << "):" << endl;
            rbfm->printRecord(recordDescriptor, maxRecord);


            // Replace the smallest record with a copy of the largest record.
            // Guarantee that there is free space for the updatedRecord.
            rc = rbfm->deleteRecord(fileHandle, recordDescriptor, rids[argmaxSize]);
            assert(rc == SUCCESS && "Deleting largest record should not fail.");
            rc = rbfm->updateRecord(fileHandle, recordDescriptor, maxRecord, rids[argminSize]);
            assert(rc == SUCCESS && "Update record should not fail when shrinking size.");

            // Load updated record.
            void *updatedRecord = calloc(sizes[argmaxSize], sizeof(uint8_t));
            rc = rbfm->readRecord(fileHandle, recordDescriptor, rids[argminSize], updatedRecord);
            assert(rc == SUCCESS && "Reading the updated record should not fail.");
            cout << "Updated record found ("; rids[argminSize].print(); cout << "):" << endl;
            rbfm->printRecord(recordDescriptor, updatedRecord);

            // Updated record SHOULD BE MAX record.
            bool updated_eq_max = memcmp(updatedRecord, maxRecord, sizes[argminSize]) == 0 ? true : false;
            assert(updated_eq_max && "Updated record should be the max record.");

            // Updated record SHOULD NOT BE MIN record.
            bool updated_eq_min = memcmp(updatedRecord, minRecord, sizes[argminSize]) == 0 ? true : false;
            assert(!updated_eq_min && "Updated record should not be the min record.");

            free(updatedRecord);
            free(maxRecord);
            free(minRecord);
            free(page);
            cout << "RBF Test Case 14 Finished (unforwarded to unforwarded, incr)" << endl << endl;
            return success;
        }

        // Updated record is now forwarded.
        // The way we set this up is pretty brute force...
        // createUnforwardedPage calls prepareLargeRecord where records have somewhere around 175-250 bytes in size.
        // So we update a record to be > 2048 bytes, forcing it onto another page.
        // updateRecord() should then properly forward the original RID to the new/updated record.
        int toForwarded(RecordBasedFileManager *rbfm)
        {
            cout << "**** In RBF Test Case 14 (unforwarded to forwarded) ****" << endl;
            RID forwardingRID;
            vector<RID> rids;
            vector<Attribute> recordDescriptor;
            createForwardingPages(rbfm, fileHandle, forwardingRID, recordDescriptor, rids);
            cout << "RBF Test Case 14 Finished (unforwarded to forwarded)" << endl << endl;
            return success;
        }

        int runAll(RecordBasedFileManager *rbfm)
        {
            vector<int (*)(RecordBasedFileManager *)> fs
            {
                toUnforwarded_shrinkSize,
                toUnforwarded_constSize,
                toUnforwarded_incrSize,
                toForwarded
            };

            for (auto f : fs)
            {
                beforeEach(rbfm);
                auto rc = f(rbfm);
                assert(rc == success);
                afterEach(rbfm);
            }
            return success;
        }
    }

    // Record is initially forwarded.
    namespace Forwarded
    {
        RID forwardingRID;
        vector<RID> rids;
        vector<Attribute> recordDescriptor;
        void beforeEach(RecordBasedFileManager *rbfm)
        {
            remove(fileName.c_str());

            // Create the file.
            auto rc = rbfm->createFile(fileName);
            assert(rc == success && "Creating the file should not fail.");

            rc = createFileShouldSucceed(fileName);
            assert(rc == success && "Creating the file failed.");

            // Open the file.
            rc = rbfm->openFile(fileName, fileHandle);
            assert(rc == success && "Opening the file should not fail.");

            createForwardingPages(rbfm, fileHandle, forwardingRID, recordDescriptor, rids);
        }

        void afterEach(RecordBasedFileManager *rbfm)
        {
            // Close the file.
            auto rc = rbfm->closeFile(fileHandle);
            assert(rc == success && "Closing the file should not fail.");
            remove(fileName.c_str());
        }

        int toSamePage(RecordBasedFileManager *rbfm)
        {
            cout << "**** In RBF Test Case 14 (forwarded, updated record on same page as before) ****" << endl;

            // Allocate our huge record s.t. it's guaranteed to forward.
            string lengthyVarChar (2048, 'c');
            int newSize;
            void *newRecord = calloc(PAGE_SIZE, sizeof(uint8_t));
            prepareRecord_varchar2048(lengthyVarChar, recordDescriptor, newRecord, &newSize); 

            auto rc = rbfm->updateRecord(fileHandle, recordDescriptor, newRecord, forwardingRID);
            assert(rc == SUCCESS && "Updating record should not fail.");

            void *updatedRecord = calloc(newSize, sizeof(uint8_t));
            rc = rbfm->readRecord(fileHandle, recordDescriptor, forwardingRID, updatedRecord);
            assert(rc == SUCCESS && "Reading updated record should not fail.");

            bool updated_eq_new = memcmp(updatedRecord, newRecord, newSize) == 0 ? true : false;
            assert(updated_eq_new && "Updated record should be equal to the new record.");

            cout << "RBF Test Case 14 Finished (forwarded, updated record on same page as before)" << endl << endl;
            free(newRecord);
            free(updatedRecord);
            return success;
        }

        int toDiffPage(RecordBasedFileManager *rbfm)
        {
            cout << "**** In RBF Test Case 14 (forwarded, updated record on different page as before) ****" << endl;

            // Delete records that are not the forwarding.
            // This clears up space in the page for our updated record in the next step.
            for (auto it = rids.begin(); it != rids.end(); it++)
            {
                RID rid = *it;
                if (rid != forwardingRID)
                {
                    auto rc = rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
                    assert(rc == SUCCESS && "Delete record should not fail.");
                }
            }

            // Allocate some record s.t. it's guaranteed to forward to the previous page.
            string lengthyVarChar (1, 'b');
            int newSize;
            void *newRecord = calloc(PAGE_SIZE, sizeof(uint8_t));
            prepareRecord_varchar2048(lengthyVarChar, recordDescriptor, newRecord, &newSize); 

            auto rc = rbfm->updateRecord(fileHandle, recordDescriptor, newRecord, forwardingRID);
            assert(rc == SUCCESS && "Updating record should not fail.");

            void *updatedRecord = calloc(newSize, sizeof(uint8_t));
            rc = rbfm->readRecord(fileHandle, recordDescriptor, forwardingRID, updatedRecord);
            assert(rc == SUCCESS && "Reading updated record should not fail.");

            bool updated_eq_new = memcmp(updatedRecord, newRecord, newSize) == 0 ? true : false;
            assert(updated_eq_new && "Updated record should be equal to the new record.");

            cout << "RBF Test Case 14 Finished (forwarded, updated record on different page as before)" << endl << endl;
            free(newRecord);
            free(updatedRecord);
            return success;
        }

        int runAll(RecordBasedFileManager *rbfm)
        {
            vector<int (*)(RecordBasedFileManager *)> fs
            {
                toSamePage,
                toDiffPage,
            };

            for (auto f : fs)
            {
                beforeEach(rbfm);
                auto rc = f(rbfm);
                assert(rc == success);
                afterEach(rbfm);
            }
            return success;
        }
    }
};

namespace RBFTest_15
{
    string fileName = "test15";
    FileHandle fileHandle;

    void beforeEach(RecordBasedFileManager *rbfm)
    {
        remove(fileName.c_str());

        // Create the file.
        auto rc = rbfm->createFile(fileName);
        assert(rc == success && "Creating the file should not fail.");

        rc = createFileShouldSucceed(fileName);
        assert(rc == success && "Creating the file failed.");

        // Open the file.
        rc = rbfm->openFile(fileName, fileHandle);
        assert(rc == success && "Opening the file should not fail.");

    }

    void afterEach(RecordBasedFileManager *rbfm)
    {
        // Close the file.
        auto rc = rbfm->closeFile(fileHandle);
        assert(rc == success && "Closing the file should not fail.");
        remove(fileName.c_str());
    }

    namespace SinglePage
    {
        namespace SingleRecord
        {
            int test(RecordBasedFileManager *rbfm, vector<string> projectedAttrNames)
            {
                // Create record on page.
                int size;
                vector<Attribute> recordDescriptor;
                void *record = calloc(PAGE_SIZE, sizeof(uint8_t));
                prepareRecord_int_varchar2048_real(12, "345", 6.789, recordDescriptor, record, &size);

                RID rid;
                auto rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
                assert(rc == SUCCESS && "Insert record should not fail.");

                // Setup scan parameters.  Project a single attribute.
                string targetAttrName = recordDescriptor.back().name; // The real attribute.
                CompOp compOp = EQ_OP;
                float compValue = 6.789;
                RBFM_ScanIterator si;
                rc = rbfm->scan(fileHandle, recordDescriptor, targetAttrName, compOp, (void *) (&compValue), projectedAttrNames, si);
                assert(rc == SUCCESS && "Scan on single attribute should not fail.");

                // Scan for a record.
                RID nextRID;
                void *nextRecord = calloc(PAGE_SIZE, sizeof(uint8_t));
                rc = si.getNextRecord(nextRID, nextRecord);
                assert (rc == SUCCESS && "getNextRecord() should not fail.");

                // Create our expected record.
                int expectedSize;
                vector<Attribute> expectedRecordDescriptor;
                void *expectedRecord = calloc(PAGE_SIZE, sizeof(uint8_t));
                prepareRecord_real(compValue, expectedRecordDescriptor, expectedRecord, &expectedSize);

                // Compare expected ==? actual.
                const bool expected_eq_next = memcmp(nextRecord, expectedRecord, expectedSize) == 0 ? true : false;
                assert(expected_eq_next && "nextRecord does not match what we expected.");

                free(expectedRecord);
                free(nextRecord);
                free(record);
                return success;
            }

            int singleAttribute(RecordBasedFileManager *rbfm)
            {
                cout << "**** In RBF Test Case 15 (single page, single record, single projected attribute) ****" << endl;
                vector<string> projectedAttrNames { "Real" };
                auto rc = test(rbfm, projectedAttrNames);
                assert(rc == SUCCESS);
                cout << "RBF Test Case 15 Finished (single page, single record, single projected attribute)" << endl << endl;
                return success;
            }

            int manyAttributes(RecordBasedFileManager *rbfm)
            {
                cout << "**** In RBF Test Case 15 (single page, single record, many projected attributes) ****" << endl;
                vector<string> projectedAttrNames { "Real" };
                auto rc = test(rbfm, projectedAttrNames);
                assert(rc == SUCCESS);
                cout << "RBF Test Case 15 Finished (single page, single record, single projected attribute)" << endl << endl;
                return success;
            }
        };

        namespace ManyRecords
        {
            int consecutive(RecordBasedFileManager *rbfm)
            {
                cout << "**** In RBF Test Case 15 (single page, many records, consecutive) ****" << endl;
                // Insert two identical records (one attribute).
                // Expect both records to be returned by scan (no filtering of attributes is necessary).

                // Create record on page.
                int size;
                vector<Attribute> recordDescriptor;
                void *record = calloc(PAGE_SIZE, sizeof(uint8_t));
                prepareRecord_int_varchar2048_real(12, "345", 6.789, recordDescriptor, record, &size);

                RID rid;
                auto rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
                assert(rc == SUCCESS && "Insert record should not fail.");

                rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
                assert(rc == SUCCESS && "Insert record should not fail.");

                // Setup scan parameters.  Project a single attribute.
                string targetAttrName = recordDescriptor.back().name; // The real attribute.
                CompOp compOp = EQ_OP;
                float compValue = 6.789;
                RBFM_ScanIterator si;
                vector<string> projectedAttrNames { "Real" };
                rc = rbfm->scan(fileHandle, recordDescriptor, targetAttrName, compOp, (void *) (&compValue), projectedAttrNames, si);
                assert(rc == SUCCESS && "Scan on single attribute should not fail.");

                // Scan for a record.
                RID nextRID;
                void *nextRecord = calloc(PAGE_SIZE, sizeof(uint8_t));
                rc = si.getNextRecord(nextRID, nextRecord);
                assert (rc == SUCCESS && "getNextRecord() should not fail.");

                // Create our expected record.
                int expectedSize;
                vector<Attribute> expectedRecordDescriptor;
                void *expectedRecord = calloc(PAGE_SIZE, sizeof(uint8_t));
                prepareRecord_real(compValue, expectedRecordDescriptor, expectedRecord, &expectedSize);

                // Compare expected ==? actual.
                bool expected_eq_next = memcmp(nextRecord, expectedRecord, expectedSize) == 0 ? true : false;
                assert(expected_eq_next && "nextRecord does not match what we expected.");

                // Scan for a record.
                rc = si.getNextRecord(nextRID, nextRecord);
                assert (rc == SUCCESS && "getNextRecord() should not fail.");

                // Create our expected record.
                prepareRecord_real(compValue, expectedRecordDescriptor, expectedRecord, &expectedSize);

                // Compare expected ==? actual.
                expected_eq_next = memcmp(nextRecord, expectedRecord, expectedSize) == 0 ? true : false;
                assert(expected_eq_next && "nextRecord does not match what we expected.");

                cout << "RBF Test Case 15 Finished (single page, many records, consecutive)" << endl << endl;
                free(expectedRecord);
                free(nextRecord);
                free(record);
                return success;
            }

            int nonconsecutive(RecordBasedFileManager *rbfm)
            {
                // Insert three records: r0, r1, r2.  Let r0 == r2, while r1 is different from both r0 and r2.
                // i.e. Insert records X, Y, X.
                // Expect both Xs to be returned by scan.  Also expect Y to not be returned by scan.
                // (no filtering of attributes is necessary).

                cout << "**** In RBF Test Case 15 (single page, many records, nonconsecutive) ****" << endl;
                // Insert two identical records (one attribute).
                // Expect both records to be returned by scan (no filtering of attributes is necessary).

                // Create record on page.
                int size_X;
                vector<Attribute> recordDescriptor_X;
                void *record_X = calloc(PAGE_SIZE, sizeof(uint8_t));
                prepareRecord_int_varchar2048_real(12, "345", 6.789, recordDescriptor_X, record_X, &size_X);

                int size_Y;
                vector<Attribute> recordDescriptor_Y;
                void *record_Y = calloc(PAGE_SIZE, sizeof(uint8_t));
                prepareRecord_int_varchar2048_real(12, "345", 9.876, recordDescriptor_Y, record_Y, &size_Y);

                RID rid;

                // Insert X.
                auto rc = rbfm->insertRecord(fileHandle, recordDescriptor_X, record_X, rid);
                assert(rc == SUCCESS && "Insert record should not fail.");

                // Insert Y.
                rc = rbfm->insertRecord(fileHandle, recordDescriptor_Y, record_Y, rid);
                assert(rc == SUCCESS && "Insert record should not fail.");

                // Insert X.
                rc = rbfm->insertRecord(fileHandle, recordDescriptor_X, record_X, rid);
                assert(rc == SUCCESS && "Insert record should not fail.");

                // Setup scan parameters.  Project a single attribute.
                string targetAttrName = recordDescriptor_X.back().name; // The real attribute.
                CompOp compOp = EQ_OP;
                float compValue = 6.789;
                RBFM_ScanIterator si;
                vector<string> projectedAttrNames { "Real" };
                rc = rbfm->scan(fileHandle, recordDescriptor_X, targetAttrName, compOp, (void *) (&compValue), projectedAttrNames, si);
                assert(rc == SUCCESS && "Scan on single attribute should not fail.");

                // Scan for a record.
                RID nextRID;
                void *nextRecord = calloc(PAGE_SIZE, sizeof(uint8_t));
                rc = si.getNextRecord(nextRID, nextRecord);
                assert (rc == SUCCESS && "getNextRecord() should not fail.");

                // Create our expected record.
                int expectedSize;
                vector<Attribute> expectedRecordDescriptor;
                void *expectedRecord = calloc(PAGE_SIZE, sizeof(uint8_t));
                prepareRecord_real(compValue, expectedRecordDescriptor, expectedRecord, &expectedSize);

                // Compare expected ==? actual.
                bool expected_eq_next = memcmp(nextRecord, expectedRecord, expectedSize) == 0 ? true : false;
                assert(expected_eq_next && "nextRecord does not match what we expected.");

                // Scan for a record.
                rc = si.getNextRecord(nextRID, nextRecord);
                assert (rc == SUCCESS && "getNextRecord() should not fail.");

                // Create our expected record.
                prepareRecord_real(compValue, expectedRecordDescriptor, expectedRecord, &expectedSize);

                // Compare expected ==? actual.
                expected_eq_next = memcmp(nextRecord, expectedRecord, expectedSize) == 0 ? true : false;
                assert(expected_eq_next && "nextRecord does not match what we expected.");

                cout << "RBF Test Case 15 Finished (single page, many records, nonconsecutive)" << endl << endl;
                free(expectedRecord);
                free(nextRecord);
                free(record_Y);
                free(record_X);
                return success;
            }
        };

        int runAll(RecordBasedFileManager *rbfm)
        {
            vector<int (*)(RecordBasedFileManager *)> fs
            {
                SingleRecord::singleAttribute,
                SingleRecord::manyAttributes,
                ManyRecords::consecutive,
                ManyRecords::nonconsecutive,
            };

            for (auto f : fs)
            {
                beforeEach(rbfm);
                auto rc = f(rbfm);
                assert(rc == success);
                afterEach(rbfm);
            }
            return success;
        }
    };

    namespace ManyPages
    {


        int nonconsecutive(RecordBasedFileManager *rbfm)
        {
            // Fill the first page for a file with records X.
            // Update the last record in the page to be Y.
            // Insert another record Y in the second page.
            // Scan and expect both Ys to be returned.

            cout << "**** In RBF Test Case 15 (many pages) ****" << endl;
            // Insert two identical records (one attribute).
            // Expect both records to be returned by scan (no filtering of attributes is necessary).

            // Create record on page.
            int size_X;
            vector<Attribute> recordDescriptor_X;
            void *record_X = calloc(PAGE_SIZE, sizeof(uint8_t));
            prepareRecord_int_varchar2048_real(12, "345", 6.789, recordDescriptor_X, record_X, &size_X);

            int size_Y;
            vector<Attribute> recordDescriptor_Y;
            void *record_Y = calloc(PAGE_SIZE, sizeof(uint8_t));
            prepareRecord_int_varchar2048_real(12, "345", 9.876, recordDescriptor_Y, record_Y, &size_Y);

            RID prevRID, currRID;
            PageNum initialPageNum;

            // Insert initial X.
            auto rc = rbfm->insertRecord(fileHandle, recordDescriptor_X, record_X, currRID);
            assert(rc == SUCCESS && "Insert record should not fail.");
            initialPageNum = currRID.pageNum;

            // Repeatedly insert more X until an X is on another page.
            while (currRID.pageNum == initialPageNum)
            {
                prevRID = currRID;
                rc = rbfm->insertRecord(fileHandle, recordDescriptor_X, record_X, currRID);
                assert(rc == SUCCESS && "Insert record should not fail.");
            }
            // Update the last two Xs to be Ys.
            rc = rbfm->updateRecord(fileHandle, recordDescriptor_Y, record_Y, prevRID); // On initial page.
            assert(rc == SUCCESS && "Update record should not fail.");

            rc = rbfm->updateRecord(fileHandle, recordDescriptor_Y, record_Y, currRID); // On another page.
            assert(rc == SUCCESS && "Update record should not fail.");

            // Setup scan parameters.  Project a single attribute.
            string targetAttrName = recordDescriptor_Y.back().name; // The real attribute.
            CompOp compOp = EQ_OP;
            float compValue = 9.876;
            RBFM_ScanIterator si;
            vector<string> projectedAttrNames { "Real" };
            rc = rbfm->scan(fileHandle, recordDescriptor_Y, targetAttrName, compOp, (void *) (&compValue), projectedAttrNames, si);
            assert(rc == SUCCESS && "Scan on single attribute should not fail.");

            // Scan for a record.
            RID nextRID;
            void *nextRecord = calloc(PAGE_SIZE, sizeof(uint8_t));
            rc = si.getNextRecord(nextRID, nextRecord);
            assert (rc == SUCCESS && "getNextRecord() should not fail.");

            // Create our expected record.
            int expectedSize;
            vector<Attribute> expectedRecordDescriptor;
            void *expectedRecord = calloc(PAGE_SIZE, sizeof(uint8_t));
            prepareRecord_real(compValue, expectedRecordDescriptor, expectedRecord, &expectedSize);

            // Compare expected ==? actual.
            bool expected_eq_next = memcmp(nextRecord, expectedRecord, expectedSize) == 0 ? true : false;
            assert(expected_eq_next && "nextRecord does not match what we expected.");

            // Scan for a record.
            rc = si.getNextRecord(nextRID, nextRecord);
            assert (rc == SUCCESS && "getNextRecord() should not fail.");

            // Compare expected ==? actual.
            expected_eq_next = memcmp(nextRecord, expectedRecord, expectedSize) == 0 ? true : false;
            assert(expected_eq_next && "nextRecord does not match what we expected.");

            cout << "RBF Test Case 15 Finished (many pages)" << endl << endl;
            free(expectedRecord);
            free(nextRecord);
            free(record_Y);
            free(record_X);
            return success;
        }

        int runAll(RecordBasedFileManager *rbfm)
        {
            vector<int (*)(RecordBasedFileManager *)> fs
            {
                nonconsecutive,
            };

            for (auto f : fs)
            {
                beforeEach(rbfm);
                auto rc = f(rbfm);
                assert(rc == success);
                afterEach(rbfm);
            }
            return success;
        }
        
    };
};

int RBFTest_16(RecordBasedFileManager *rbfm)
{
    cout << "**** In RBF Test Case 16 ****" << endl;
    string fileName { "test16" };
    remove(fileName.c_str());

    // Create the file.
    auto rc = rbfm->createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file failed.");

    // Open the file.
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");

    // Create record on page.
    int size;
    vector<Attribute> recordDescriptor;
    void *record = calloc(PAGE_SIZE, sizeof(uint8_t));
    prepareRecord_int_varchar2048_real(12, "345", 6.789, recordDescriptor, record, &size);

    RID rid;
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
    assert(rc == SUCCESS && "Insert record should not fail.");

    // Setup scan parameters.  Project a single attribute.
    string targetAttrName = recordDescriptor.back().name; // The real attribute.
    CompOp compOp = EQ_OP;
    float compValue = 6.789;
    vector<string> projectedAttrNames = { "Real" };
    RBFM_ScanIterator si;
    rc = rbfm->scan(fileHandle, recordDescriptor, targetAttrName, compOp, (void *) (&compValue), projectedAttrNames, si);
    assert(rc == SUCCESS && "Scan on single attribute should not fail.");

    // Close iterator.
    rc = si.close();
    assert(rc == SUCCESS && "Closing a ScanIterator should not fail.");

    // Scan for a record.
    RID nextRID;
    void *nextRecord = calloc(PAGE_SIZE, sizeof(uint8_t));
    rc = si.getNextRecord(nextRID, nextRecord);
    assert (rc == RBFM_SI_CLOSED && "Should not be able to get a record when a ScanIterator is closed.");

    // Close the file.
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");
    remove(fileName.c_str());
    cout << "RBF Test Case 16 Finished" << endl << endl;

    free(nextRecord);
    free(record);
    return success;
}

int main()
{
    // To test the functionality of the paged file manager
    PagedFileManager *pfm = PagedFileManager::instance();

    // To test the functionality of the record-based file manager
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    // Remove files that might be created by previous test run
    remove("test");
    remove("test_1");
    remove("test_2");
    remove("test_3");
    remove("test_4");

    remove("test8");
    remove("test9");
    remove("test9rids");
    remove("test9sizes");
    remove("test12");
    remove("test13");
    remove("test14");

    RBFTest_1(pfm);
    RBFTest_2(pfm);
    RBFTest_3(pfm);
    RBFTest_4(pfm);
    RBFTest_5(pfm);
    RBFTest_6(pfm);
    RBFTest_7(pfm);
  
    RBFTest_8(rbfm);

    vector<RID> rids;
    vector<int> sizes;
    RBFTest_9(rbfm, rids, sizes);
    RBFTest_10(rbfm);

    RBFTest_11(); // Forwarding utils.

    RBFTest_12(rbfm); 
    RBFTest_13(rbfm);

    RBFTest_14::Unforwarded::runAll(rbfm);
    RBFTest_14::Forwarded::runAll(rbfm);

    RBFTest_15::SinglePage::runAll(rbfm);
    RBFTest_15::ManyPages::runAll(rbfm);

    RBFTest_16(rbfm);

    return 0;
}
