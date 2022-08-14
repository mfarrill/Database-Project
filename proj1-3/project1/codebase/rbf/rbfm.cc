#include "rbfm.h"

const size_t SlotSize = 2 * sizeof(uint32_t);

//https://stackoverflow.com/questions/12276675/modulus-with-negative-numbers-in-c
int mod(int a, int b)
{
    return ((a % b) + b) % b;
}

unsigned char *getNullFlags(const vector<Attribute> &recordDescriptor, const void *data, uint32_t &length) {
    uint32_t numberOfFields = recordDescriptor.size();

    //Calculate the number of null flag bits and allocate enough memory
    length = ceil((double)numberOfFields / CHAR_BIT);
    unsigned char *nullsIndicator = (unsigned char *)malloc(length);

    //Retrieve the null flags and advance the position
    memcpy(nullsIndicator, (char *)data, length);
    return nullsIndicator;
}

/* Reads from raw valuesData and returns the necessary space for storing them in a writable record.
 *   - valuesData must point directly to the first value.
 */
const uint32_t getValuesLengthForRecord(const vector<Attribute> &recordDescriptor, const void *valuesData, const unsigned char *nullsIndicator) {
    uint32_t position = 0;
    uint32_t nbytes = 0;
    int index = 0;
    for (Attribute descriptor : recordDescriptor)
    {
        ++index;

        int nullBitPosition = mod(-index, CHAR_BIT);
        bool nullBit = nullsIndicator[(index - 1) / CHAR_BIT] & (1 << nullBitPosition);
        if (nullBit)
        {
            continue;
        }

        switch (descriptor.type)
        {
        case TypeInt:
        {
            position += sizeof(uint32_t);
            nbytes += sizeof(uint32_t);
            break;
        }
        case TypeReal:
        {
            position += sizeof(float);
            nbytes += sizeof(float);
            break;
        }
        case TypeVarChar:
        {
            uint32_t varCharSize = 0;
            memcpy(&varCharSize, (char *)valuesData + position, sizeof(varCharSize)); // Get the length.
            position += sizeof(varCharSize) + varCharSize; // Skip over the length value itself and the VarChar.
            nbytes += varCharSize; // We don't need to store the length for our WR format.
            break;
        }
        }
    }

    return nbytes; 
}

const void *getWritableRecord(const vector<Attribute> &recordDescriptor, const void *data, size_t &recordSize) {
    uint32_t nullsFlagLength = 0;
    unsigned char *nullsIndicator = getNullFlags(recordDescriptor, data, nullsFlagLength);

    const uint32_t fieldCount = recordDescriptor.size();
    const uint32_t fieldOffsetSize = sizeof(uint32_t);
    const uint32_t fieldOffsetTotalLength = fieldCount * fieldOffsetSize;

    char *valuesData = (char *)data + nullsFlagLength;
    uint32_t valuesLength = getValuesLengthForRecord(recordDescriptor, valuesData, nullsIndicator);

    const int recordValuesStart = sizeof(fieldCount) + nullsFlagLength + fieldOffsetTotalLength;
    recordSize = recordValuesStart + valuesLength;
    unsigned char *record = (unsigned char *)malloc(recordSize);


    int positionInRecord = 0;

    // Set field length.
    memcpy(record + positionInRecord, &fieldCount, sizeof(fieldCount));
    positionInRecord += sizeof(fieldCount);

    // Set null bytes.
    memcpy(record + positionInRecord, nullsIndicator, nullsFlagLength);
    positionInRecord += nullsFlagLength;

    // Iterate through our fields, copying the data's values
    // while simultaneously setting resulting field offsets.

    const uint32_t dataValuesStart = nullsFlagLength; // Values follow null flags.
    int positionInData = dataValuesStart;

    int recordPreviousValueEnd = recordValuesStart;

    //cout << "WRITING\n";

    int index = 0;
    for (Attribute descriptor : recordDescriptor)
    {
        ++index;

        int nullBitPosition = mod(-index, CHAR_BIT);
        bool nullBit = nullsIndicator[(index - 1) / CHAR_BIT] & (1 << nullBitPosition);
        if (nullBit)
        {
            continue;
        }

        //cout << "name: " << descriptor.name << "\n";
        //cout << "\ttype: " << descriptor.type << "\n";
        //cout << "\tlength: " << descriptor.length << "\n";

        int fieldSize = 0;
        switch (descriptor.type)
        {
            case TypeInt:
            {
                fieldSize = sizeof(uint32_t);
                break;
            }
            case TypeReal:
            {
                fieldSize = sizeof(float);
                break;
            }
            case TypeVarChar:
            {
                // Get length of VarChar.
                memcpy(&fieldSize, (char *)data + positionInData, sizeof(uint32_t));
                positionInData += sizeof(uint32_t); // Setup "pointer" to the VarChar itself.
                break;
            }
        }

        // Copy field value from data to record.
        const uint32_t recordCurrentValueStart = recordPreviousValueEnd;
        memcpy(record + recordCurrentValueStart, (char *)data + positionInData, fieldSize);
        
        if (descriptor.type == TypeVarChar) {
            char *tmp = (char *)calloc(fieldSize + 1, sizeof(char));
            memcpy(tmp, (char *)data + positionInData, fieldSize);
            //cout << "\tvalue: " << tmp << "\n";
            free(tmp);
        } else if (descriptor.type == TypeInt) {
            int tmp;
            memcpy(&tmp, (char *)data + positionInData, fieldSize);
            //cout << "\tvalue: " << tmp << "\n";
        } else {
            float tmp;
            memcpy(&tmp, (char *)data + positionInData, fieldSize);
            //cout << "\tvalue: " << tmp << "\n";
        }

        positionInData += fieldSize;

        // "Point" field offset at end of value.
        const uint32_t recordCurrentValueEnd = recordCurrentValueStart + fieldSize;
        memcpy(record + positionInRecord, &recordCurrentValueEnd, fieldOffsetSize);
        positionInRecord += fieldOffsetSize;

        recordPreviousValueEnd = recordCurrentValueEnd;
    }

    free(nullsIndicator);
    return (void *)record;
}

void *findPageForRecord(FileHandle &fileHandle, const void *record) {
    return 0;
}

RecordBasedFileManager *RecordBasedFileManager::_rbf_manager = 0;

PagedFileManager *RecordBasedFileManager::pfm;

RecordBasedFileManager *RecordBasedFileManager::instance()
{
    if (!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
    pfm = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

/* SLOT: [uint32_t length, uint32_t offset] 
   0-indexed
*/
int findOffset(uint32_t slotNum, const void *page, int *length = nullptr)
{
    int offset;
    size_t freeSpacePointerLength = sizeof(uint32_t);
    size_t slotCounterLength = sizeof(uint32_t);
    int slotOffset = PAGE_SIZE - freeSpacePointerLength - slotCounterLength - SlotSize * (slotNum + 1);
    if (length != nullptr)
    {
        memcpy(length, (char *)page + slotOffset, sizeof(uint32_t));
    }
    slotOffset += sizeof(uint32_t);
    memcpy(&offset, (char *)page + slotOffset, sizeof(uint32_t));
    return offset;
}

// Assume units of words (4 bytes) unless otherwise stated.
size_t getFreeSpace(const void *page) 
{
    const uint32_t freeSpaceOffsetPosition = PAGE_END_INDEX;
    const uint32_t *freeSpaceStartBytes = (const uint32_t *) page + freeSpaceOffsetPosition;

    // Check if the slot count is 0.
    const uint32_t slotCountPosition = freeSpaceOffsetPosition - 1;
    const uint32_t *slotCount = (const uint32_t *) page + slotCountPosition;
    if (*slotCount == 0) 
    {
        int slotCountPositionBytes = slotCountPosition * sizeof(uint32_t); // Convert word position to bytes.
        return slotCountPositionBytes - *freeSpaceStartBytes;
    }
    
    int freeSpaceEnd = findOffset(*slotCount - 1, page);
    return freeSpaceEnd - *freeSpaceStartBytes;
}

bool recordFits(const size_t recordSize, const void *page)
{
    return recordSize + SlotSize <= getFreeSpace(page);
}

void *getPageToInsertRecord(FileHandle &fileHandle, const int recordSize, PageNum &pageNum) {
    void *pageData = malloc(PAGE_SIZE); // Must be deleted later.
    const char *initBytes = ""; // Data used to initialize the new page with.  We want it blank.

    auto npages = fileHandle.getNumberOfPages();
    if (npages == 0) {
        fileHandle.appendPage((const void *)initBytes);
        fileHandle.readPage(0, pageData);
        pageNum = 0;
        return pageData;
    }

    PageNum lastPage = npages - 1;

    fileHandle.readPage(lastPage, pageData);
    if (recordFits(recordSize, pageData))
    {
        pageNum = lastPage;
        return pageData;
    }

    for (PageNum i = 0; i < lastPage; i++)
    {
        fileHandle.readPage(i, pageData);
        if (recordFits(recordSize, pageData))
        {
            pageNum = i;
            return pageData;
        }
    }

    fileHandle.appendPage((const void *)initBytes);
    auto appendedPage = npages;
    fileHandle.readPage(appendedPage, pageData);
    pageNum = appendedPage;
    return pageData;
}

RC RecordBasedFileManager::createFile(const string &fileName)
{
    return pfm->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName)
{
    return pfm->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    return pfm->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle)
{
    return pfm->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid)
{
    size_t recordSize = 0;
    PageNum pageNum = 0;
    const void *record = getWritableRecord(recordDescriptor, data, recordSize);
    //cout << "recordSize: " << recordSize << "\n";
    void *page = getPageToInsertRecord(fileHandle, recordSize, pageNum);
    
    const uint32_t freeSpaceOffsetPosition = PAGE_END_INDEX;
    uint32_t *freeSpaceOffsetValue = (uint32_t *)page + freeSpaceOffsetPosition;
    //cout << "freeSpaceOffsetPos: " << freeSpaceOffsetPosition << "\n";
    //cout << "freeSpaceOFfsetValue: " << *freeSpaceOffsetValue << "\n";

    const uint32_t slotCountPosition = freeSpaceOffsetPosition - 1;
    uint32_t *slotCount = (uint32_t *)page + slotCountPosition;
    //cout << "slotCountPos: " << slotCountPosition << "\n";
    //cout << "slotCount: " << *slotCount << "\n";

    uint8_t *recordInsertPosition = (uint8_t *)page + *freeSpaceOffsetValue;
    memcpy(recordInsertPosition, record, recordSize);

    rid.pageNum = pageNum;
    rid.slotNum = *slotCount;
    //cout << "pageNum: " << rid.pageNum << "\n";
    //cout << "slotNum: " << rid.slotNum << "\n";

    *freeSpaceOffsetValue += recordSize;
    *slotCount += 1;
    //cout << "(after) freeSpaceOFfsetValue: " << *freeSpaceOffsetValue << "\n";
    //cout << "(after) slotCount: " << *slotCount << "\n";

    if (fileHandle.writePage(pageNum, page) != 0) {
        return -1;
    }
    
    free(page);
    free(const_cast<void *>(record));
    return 0;
}


//EXAMPLE RECORD LAYOUT: [byteArray:nullFlags, int*:pointerToIntegerField, float*:pointerToRealField, void*:pointerToVarCharField, int:integerField, float:realField, int:lengthOfVarCharField, charArray:varCharField]
RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data)
{
    //cout << "readRecord():\n";
    //cout << "rid.pageNum: " << rid.pageNum << "\n";
    //cout << "rid.slotNum: " << rid.slotNum << "\n";
    //read page into memory
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);

    //find the offset of the record in the page
    int length = 0;
    int recordOffset = findOffset(rid.slotNum, page, &length);
    int positionInData = 0;
    int positionInRecord = 0;

    //Calculate the length of the null bits field
    int numberOfFields = recordDescriptor.size();
    int nullFlagLength = ceil((double)numberOfFields / CHAR_BIT);
    unsigned char *nullsIndicator = (unsigned char *)malloc(nullFlagLength);

    //Retrieve the null flags, write them to data and advance the position
    const char *record = (char *)page + recordOffset + sizeof(uint32_t); // Skip over the first number of fields.
    memcpy(nullsIndicator, record, nullFlagLength);
    memcpy(data, nullsIndicator, nullFlagLength);
    positionInData += nullFlagLength;
    positionInRecord += nullFlagLength + (sizeof(uint32_t) * numberOfFields);

    //cout << "READING\n";

    int index = 0;
    for (Attribute descriptor : recordDescriptor)
    {
        ++index;
        //Where in the 8 bits is the flag? The Most Significant Bit is bit 7 and Least 0
        //SEE SLACK CHAT FOR EXPLAINATION: https://cmps181group.slack.com/archives/CHR7PLWT1/p1555617848047000
        //NOTE: -a MOD b =
        int nullBitPosition = mod(-index, CHAR_BIT);

        //1 array element for every CHAR_BIT fields so [0] = 0:7 [1] = 8:15 etc.
        //Create a Mask with a "1" in the correct position and AND it with the value of nullsIndicator
        bool nullBit = nullsIndicator[(index - 1) / CHAR_BIT] & (1 << nullBitPosition);

        //cout << "name: " << descriptor.name << "\n";
        //cout << "\ttype: " << descriptor.type << "\n";
        //cout << "\tlength: " << descriptor.length << "\n";

        if (!nullBit)
        {
            switch (descriptor.type)
            {
            case TypeInt:
            {
                //cout << "\tsize: 4 bytes\n";

                uint32_t tmp;
                memcpy(&tmp, record + positionInRecord, sizeof(uint32_t));
                //cout << "\tvalue: " << tmp << "\n";

                memcpy((char *)data + positionInData, record + positionInRecord, sizeof(uint32_t));
                positionInRecord += sizeof(uint32_t);
                positionInData += sizeof(uint32_t);
            }
            break;
            case TypeReal:
            {
                //cout << "\tsize: 4 bytes\n";

                float tmp;
                memcpy(&tmp, record + positionInRecord, sizeof(uint32_t));
                //cout << "\tvalue: " << tmp << "\n";

                memcpy((char *)data + positionInData, record + positionInRecord, sizeof(uint32_t));
                positionInRecord += sizeof(uint32_t);
                positionInData += sizeof(uint32_t);
            }
            break;
            case TypeVarChar:
            {
                //const uint32_t varCharSize = descriptor.length;
                const uint32_t varCharSize = 8; // [DEBUG] REMOVE THIS, TESTING ONLY
                //cout << "\tvarCharSize: " << varCharSize << "\n";
                memcpy((char *)data + positionInData, &varCharSize, sizeof(uint32_t));
                positionInData += sizeof(uint32_t);

                char *tmp = (char *)calloc(varCharSize + 1, sizeof(char));
                memcpy(tmp, record + positionInRecord, varCharSize);
                //cout << "\tvalue: " << tmp << "\n";
                free(tmp);

                memcpy((char *)data + positionInData, record + positionInRecord, varCharSize);
                positionInRecord += varCharSize;
                positionInData += varCharSize;
            }
            break;
            }
        }
    }
    free(page);
    free(nullsIndicator);
    return 0;
}

// The format is as follows:
// field1-name: field1-value  field2-name: field2-value ... \n
// (e.g., age: 24  height: 6.1  salary: 9000
//        age: NULL  height: 7.5  salary: 7500)
RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data)
{
    int numberOfFields = recordDescriptor.size();
    int position = 0;

    //Calculate the number of null flag bits and allocate enough memory
    int nullFlagLength = ceil((double)numberOfFields / CHAR_BIT);
    unsigned char *nullsIndicator = (unsigned char *)malloc(nullFlagLength);

    //Retrieve the null flags and advance the position
    memcpy(nullsIndicator, (char *)data, nullFlagLength);
    position += nullFlagLength;

    int index = 0;
    for (Attribute descriptor : recordDescriptor)
    {
        ++index;
        cout << descriptor.name << ": ";
        //Where in the 8 bits is the flag? The Most Significant Bit is bit 7 and Least 0
        //SEE SLACK CHAT FOR EXPLAINATION: https://cmps181group.slack.com/archives/CHR7PLWT1/p1555617848047000
        //NOTE: -a MOD b =
        int nullBitPosition = mod(-index, CHAR_BIT);

        //1 array element for every CHAR_BIT fields so [0] = 0:7 [1] = 8:15 etc.
        //Create a Mask with a "1" in the correct position and AND it with the value of nullsIndicator
        bool nullBit = nullsIndicator[(index - 1) / CHAR_BIT] & (1 << nullBitPosition);

        //If the null indicator is 1 then skip this iteration
        if (nullBit)
        {
            cout << "NULL ";
            continue;
        }
        switch (descriptor.type)
        {
        case TypeInt:
        {
            int number = 0;
            memcpy(&number, (char *)data + position, sizeof(int));
            cout << number << " ";
            position += sizeof(int);
            break;
        }
        case TypeReal:
        {
            float number = 0;
            memcpy(&number, (char *)data + position, sizeof(float));
            cout << number << " ";
            position += sizeof(float);
            break;
        }
        case TypeVarChar:
        {
            int varCharSize = 0;
            memcpy(&varCharSize, (char *)data + position, sizeof(int));
            char varChar[varCharSize + 1];
            memcpy(&varChar, (char *)data + position + sizeof(int), varCharSize);
            varChar[varCharSize] = '\0';
            cout << varChar << " ";
            position += sizeof(int) + varCharSize;
            break;
        }
        }
    }
    cout << endl;
    free(nullsIndicator);
    return 0;
}