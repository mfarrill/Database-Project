#include "rbfm.h"
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <string.h>
#include <iomanip>

void RID::print()
{
    cout << "<pid=" << pageNum << ", sid=" << slotNum << ">";
}

bool operator==(const RID &x, const RID &y)
{
    return x.pageNum == y.pageNum && x.slotNum == y.slotNum;
}

bool operator!=(const RID &x, const RID &y)
{
    return !(x == y);
}

size_t RIDHasher::operator()(const RID rid) const
{
    return hash<unsigned>{}(rid.pageNum ^ rid.slotNum);
}

bool evalCompOp(void *x, void *y, CompOp op, AttrType attrType)
{
    if (op == NO_OP)
        return true;

    if (x == nullptr || y == nullptr)
        return false;

    int ix, iy;
    float fx, fy;
    string sx, sy;
    int cmp;

    bool x_LT_y;
    bool x_EQ_y;
    bool x_GT_y;

    switch (attrType)
    {
    case TypeInt:
        ix = *(int *)x;
        iy = *(int *)y;

        x_LT_y = ix < iy;
        x_EQ_y = ix == iy;
        x_GT_y = ix > iy;
        break;

    case TypeReal:
        fx = *(float *)x;
        fy = *(float *)y;

        x_LT_y = fx < fy;
        x_EQ_y = fx == fy;
        x_GT_y = fx > fy;
        break;

    case TypeVarChar:
        sx = string{(char *)x};
        sy = string{(char *)y};

        cmp = sx.compare(sy);

        x_LT_y = cmp < 0;
        x_EQ_y = cmp == 0;
        x_GT_y = cmp > 0;
        break;

    default:
        return false;
    }

    switch (op)
    {
    case EQ_OP:
        return x_EQ_y;
    case LT_OP:
        return x_LT_y;
    case LE_OP:
        return x_LT_y || x_EQ_y;
    case GT_OP:
        return x_GT_y;
    case GE_OP:
        return x_GT_y || x_EQ_y;
    case NE_OP:
        return !x_EQ_y;
    case NO_OP:
        return true;

    default:
        return false;
    }
}

RecordBasedFileManager *RBFM_ScanIterator::rbfm_ = NULL;
RecordBasedFileManager *RecordBasedFileManager::_rbf_manager = NULL;
PagedFileManager *RecordBasedFileManager::_pf_manager = NULL;

RecordBasedFileManager *RecordBasedFileManager::instance()
{
    if (!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName)
{
    // Creating a new paged file.
    if (_pf_manager->createFile(fileName))
        return RBFM_CREATE_FAILED;

    // Setting up the first page.
    void *firstPageData = calloc(PAGE_SIZE, 1);
    if (firstPageData == NULL)
        return RBFM_MALLOC_FAILED;
    newRecordBasedPage(firstPageData);
    // Adds the first record based page.

    FileHandle handle;
    if (_pf_manager->openFile(fileName.c_str(), handle))
        return RBFM_OPEN_FAILED;
    if (handle.appendPage(firstPageData))
        return RBFM_APPEND_FAILED;
    _pf_manager->closeFile(handle);

    free(firstPageData);

    return SUCCESS;
}

RC RecordBasedFileManager::destroyFile(const string &fileName)
{
    return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    return _pf_manager->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle)
{
    return _pf_manager->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid)
{
    // Gets the size of the record.
    unsigned recordSize = getRecordSize(recordDescriptor, data);

    // Cycles through pages looking for enough free space for the new entry.
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;

    int32_t slotCandidate = -1;
    bool foundEmptySlot = false;
    bool spaceFound = false;
    unsigned i;
    unsigned numPages = fileHandle.getNumberOfPages();

    // When we find a page with enough space (accounting also for the size that will be added to the slot directory), we stop the loop.
    for (i = 0; i < numPages; i++)
    {
        if (fileHandle.readPage(i, pageData))
            return RBFM_READ_FAILED;

        const auto freeSpaceSize = getPageFreeSpaceSize(pageData);
        const bool mayFillEmptySlot = freeSpaceSize >= recordSize;
        const bool canAllocateWithNewSlot = freeSpaceSize >= sizeof(SlotDirectoryRecordEntry) + recordSize;

        // Check for allocated but unused slot.
        if (mayFillEmptySlot && (slotCandidate = findEmptySlot(pageData)) >= 0)
        {
            foundEmptySlot = true;
            spaceFound = true;
            break;
        }
        else if (canAllocateWithNewSlot)
        {
            spaceFound = true;
            break;
        }
    }

    SlotDirectoryHeader slotHeader;
    if (!spaceFound)
    {
        newRecordBasedPage(pageData);
        slotHeader = getSlotDirectoryHeader(pageData);
        rid.slotNum = 0;
    }
    else
    {
        slotHeader = getSlotDirectoryHeader(pageData);
        rid.slotNum = foundEmptySlot ? slotCandidate : slotHeader.recordEntriesNumber;
    }

    rid.pageNum = i;

    // Adding the new record reference in the slot directory.
    SlotDirectoryRecordEntry newRecordEntry;
    newRecordEntry.length = recordSize;
    newRecordEntry.offset = slotHeader.freeSpaceOffset - recordSize;
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);

    // Updating the slot directory header.
    slotHeader.freeSpaceOffset = newRecordEntry.offset;
    if (!foundEmptySlot)
    {
        slotHeader.recordEntriesNumber++;
    }
    setSlotDirectoryHeader(pageData, slotHeader);

    // Adding the record data.
    setRecordAtOffset(pageData, newRecordEntry.offset, recordDescriptor, data);

    // Writing the page to disk.
    if (spaceFound)
    {
        if (fileHandle.writePage(i, pageData))
            return RBFM_WRITE_FAILED;
    }
    else
    {
        if (fileHandle.appendPage(pageData))
            return RBFM_APPEND_FAILED;
    }

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data)
{
    // Retrieve the specified page
    void *pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

    if (slotHeader.recordEntriesNumber < rid.slotNum)
    {
        free(pageData);
        return RBFM_SLOT_DN_EXIST;
    }

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);

    // Retrieve the actual entry data
    if (recordEntry.offset < 0)
    {
        free(pageData);
        return RBFM_SLOT_DN_EXIST;
    }

    if (isSlotForwarding(recordEntry))
    {
        free(pageData);
        RID new_rid = getRID(recordEntry);
        return readRecord(fileHandle, recordDescriptor, new_rid, data);
    }

    getRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);
    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data)
{
    // Parse the null indicator and save it into an array
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, data, nullIndicatorSize);

    // We've read in the null indicator, so we can skip past it now
    unsigned offset = nullIndicatorSize;

    cout << "----" << endl;
    for (unsigned i = 0; i < (unsigned)recordDescriptor.size(); i++)
    {
        cout << setw(10) << left << recordDescriptor[i].name << ": ";
        // If the field is null, don't print it
        bool isNull = fieldIsNull(nullIndicator, i);
        if (isNull)
        {
            cout << "NULL" << endl;
            continue;
        }
        switch (recordDescriptor[i].type)
        {
        case TypeInt:
            uint32_t data_integer;
            memcpy(&data_integer, ((char *)data + offset), INT_SIZE);
            offset += INT_SIZE;

            cout << "" << data_integer << endl;
            break;
        case TypeReal:
            float data_real;
            memcpy(&data_real, ((char *)data + offset), REAL_SIZE);
            offset += REAL_SIZE;

            cout << "" << data_real << endl;
            break;
        case TypeVarChar:
            // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
            uint32_t varcharSize;
            memcpy(&varcharSize, ((char *)data + offset), VARCHAR_LENGTH_SIZE);
            offset += VARCHAR_LENGTH_SIZE;

            // Gets the actual string.
            char *data_string = (char *)malloc(varcharSize + 1);
            if (data_string == NULL)
                return RBFM_MALLOC_FAILED;
            memcpy(data_string, ((char *)data + offset), varcharSize);

            // Adds the string terminator.
            data_string[varcharSize] = '\0';
            offset += varcharSize;

            cout << data_string << endl;
            free(data_string);
            break;
        }
    }
    cout << "----" << endl;

    return SUCCESS;
}

SlotDirectoryHeader RecordBasedFileManager::getSlotDirectoryHeader(void *page)
{
    // Getting the slot directory header.
    SlotDirectoryHeader slotHeader;
    memcpy(&slotHeader, page, sizeof(SlotDirectoryHeader));
    return slotHeader;
}

void RecordBasedFileManager::setSlotDirectoryHeader(void *page, SlotDirectoryHeader slotHeader)
{
    // Setting the slot directory header.
    memcpy(page, &slotHeader, sizeof(SlotDirectoryHeader));
}

SlotDirectoryRecordEntry RecordBasedFileManager::getSlotDirectoryRecordEntry(void *page, unsigned recordEntryNumber)
{
    // Getting the slot directory entry data.
    SlotDirectoryRecordEntry recordEntry;
    memcpy(
        &recordEntry,
        ((char *)page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
        sizeof(SlotDirectoryRecordEntry));

    return recordEntry;
}

void RecordBasedFileManager::setSlotDirectoryRecordEntry(void *page, unsigned recordEntryNumber, SlotDirectoryRecordEntry recordEntry)
{
    // Setting the slot directory entry data.
    memcpy(
        ((char *)page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
        &recordEntry,
        sizeof(SlotDirectoryRecordEntry));
}

// Configures a new record based page, and puts it in "page".
void RecordBasedFileManager::newRecordBasedPage(void *page)
{
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    SlotDirectoryHeader slotHeader;
    slotHeader.freeSpaceOffset = PAGE_SIZE;
    slotHeader.recordEntriesNumber = 0;
    memcpy(page, &slotHeader, sizeof(SlotDirectoryHeader));
}

unsigned RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char *)data, nullIndicatorSize);

    // Offset into *data. Start just after the null indicator
    unsigned offset = nullIndicatorSize;
    // Running count of size. Initialize it to the size of the header
    unsigned size = sizeof(RecordLength) + (recordDescriptor.size()) * sizeof(ColumnOffset) + nullIndicatorSize;

    for (unsigned i = 0; i < (unsigned)recordDescriptor.size(); i++)
    {
        // Skip null fields
        if (fieldIsNull(nullIndicator, i))
            continue;
        switch (recordDescriptor[i].type)
        {
        case TypeInt:
            size += INT_SIZE;
            offset += INT_SIZE;
            break;
        case TypeReal:
            size += REAL_SIZE;
            offset += REAL_SIZE;
            break;
        case TypeVarChar:
            uint32_t varcharSize;
            // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
            memcpy(&varcharSize, (char *)data + offset, VARCHAR_LENGTH_SIZE);
            size += varcharSize;
            offset += varcharSize + VARCHAR_LENGTH_SIZE;
            break;
        }
    }

    return size;
}
RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data)
{
    int offset = 0;
    int nullFieldSize = getNullIndicatorSize(recordDescriptor.size());
    char *nullFlags = (char *)malloc(nullFieldSize);
    int fieldPosition = 0;
    void *record = malloc(PAGE_SIZE);
    RC result = readRecord(fileHandle, recordDescriptor, rid, record);
    if (result != SUCCESS)
    {
        free(record);
        free(nullFlags);
        return result;
    }
    memcpy(nullFlags, record, nullFieldSize);
    offset += nullFieldSize;
    for (Attribute attr : recordDescriptor)
    {
        if (fieldIsNull(nullFlags, fieldPosition))
        {
            //If desired filed is null, set null bit and return
            if (attr.name.compare(attributeName) == 0)
            {
                unsigned char nullFlag = 1 << 7;
                memcpy(data, &nullFlag, sizeof(unsigned char));
                free(record);
                free(nullFlags);
                return SUCCESS;
            }
            //Otherwise increment fieldPosition and continue
            ++fieldPosition;
            continue;
        }
        switch (attr.type)
        {
        case TypeInt:
        case TypeReal:
            //If this is the field we want, copy it to data with a 0 null bit and return
            if (attr.name.compare(attributeName) == 0)
            {
                unsigned char nullFlag = 0;
                memcpy(data, &nullFlag, sizeof(unsigned char));
                memcpy((char *)data + sizeof(unsigned char), (char *)record + offset, sizeof(uint32_t));
                offset += sizeof(uint32_t);
                free(record);
                free(nullFlags);
                return SUCCESS;
            }
            else
            {
                offset += sizeof(uint32_t);
            }
            break;
        case TypeVarChar:
            //Get the size of the string
            int sizeOfString = 0;
            memcpy(&sizeOfString, (char *)record + offset, sizeof(uint32_t));
            offset += sizeof(uint32_t);
            //If this is the field we want, copy it to data with a 0 null bit, followed by the length, followed by the string
            if (attr.name.compare(attributeName) == 0)
            {
                unsigned char nullFlag = 0;
                memcpy(data, &nullFlag, sizeof(unsigned char));
                memcpy((char *)data + sizeof(unsigned char), &sizeOfString, sizeof(uint32_t));
                memcpy((char *)data + sizeof(unsigned char) + sizeof(uint32_t), (char *)record + offset, sizeOfString);
                offset += sizeOfString;
                free(record);
                free(nullFlags);
                return SUCCESS;
            }
            else
            {
                offset += sizeOfString;
            }
            break;
        }
        fieldPosition++;
    }
    free(nullFlags);
    free(record);
    return -1;
}
// Calculate actual bytes for null-indicator for the given field counts
int RecordBasedFileManager::getNullIndicatorSize(int fieldCount)
{
    return int(ceil((double)fieldCount / CHAR_BIT));
}

bool RecordBasedFileManager::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

void RecordBasedFileManager::setFieldToNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    nullIndicator[indicatorIndex] |= indicatorMask;
}

// Computes the free space of a page (function of the free space pointer and the slot directory size).
unsigned RecordBasedFileManager::getPageFreeSpaceSize(void *page)
{
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
    return slotHeader.freeSpaceOffset - slotHeader.recordEntriesNumber * sizeof(SlotDirectoryRecordEntry) - sizeof(SlotDirectoryHeader);
}

// Support header size and null indicator. If size is less than recordDescriptor size, then trailing records are null
void RecordBasedFileManager::getRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, void *data)
{
    // Pointer to start of record
    char *start = (char *)page + offset;

    // Allocate space for null indicator.
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy(&len, start, sizeof(RecordLength));
    int recordNullIndicatorSize = getNullIndicatorSize(len);

    // Read in the existing null indicator
    memcpy(nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

    // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
    for (unsigned i = len; i < recordDescriptor.size(); i++)
    {
        int indicatorIndex = (i + 1) / CHAR_BIT;
        int indicatorMask = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        nullIndicator[indicatorIndex] |= indicatorMask;
    }
    // Write out null indicator
    memcpy(data, nullIndicator, nullIndicatorSize);

    // Initialize some offsets
    // rec_offset: points to data in the record. We move this forward as we read data from our record
    unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
    // data_offset: points to our current place in the output data. We move this forward as we write to data.
    unsigned data_offset = nullIndicatorSize;
    // directory_base: points to the start of our directory of indices
    char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;

    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        if (fieldIsNull(nullIndicator, i))
            continue;

        // Grab pointer to end of this column
        ColumnOffset endPointer;
        memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));

        // rec_offset keeps track of start of column, so end-start = total size
        uint32_t fieldSize = endPointer - rec_offset;

        // Special case for varchar, we must give data the size of varchar first
        if (recordDescriptor[i].type == TypeVarChar)
        {
            memcpy((char *)data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
            data_offset += VARCHAR_LENGTH_SIZE;
        }
        // Next we copy bytes equal to the size of the field and increase our offsets
        memcpy((char *)data + data_offset, start + rec_offset, fieldSize);
        rec_offset += fieldSize;
        data_offset += fieldSize;
    }
}

void RecordBasedFileManager::setRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char *)data, nullIndicatorSize);

    // Points to start of record
    char *start = (char *)page + offset;

    // Offset into *data
    unsigned data_offset = nullIndicatorSize;
    // Offset into page header
    unsigned header_offset = 0;

    RecordLength len = recordDescriptor.size();
    memcpy(start + header_offset, &len, sizeof(len));
    header_offset += sizeof(len);

    memcpy(start + header_offset, nullIndicator, nullIndicatorSize);
    header_offset += nullIndicatorSize;

    // Keeps track of the offset of each record
    // Offset is relative to the start of the record and points to the END of a field
    ColumnOffset rec_offset = header_offset + (recordDescriptor.size()) * sizeof(ColumnOffset);

    unsigned i = 0;
    for (i = 0; i < recordDescriptor.size(); i++)
    {
        if (!fieldIsNull(nullIndicator, i))
        {
            // Points to current position in *data
            char *data_start = (char *)data + data_offset;

            // Read in the data for the next column, point rec_offset to end of newly inserted data
            switch (recordDescriptor[i].type)
            {
            case TypeInt:
                memcpy(start + rec_offset, data_start, INT_SIZE);
                rec_offset += INT_SIZE;
                data_offset += INT_SIZE;
                break;
            case TypeReal:
                memcpy(start + rec_offset, data_start, REAL_SIZE);
                rec_offset += REAL_SIZE;
                data_offset += REAL_SIZE;
                break;
            case TypeVarChar:
                unsigned varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, data_start, VARCHAR_LENGTH_SIZE);
                memcpy(start + rec_offset, data_start + VARCHAR_LENGTH_SIZE, varcharSize);
                // We also have to account for the overhead given by that integer.
                rec_offset += varcharSize;
                data_offset += VARCHAR_LENGTH_SIZE + varcharSize;
                break;
            }
        }
        // Copy offset into record header
        // Offset is relative to the start of the record and points to END of field
        memcpy(start + header_offset, &rec_offset, sizeof(ColumnOffset));
        header_offset += sizeof(ColumnOffset);
    }
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
    void *pageData = calloc(PAGE_SIZE, sizeof(char));
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;

    auto rc = fileHandle.readPage(rid.pageNum, pageData);
    if (rc != SUCCESS)
    {
        free(pageData);
        return RBFM_READ_FAILED;
    }

    SlotDirectoryHeader directoryHeader = getSlotDirectoryHeader(pageData);
    if (rid.slotNum >= directoryHeader.recordEntriesNumber)
    {
        free(pageData);
        return RBFM_SLOT_DN_EXIST;
    }

    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    if (recordEntry.offset < 0)
    {
        free(pageData);
        return RBFM_SLOT_DN_EXIST;
    }

    if (isSlotForwarding(recordEntry))
    {
        RID new_rid = getRID(recordEntry);
        rc = deleteRecord(fileHandle, recordDescriptor, new_rid); // Jump to our forwarded location and delete there.
        if (rc != SUCCESS)
            return rc;

        recordEntry.offset = -1; // Clean up forwarding by marking the slot as empty.
        markSlotAsTerminal(recordEntry);
        setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);
        free(pageData);
        return SUCCESS;
    }

    const auto gainedFreeSpace = recordEntry.length;

    const bool adjacent_to_free_space = recordEntry.offset == directoryHeader.freeSpaceOffset;
    if (!adjacent_to_free_space)
    {
        // Shift all records after the deleted record into the new hole.
        memmove(
            (char *)pageData + directoryHeader.freeSpaceOffset + gainedFreeSpace,
            (char *)pageData + directoryHeader.freeSpaceOffset,
            recordEntry.offset - directoryHeader.freeSpaceOffset);
    }

    for (int i = 0; i < directoryHeader.recordEntriesNumber; i++)
    {
        SlotDirectoryRecordEntry entry_i = getSlotDirectoryRecordEntry(pageData, i);

        bool entryIsInPage = entry_i.offset >= 0 && !isSlotForwarding(entry_i);
        bool entryIsBeforeDeletion = entry_i.offset < recordEntry.offset;
        bool mustShiftOffset = entryIsInPage && entryIsBeforeDeletion;
        if (!mustShiftOffset)
            continue;

        entry_i.offset += recordEntry.length;
        setSlotDirectoryRecordEntry(pageData, i, entry_i);
    }

    // Clear any old data to reclaim our free space.
    //const auto new_free_space_length = recordEntry.length;
    //const auto new_free_space_offset = directoryHeader.freeSpaceOffset - new_free_space_length;
    //memset((char *) pageData + new_free_space_offset, 0, new_free_space_length);
    memset((char *)pageData + directoryHeader.freeSpaceOffset, 0, gainedFreeSpace);

    //directoryHeader.freeSpaceOffset = new_free_space_offset; // Update free space offset.
    directoryHeader.freeSpaceOffset += gainedFreeSpace; // Update free space offset.
    setSlotDirectoryHeader(pageData, directoryHeader);

    recordEntry.offset = -1; // Invalidate record offset.
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);

    if (fileHandle.writePage(rid.pageNum, pageData))
        return RBFM_WRITE_FAILED;

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
    auto rc = deleteRecord(fileHandle, recordDescriptor, rid);
    if (rc != SUCCESS)
        return rc;

    RID newRID;
    rc = insertRecord(fileHandle, recordDescriptor, data, newRID);
    if (rc != SUCCESS)
        return rc;

    if (newRID == rid) // No forwarding, we're already in the right location.
        return SUCCESS;

    // Otherwise, fix the link s.t given RID forwards to new RID.

    // Get page for given RID (which is the slot we must forward).
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
    {
        return RBFM_MALLOC_FAILED;
    }
    rc = fileHandle.readPage(rid.pageNum, pageData);
    if (rc != SUCCESS)
        return rc;

    // Overwrite slot with forwarding entry.
    SlotDirectoryRecordEntry forwardingEntry = getRecordEntry(newRID);
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, forwardingEntry);
    rc = fileHandle.writePage(rid.pageNum, pageData);
    free(pageData);
    return rc;
}

int32_t RecordBasedFileManager::findEmptySlot(void *pageData)
{
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    for (uint16_t i = 0; i < slotHeader.recordEntriesNumber; i++)
    {
        SlotDirectoryRecordEntry entry = getSlotDirectoryRecordEntry(pageData, i);
        bool slotIsEmpty = entry.offset < 0;
        if (slotIsEmpty)
        {
            return i;
        }
    }
    return -1;
}

RC RecordBasedFileManager::scan(
    FileHandle &fileHandle,
    const vector<Attribute> &recordDescriptor,
    const string &conditionAttributeName,
    const CompOp compOp,                  // comparision type such as "<" and "="
    const void *value,                    // used in the comparison
    const vector<string> &attributeNames, // a list of projected attributes (TODO: note the ORDER of projected attrs)
    RBFM_ScanIterator &rbfm_ScanIterator)
{
    return rbfm_ScanIterator.load(instance(), fileHandle, recordDescriptor, conditionAttributeName, compOp, value, attributeNames);
}

// Like printRecord() but we ignore the values that are not of the target attribute.
// This will allocate space on the heap for the value.
RC RBFM_ScanIterator::getValueFromRecord(void *data, const vector<Attribute> recordDescriptor, const string targetAttrName, void *&value)
{
    // Parse the null indicator and save it into an array
    int nullIndicatorSize = rbfm_->getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, data, nullIndicatorSize);

    // We've read in the null indicator, so we can skip past it now
    unsigned offset = nullIndicatorSize;

    for (unsigned i = 0; i < (unsigned)recordDescriptor.size(); i++)
    {
        bool isNull = rbfm_->fieldIsNull(nullIndicator, i);
        if (isNull)
            return RBFM_SI_NULL_VALUE;

        bool isTargetAttr = recordDescriptor[i].name.compare(targetAttrName) == 0;

        switch (recordDescriptor[i].type)
        {
        case TypeInt:
            uint32_t data_integer;
            memcpy(&data_integer, ((char *)data + offset), INT_SIZE);
            offset += INT_SIZE;

            if (isTargetAttr)
            {
                value = malloc(INT_SIZE);
                memcpy(value, &data_integer, INT_SIZE);
                return SUCCESS;
            }
            break;

        case TypeReal:
            float data_real;
            memcpy(&data_real, ((char *)data + offset), REAL_SIZE);
            offset += REAL_SIZE;

            if (isTargetAttr)
            {
                value = malloc(REAL_SIZE);
                memcpy(value, &data_real, REAL_SIZE);
                return SUCCESS;
            }
            break;

        case TypeVarChar:
            // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
            uint32_t varcharSize;
            memcpy(&varcharSize, ((char *)data + offset), VARCHAR_LENGTH_SIZE);
            offset += VARCHAR_LENGTH_SIZE;

            // Gets the actual string.
            char *data_string = (char *)malloc(varcharSize + 1);
            if (data_string == NULL)
                return RBFM_MALLOC_FAILED;
            memcpy(data_string, ((char *)data + offset), varcharSize);

            // Adds the string terminator.
            data_string[varcharSize] = '\0';
            offset += varcharSize;

            if (isTargetAttr)
            {
                value = data_string;
                return SUCCESS;
            }
            free(data_string);
            break;
        }
    }
    return RBFM_SI_NO_VALUE_IN_RECORD;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{
    if (!paramsLoaded_)
        return RBFM_SI_UNLOADED;
    if (iteratorClosed_)
        return RBFM_SI_CLOSED;

    // Search for first matching record.
    int rc;
    const uint32_t npages = fileHandle_.getNumberOfPages();
    const uint32_t startingPage = lastRIDInitialized_ ? lastRID_.pageNum : 0;
    for (uint32_t page_i = startingPage; page_i < npages; page_i++)
    {
        void *pageData = calloc(PAGE_SIZE, sizeof(uint8_t));
        if (pageData == NULL)
            return RBFM_MALLOC_FAILED;
        rc = fileHandle_.readPage(page_i, pageData);
        if (rc != SUCCESS)
        {
            free(pageData);
            return rc;
        }
        SlotDirectoryHeader header = rbfm_->getSlotDirectoryHeader(pageData);

        const uint32_t startingSlot = lastRIDInitialized_ && page_i == lastRID_.pageNum ? lastRID_.slotNum + 1 : 0;
        for (uint32_t slot_j = startingSlot; slot_j < header.recordEntriesNumber; slot_j++)
        {
            SlotDirectoryRecordEntry entry = rbfm_->getSlotDirectoryRecordEntry(pageData, slot_j);

            bool isSlotEmpty = entry.offset < 0;
            bool recordNotInPage = isSlotForwarding(entry) || isSlotEmpty;
            if (recordNotInPage)
                continue;

            rid.pageNum = page_i;
            rid.slotNum = slot_j;
            vector<Attribute> recordDescriptor;
            void *record = calloc(PAGE_SIZE, sizeof(uint8_t));
            rc = rbfm_->readRecord(fileHandle_, recordDescriptor_, rid, record);
            if (rc != SUCCESS)
            {
                free(record);
                free(pageData);
                return rc;
            }


            bool recordMatchesSearchCriteria = false; // Assume no match until proven otherwise.
            void *value = nullptr;

            if (compOp_ == NO_OP)
                goto recordMatches;

            rc = getValueFromRecord(record, recordDescriptor_, conditionAttribute_.name, value);
            if (rc != SUCCESS && rc != RBFM_SI_NULL_VALUE)
            {
                free(record);
                free(pageData);
                return rc;
            }

            if (rc == RBFM_SI_NULL_VALUE)
            {
                recordMatchesSearchCriteria = false;
            }
            else 
            {
                recordMatchesSearchCriteria = evalCompOp(value, value_, compOp_, conditionAttribute_.type);
            }

            if (value != nullptr)
                free(value);

            if (recordMatchesSearchCriteria)
            {
recordMatches:
                lastRID_.pageNum = page_i;
                lastRID_.slotNum = slot_j;
                lastRIDInitialized_ = true;

                int projectedSize;
                void *projectedRecord;
                vector<Attribute> projectedRecordDescriptor;
                rc = projectRecord(record, recordDescriptor_, projectedRecord, projectedRecordDescriptor, projectedSize, attributeNames_);

                memcpy(data, projectedRecord, projectedSize);

                free(projectedRecord);
                free(record);
                free(pageData);
                return rc;
            }
            free(record);
        }
        free(pageData);
    }

    // No matches found.
    return RBFM_EOF;
}

// TODO: Think about this some more.  Not sure what else we may need to do here.
RC RBFM_ScanIterator::close()
{
    /*auto rc = rbfm_->closeFile(fileHandle_);
    if (rc != SUCCESS)
        return rc;
    */

    iteratorClosed_ = true;
    return SUCCESS;
}

RC RBFM_ScanIterator::load(
    RecordBasedFileManager *rbfm,
    FileHandle &fileHandle,
    const vector<Attribute> recordDescriptor,
    const string conditionAttributeName,
    const CompOp compOp,                // comparision type such as "<" and "="
    const void *value,                  // used in the comparison
    const vector<string> attributeNames // a list of projected attributes
)
{
    rbfm_ = rbfm->instance();
    fileHandle_ = fileHandle;
    recordDescriptor_ = recordDescriptor;
    compOp_ = compOp;
    value_ = const_cast<void *>(value);
    attributeNames_ = attributeNames;

    lastRIDInitialized_ = false;
    paramsLoaded_ = true;
    iteratorClosed_ = false;

    // Check that all requested projected attributes are valid.
    for (size_t i = 0; i < attributeNames.size(); i++)
    {
        bool foundAttribute = false;
        for (size_t j = 0; j < recordDescriptor.size(); j++)
        {
            if (attributeNames[i] == recordDescriptor[j].name)
                foundAttribute = true;
        }

        if (!foundAttribute)
            return -1;
    }

    if (compOp == NO_OP)
        return SUCCESS;

    // Check that we can compare on some attribute.
    for (size_t i = 0; i < recordDescriptor.size(); i++)
    {
        bool attrNamesMatch = recordDescriptor[i].name.compare(conditionAttributeName) == 0;
        if (attrNamesMatch)
        {
            conditionAttribute_ = recordDescriptor[i];
            return SUCCESS;
        }
    }

    return -1;
}

RC RBFM_ScanIterator::projectRecord(const void *data_og, const vector<Attribute> recordDescriptor_og,
                                    void *&data_pj, vector<Attribute> &recordDescriptor_pj, int &size_pj,
                                    const vector<string> projectedAttributeNames)
{
    if (!paramsLoaded_)
        return RBFM_SI_UNLOADED;

    recordDescriptor_pj.clear();
    data_pj = malloc(PAGE_SIZE);

    // Parse the null indicator and save it into an array
    int nullIndicatorSize_og = rbfm_->getNullIndicatorSize(recordDescriptor_og.size());
    char nullIndicator_og[nullIndicatorSize_og];
    memset(nullIndicator_og, 0, nullIndicatorSize_og);
    memcpy(nullIndicator_og, data_og, nullIndicatorSize_og);

    // Setup null indicator for projected record.
    // Note: we still need to fill this in with null fields.
    int nullIndicatorSize_pj = rbfm_->getNullIndicatorSize(projectedAttributeNames.size());
    char nullIndicator_pj[nullIndicatorSize_pj];
    memset(nullIndicator_pj, 0, nullIndicatorSize_pj);

    // We've read in the null indicator, so we can skip past it now
    unsigned offset_og = nullIndicatorSize_og;
    unsigned offset_pj = nullIndicatorSize_pj;
    unsigned nullIndicatorField_pj = 0; // We'll build our projected nullIndicator as we go.

    // For each original attribute, if it's a projected attribute, add it to the projected data.
    for (unsigned i = 0; i < (unsigned)recordDescriptor_og.size(); i++)
    {
        // Check if this is something we should be projecting.
        bool shouldProject = false;
        for (auto attrName_pj : projectedAttributeNames)
        {
            bool attrNamesMatch = attrName_pj.compare(recordDescriptor_og[i].name) == 0;
            if (attrNamesMatch)
            {
                shouldProject = true;
                break;
            }
        }

        auto currentNullIndicatorField_pj = nullIndicatorField_pj;
        if (shouldProject)
        {
            recordDescriptor_pj.push_back(recordDescriptor_og[i]);
            nullIndicatorField_pj++;
        }


        bool isNull = rbfm_->fieldIsNull(nullIndicator_og, i);
        if (isNull)
        {
            if (shouldProject)
            {
                rbfm_->setFieldToNull(nullIndicator_pj, currentNullIndicatorField_pj);
            }
            continue;
        }
        switch (recordDescriptor_og[i].type)
        {
        case TypeInt:
            if (shouldProject)
            {
                memcpy((char *)data_pj + offset_pj, (char *)data_og + offset_og, INT_SIZE);
                offset_pj += INT_SIZE;
            }
            offset_og += INT_SIZE;
            break;

        case TypeReal:
            if (shouldProject)
            {
                memcpy((char *)data_pj + offset_pj, (char *)data_og + offset_og, REAL_SIZE);
                offset_pj += REAL_SIZE;
            }
            offset_og += REAL_SIZE;
            break;

        case TypeVarChar:
            // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
            uint32_t varcharSize;
            memcpy(&varcharSize, (char *)data_og + offset_og, VARCHAR_LENGTH_SIZE);
            if (shouldProject)
            {
                memcpy((char *)data_pj + offset_pj, (char *)data_og + offset_og, VARCHAR_LENGTH_SIZE);
                offset_pj += VARCHAR_LENGTH_SIZE;
            }
            offset_og += VARCHAR_LENGTH_SIZE;

            // Gets the actual string.
            if (shouldProject)
            {
                memcpy((char *)data_pj + offset_pj, (char *)data_og + offset_og, varcharSize);
                offset_pj += varcharSize;
            }
            offset_og += varcharSize;

            break;
        }
    }

    size_pj = offset_pj;

    memcpy(data_pj, nullIndicator_pj, nullIndicatorSize_pj);

    // Then ensure that each attribute we wanted to project has been hit.
    // We may need to ensure that they are in the intended projected order (by order of the elements in vector).

    return SUCCESS;
}

RC RBFM_ScanIterator::reset()
{
    if (!paramsLoaded_)
        return RBFM_SI_UNLOADED;
    if (iteratorClosed_)
        return RBFM_SI_CLOSED;

    // Start scanning from the beginning.
    lastRID_.pageNum = 0;
    lastRID_.slotNum = 0;
    lastRIDInitialized_ = true;
    return SUCCESS;
}

uint32_t getForwardingMask(const SlotDirectoryRecordEntry recordEntry)
{
    const auto nbits_length = sizeof(recordEntry.length) * CHAR_BIT;
    const auto last_bit_position = nbits_length - 1;
    return 1 << last_bit_position;
}

void markSlotAsForwarding(SlotDirectoryRecordEntry &recordEntry)
{
    recordEntry.length |= getForwardingMask(recordEntry);
}

void markSlotAsTerminal(SlotDirectoryRecordEntry &recordEntry)
{
    recordEntry.length &= (~getForwardingMask(recordEntry));
}

bool isSlotForwarding(const SlotDirectoryRecordEntry recordEntry)
{
    const auto fwd = recordEntry.length & getForwardingMask(recordEntry);
    return fwd != 0;
}

RID getRID(SlotDirectoryRecordEntry recordEntry)
{
    markSlotAsTerminal(recordEntry); // We don't want the fwd bit to impact our new slot number.
    RID new_rid;
    new_rid.pageNum = recordEntry.offset;
    new_rid.slotNum = recordEntry.length;
    return new_rid;
}

SlotDirectoryRecordEntry getRecordEntry(const RID rid)
{
    SlotDirectoryRecordEntry recordEntry;
    recordEntry.offset = rid.pageNum;
    recordEntry.length = rid.slotNum;
    markSlotAsForwarding(recordEntry);
    return recordEntry;
}

void RecordBasedFileManager::printHeaderAndAllRecordEntries(FileHandle &fileHandle)
{
    for (unsigned int page_i = 0; page_i < fileHandle.getNumberOfPages(); page_i++)
    {
        void *page = malloc(PAGE_SIZE);
        fileHandle.readPage(page_i, page);
        SlotDirectoryHeader header = getSlotDirectoryHeader(page);
        cout << "header " << page_i << " = {freeSpaceOffset: " << header.freeSpaceOffset << ", recordEntriesNumber: " << header.recordEntriesNumber << "}" << endl;
        for (unsigned int slot_j = 0; slot_j < header.recordEntriesNumber; slot_j++)
        {
            SlotDirectoryRecordEntry entry = getSlotDirectoryRecordEntry(page, slot_j);
            cout << "    entry " << slot_j << " = {length: " << entry.length << ", offset: " << entry.offset << "}" << endl;
        }
        cout << endl;
        free(page);
    }
}
