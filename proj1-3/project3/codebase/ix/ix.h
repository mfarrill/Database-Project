#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <algorithm>
#include <stdio.h>
#include <string.h>

#include "../rbf/rbfm.h"

const size_t SIZEOF_IS_LEAF = sizeof(bool);
const size_t SIZEOF_NUM_ENTRIES = sizeof(uint32_t);
const size_t SIZEOF_FREE_SPACE_OFFSET = sizeof(uint32_t);
const size_t SIZEOF_SIBLING_PAGENUM = sizeof(PageNum);
const size_t SIZEOF_CHILD_PAGENUM = sizeof(PageNum);

const size_t SIZEOF_HEADER_LEAF = SIZEOF_IS_LEAF + SIZEOF_NUM_ENTRIES + SIZEOF_FREE_SPACE_OFFSET + (SIZEOF_SIBLING_PAGENUM * 2);
const size_t SIZEOF_HEADER_INTERIOR = SIZEOF_IS_LEAF + SIZEOF_NUM_ENTRIES + SIZEOF_FREE_SPACE_OFFSET;

// Byte position in page where each field must be accessed.
const size_t POSITION_IS_LEAF = 0;
const size_t POSITION_NUM_ENTRIES = POSITION_IS_LEAF + SIZEOF_IS_LEAF;
const size_t POSITION_FREE_SPACE_OFFSET = POSITION_NUM_ENTRIES + SIZEOF_NUM_ENTRIES;
const size_t POSITION_CHILD_PAGENUM = POSITION_FREE_SPACE_OFFSET + SIZEOF_FREE_SPACE_OFFSET;
const size_t POSITION_SIBLING_PAGENUM_LEFT = POSITION_FREE_SPACE_OFFSET + SIZEOF_FREE_SPACE_OFFSET;
const size_t POSITION_SIBLING_PAGENUM_RIGHT = POSITION_SIBLING_PAGENUM_LEFT + SIZEOF_SIBLING_PAGENUM;

const int IX_EOF(-1); // end of the index scan
const int IX_SI_CLOSED(-2);
const int IX_FREE_SPACE_OFFSET_INVALID(-3);
const int IX_NODE_NOT_CHILD_OF_PARENT(-4);
const int IX_NO_SIBLINGS(-5);
const int IX_CANT_REDISTRIBUTE(-6);

// Headers for leaf nodes and internal nodes
typedef struct
{
    uint32_t numEntries;
    uint32_t freeSpaceOffset;
} HeaderInterior;

typedef struct
{
    uint32_t numEntries;
    uint32_t freeSpaceOffset;
    int leftSibling;
    int rightSibling;
} HeaderLeaf;

class IX_ScanIterator;
class IXFileHandle;
class IndexManager
{

public:
    static IndexManager *instance();

    static vector<tuple<void *, int>> getKeysWithSizes_interior(const Attribute attribute, const void *pageData);
    static vector<int> getChildPointers_interior(const Attribute attribute, const void *pageData);

    static vector<tuple<void *, int>> getDataEntriesWithSizes_leaf(const Attribute attribute, const void *pageData);
    static vector<tuple<void *, int>> getKeysWithSizes_leaf(const Attribute attribute, vector<tuple<void *, int>> dataEntriesWithSizes);
    static vector<RID> getRIDs_leaf(const Attribute attribute, vector<tuple<void *, int>> dataEntriesWithSizes);

    static tuple<void *, int> getKeyDataWithSize(const Attribute attribute, const void *key);

    static RC getHeaderInterior(const void *pageData, HeaderInterior &header);
    static void setHeaderInterior(void *pageData, const HeaderInterior header);

    static RC getHeaderLeaf(const void *pageData, HeaderLeaf &header);
    static void setHeaderLeaf(void *pageData, const HeaderLeaf header);

    // Create an index file.
    RC createFile(const string &fileName);

    // Delete an index file.
    RC destroyFile(const string &fileName);

    // Open an index and return an ixfileHandle.
    RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

    // Close an ixfileHandle for an index.
    RC closeFile(IXFileHandle &ixfileHandle);

    // Insert an entry into the given index that is indicated by the given ixfileHandle.
    RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

    // Delete an entry from the given index that is indicated by the given ixfileHandle.
    RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);
    RC deleteEntry_subtree(IXFileHandle &ixfileHandle,
                           const Attribute attribute,
                           const void *keyToDelete,
                           const RID &ridToDelete,
                           void *&oldChildEntry,
                           const int parentNodePageNum,
                           const int currentNodePageNum);
    RC deleteEntry_leaf(IXFileHandle &ixfileHandle,
                        const Attribute attribute,
                        const void *keyToDelete,
                        const RID &ridToDelete,
                        void *&oldChildEntry,
                        const int parentNodePageNum,
                        const int currentNodePageNum);
    RC deleteEntry_interior(IXFileHandle &ixfileHandle,
                            const Attribute attribute,
                            const void *keyToDelete,
                            const RID &ridToDelete,
                            void *&oldChildEntry,
                            const int parentNodePageNum,
                            const int currentNodePageNum);

    // Initialize and IX_ScanIterator to support a range search
    RC scan(IXFileHandle &ixfileHandle,
            const Attribute &attribute,
            const void *lowKey,
            const void *highKey,
            bool lowKeyInclusive,
            bool highKeyInclusive,
            IX_ScanIterator &ix_ScanIterator);
    RC scan_by_pageNumber(IXFileHandle &ixfileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator,
                          PageNum pageNumber);

    // Print the B+ tree in pre-order (in a JSON record format)
    void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;
    static void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute, uint32_t depth, PageNum pageNumber);
    static void printLeaf(IXFileHandle &ixfileHandle, const Attribute &attribute, uint32_t depth, const void *pageData);
    static void printInterior(IXFileHandle &ixfileHandle, const Attribute &attribute, uint32_t depth, const void *pageData);

    //Pre: page is a pointer to page data of any type
    //Post: the truth value of whether the page parameter is a leaf
    static bool isLeafPage(const void *page);

    static RC willNodeBeUnderfull(const void *nodePageData, size_t entrySize, bool &willBeUnderfull, size_t &spaceUntilNotUnderfull);

    static int findIndexOfKeyWithRID(const Attribute attribute,
                                     const vector<tuple<void *, int>> keysWithSizes,
                                     const vector<RID> rids,
                                     const void *targetKey,
                                     const RID targetRID);

    RC redistributeEntries(const Attribute attribute, void *parentNodePageData, void *srcNodePageData, void *&srcNodePageData_copy, void *dstNodePageData, void *&dstNodePageData_copy, int srcNodeIndex, int dstNodeIndex, size_t dstSpaceNeeded);

protected:
    IndexManager();
    ~IndexManager();

private:
    static IndexManager *_index_manager;
    static PagedFileManager *_pf_manager;
    string fileName;
    PageNum rootPage;

    RC createEmptyPage(IXFileHandle &index_file, void *page, bool isLeafPage, PageNum &pageNumber, int leftSibling = -1, int rightSibling = -1);

    static uint32_t findNumberOfEntries(const void *page);

    //Pre: val contains a valid leaf entry to be inserted and attr corresponding to that entry
    //Post: return the total size of the entry including size of RID
    static int findLeafEntrySize(const void *val, const Attribute attr);

    //Pre: val contains a valid entry that will be inserted to a non-leaf page and attr corresponding to that entry
    //Post: return the total size of the entry which will be equal to the key size
    static int findInteriorEntrySize(const void *val, const Attribute attr);

    static int findKeySize(const void *val, const Attribute attr);

    //Pre: page is a pointer to a page data of any type
    //Post: the offset of the page’s free space is returned
    static RC findFreeSpaceOffset(const void *page, size_t &freeSpaceOffset);

    //Pre: val contains a valid entry to be inserted either to a Leaf or Non-Leaf page and attr corresponding to that entry.
    //     &Page is a pointer to a FileHandle of a page
    //Post: returns whether or not the given val will fit on the given page.
    static RC willEntryFit(const void *pageData, const void *val, const Attribute attr, bool isLeafValue, bool &willFit);

    //Pre: val contains a valid entry to be inserted and attr coresponding to that entry. pageData is a
    //      page that contains traffic cops
    //Post: returns the page number of the next page to visit
    RC findTrafficCop(const void *val, const Attribute attr, const void *pageData, int &trafficCop);

    //Pre: *page contains the page where *key will be written. attr corresponds to key and isLeafNode tells
    //      whether page is a leaf page
    //Post: *page will be searched and key (which is in the correct format) will be placed in the correct position
    RC insertEntryInPage(void *page, const void *key, const RID &rid, const Attribute &attr, bool isLeafNodeconst, int rightChild = -1);

    RC updateRoot(IXFileHandle &IXFileHandle, tuple<void *, int> newChild, int leftChild, const Attribute &attr);

    RC splitPage(void *prevPage, void *newPage, int prevPageNumber, int newPageNumber, const Attribute &attribute, tuple<void *, int> &newChildEntry, bool isLeafPage);

    RC getNextEntry(void *page, uint32_t &currentOffset, uint32_t &entryCount, void *fieldValue, void *slotData, const Attribute attr, bool isLeafPage);

    RC insertToTree(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, int nodePointer, tuple<void *, int> &newChild);

    bool isRoot(PageNum pageNumber);
    bool areRIDsEqual(const RID &rid1, const RID &rid2);
    RC getRootPageNumber(const string indexFileName);
    RC updateRootPageNumber(const string indexFileName, const PageNum newRoot);
    RC getClosestSiblings(const Attribute attribute, const void *parentPageData, vector<tuple<int, int>> &siblings_PageNumWithIndex, const int myNodePageNum, int &myIndex);
    RC findSubtree(IXFileHandle &ixFileHandle, const void *key, const Attribute &attribute, int nodePageNumber, int &leafPageNumber);
};

class IXFileHandle
{
public:
    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;
    string fileName;
    FileHandle *ufh;

    // Read, write, and append functions to update counters
    RC readPage(PageNum pageNum, void *data);
    RC writePage(PageNum pageNum, const void *data);
    RC appendPage(const void *data);

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    // Put the current counter values of associated PF FileHandles into variables
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
};

class IX_ScanIterator
{
public:
    bool closed_;

    IXFileHandle ixfileHandle_;
    Attribute attribute_;
    void *lowKey_;
    void *highKey_;
    bool lowKeyInclusive_;
    bool highKeyInclusive_;

    void *currentPageData_;
    uint32_t numEntriesReadInPage_;

    // Constructor
    IX_ScanIterator();

    // Destructor
    ~IX_ScanIterator();

    // Get next matching entry
    RC getNextEntry(RID &rid, void *key);

    RC open(IXFileHandle &ixfileHandle,
            Attribute attribute,
            void *lowKey,
            void *highKey,
            bool lowKeyInclusive,
            bool highKeyInclusive,
            void *pageData);

    // Terminate index scan
    RC close();
};

RC compareKeyData(const Attribute attr, const void *keyData1, const void *keyData2, bool &lt, bool &eq, bool &gt);
void freeKeysWithSizes(vector<tuple<void *, int>> keysWithSizes);
void freeDataEntriesWithSizes(vector<tuple<void *, int>> dataEntriesWithSizes);

#endif
