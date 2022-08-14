#ifndef _pfm_h_
#define _pfm_h_

typedef unsigned PageNum;
typedef int RC;
typedef char byte;

#define PAGE_SIZE 4096
#include <string>
#include <climits>
#include <cstdio>
#include <fstream>
#include <memory>
#include <iostream>
#include <sys/stat.h>
using namespace std;

class FileHandle;

class PagedFileManager
{
public:
    static PagedFileManager* instance();                                // Access to the _pf_manager instance

    RC createFile    (const string &fileName);                          // Create a new file
    RC destroyFile   (const string &fileName);                          // Destroy a file
    RC openFile      (const string &fileName, FileHandle &fileHandle);  // Open a file
    RC closeFile     (FileHandle &fileHandle);                          // Close a file

protected:
    PagedFileManager();                                                 // Constructor
    ~PagedFileManager();                                                // Destructor

private:
    static PagedFileManager *_pf_manager;
};


class FileHandle
{
public:
    // variables to keep the counter for each operation
    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;
    unique_ptr<fstream> fs;
    string fname; // To help identify the file fs is handling.
    
    FileHandle(): readPageCounter{0}, writePageCounter{0}, appendPageCounter{0}, fs{nullptr}, fname{} {}
    ~FileHandle() {
        if (isHandling()) {
            fs->close();
        }
    }

    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();                                        // Get the number of pages in the file
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);  // Put the current counter values into variables

    bool isHandling() {
        return fs != nullptr && fs->is_open();
    }
}; 

#endif
