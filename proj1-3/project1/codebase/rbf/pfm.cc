#include "pfm.h"
#include <memory>
#include <fstream>

bool fileExists(const string &fileName)
{
    struct stat buf;
    return stat(fileName.c_str(), &buf) == 0 ? true : false;
}

unsigned long getPageStartingByteOffset(PageNum pageNum)
{
    return pageNum * PAGE_SIZE;
}

PagedFileManager *PagedFileManager::_pf_manager = 0;

PagedFileManager *PagedFileManager::instance()
{
    if (!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}

PagedFileManager::PagedFileManager()
{
}

PagedFileManager::~PagedFileManager()
{
}

RC PagedFileManager::createFile(const string &fileName)
{
    if (fileExists(fileName) || !ofstream(fileName.c_str(), ofstream::out | ofstream::binary))
    {
        return -1;
    }
    return 0;
}

RC PagedFileManager::destroyFile(const string &fileName)
{
    if (!fileExists(fileName))
    {
        return -1;
    }
    // Remove returns non-zero value on failure.
    return remove(fileName.c_str());
}

RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    if (!fileExists(fileName))
    {
        return -1;
    }

    auto fsMode = fstream::in | fstream::out | fstream::binary;
    unique_ptr<fstream> fs{new fstream(fileName.c_str(), fsMode)};
    if (!fs)
    {
        return -1;
    }

    fileHandle.fs = std::move(fs);
    fileHandle.fname = fileName;
    return 0;
}

RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    if (!fileHandle.isHandling())
    {
        return -1;
    }

    fileHandle.fs->close();
    fileHandle.fs = nullptr;
    fileHandle.fname.clear();
    return 0;
}

RC FileHandle::readPage(PageNum pageNum, void *data)
{
    if (!isHandling())
    {
        return -1;
    }

    if (pageNum >= getNumberOfPages())
    { // Note: We count pages starting from 0.
        return -1;
    }

    fs->seekg(getPageStartingByteOffset(pageNum));
    fs->read(static_cast<char *>(data), PAGE_SIZE);
    readPageCounter++;
    return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    if (!isHandling())
    {
        return -1;
    }

    auto n = getNumberOfPages();
    if (pageNum > n)
    {
        return -1;
    }
    else if (pageNum == n)
    {
        return appendPage(data);
    }

    fs->seekp(getPageStartingByteOffset(pageNum));
    fs->write(static_cast<const char *>(data), PAGE_SIZE);
    writePageCounter++;
    return 0;
}

RC FileHandle::appendPage(const void *data)
{
    if (!isHandling())
    {
        return -1;
    }

    string dataPage{static_cast<const char *>(data), 0, PAGE_SIZE};

    size_t remainingBytes = PAGE_SIZE - dataPage.length();
    bool pageNotFilled = remainingBytes > 0;

    if (pageNotFilled)
    {
        dataPage.append(remainingBytes, '\0');
    }

    fs->seekp(0, fs->end);
    fs->write(dataPage.c_str(), PAGE_SIZE);
    appendPageCounter++;
    return 0;
}

unsigned FileHandle::getNumberOfPages()
{
    if (!isHandling())
    {
        return 0;
    }

    struct stat buf;
    auto rc = stat(fname.c_str(), &buf);
    //Should this be ceil?
    return rc == 0 ? buf.st_size / PAGE_SIZE : 0;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    return 0;
}
