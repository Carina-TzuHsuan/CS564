#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}

// Put pages from disk into buffer
const Status BufMgr::allocBuf(int & frame)
{
    // Check for any free frames in the buffer pool
    int attempts = 0; // Track the number if frames checked to avoid infinite loop
    bool clearedRefBits = false;
    
    while(attempts < numBufs){
        advanceClock();
        BufDesc *bufDesc = &bufTable[clockHand]; // Reference to the current frame's descriptor
        // Search for an empty frame
        if(!bufDesc->valid){
            frame = clockHand;
            return Status::OK;
        }
        
        // If the frame is pinned, skip it
        if(bufDesc->pinCnt >0){
            attempts++;
            continue;
        }
        
        // Second chance for recently accessed frames
        if(bufDesc->refbit){

            bufDesc->refbit = false;
            clearedRefBits = true;
            attempts++;
            continue;
        }
        
        // Write back dirty pages before reuse;
        if(bufDesc->dirty){
            Status status = bufDesc->file->writePage(bufDesc->pageNo, &bufPool[clockHand]);
            if(status != Status::OK){
                return UNIXERR;
            }
            bufDesc->dirty = false;
        }
        
        // Remove from hash table if the frame is valid
        if(bufDesc->valid){
            hashTable->remove(bufDesc->file, bufDesc->pageNo);
        }
        
        //Reset the BufDesc using Clear()
        bufDesc->Clear();
        
        //Return the frame number to the caller
        frame = clockHand;
        return Status::OK;
    }
    if (clearedRefBits){
        attempts =0;
        return allocBuf(frame);
    }
    return BUFFEREXCEEDED;
}
    

	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    int frameNo;
    Status status = OK;
    // Check if the page is already in the buffer pool using the hash table
    status = hashTable->lookup(file, PageNo, frameNo);
    if(status == OK){
        // Page is already in the buffer
        bufTable[frameNo].pinCnt++;
        bufTable[frameNo].refbit = true;
        page = &bufPool[frameNo];
        return Status::OK;
    }
    
    // Allocate a free frame to load the page if it's not already in the buffer
    status = allocBuf(frameNo);
    if(status != Status::OK){
        return status;
    }
    
    // Read the page from disk into the allocated frame in the buffer pool
    status = file->readPage(PageNo, &bufPool[frameNo]);
    if(status != Status::OK){
        return status;
    }
    
    // Set up the frame metadata using Set() and add the page to the hashtable
    bufTable[frameNo].Set(file, PageNo);
    status = hashTable->insert(file, PageNo, frameNo);
    if(status != OK){
        return HASHTBLERROR;
    }
    
    // Return the frame pointer as output
    page = &bufPool[frameNo];
    return Status::OK;

}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    int frameNo;
    Status status = OK;
    
    // Check if the page is in the buffer pool
    status = hashTable->lookup(file, PageNo, frameNo);
    if(status != OK){
        return HASHTBLERROR;
    }
    
    // Check if the page is already pinned
    BufDesc* bufDesc = &bufTable[frameNo];
    if(bufDesc->pinCnt <=0){
        return PAGENOTPINNED;
    }
    
    // Decrement the pin count
    bufDesc->pinCnt--;
    
    // Mark the page as dirty if specified
    if(dirty){
        bufDesc->dirty = true;
    }
    
    return Status::OK;

}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    // Allocate a new page number in the file (in disk)
    Status status = file->allocatePage(pageNo);
    if (status != OK){
        return status;
    }
    
    
    // Called to obtain a buffer pool frame.
    // Writing a newly allocated page to the buffer without explicit instruction is based on performance optimization and convenience in data management within the buffer manager.
    int frameNo;
    status = allocBuf(frameNo);
    if (status == BUFFEREXCEEDED){
        return BUFFEREXCEEDED;
    }
    
    // Insert the new page into the hash table for fast lookup
    if (hashTable->insert(file, pageNo, frameNo) != Status::OK){
        return HASHTBLERROR;
    }
    
    // Set up the buffer frame with the new page metadata
    bufTable[frameNo].Set(file, pageNo);
    
    // Return the frames's page pointer and page number
    page = &bufPool[frameNo];
    return Status::OK;
    

}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


