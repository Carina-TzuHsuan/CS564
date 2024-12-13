#include "catalog.h"
#include "query.h"
#include "heapfile.h"
#include "stdlib.h"

const Status QU_Delete(const string & relation,
                       const string & attrName,
                       const Operator op,
                       const Datatype type,
                       const char *attrValue)
{
    cout << "Doing QU_Delete " << endl;

    Status status;

    // Step 1: Retrieve attribute descriptor
    AttrDesc attrDesc;
    if (!attrName.empty()) {
        status = attrCat->getInfo(relation, attrName, attrDesc);
        if (status != OK) {
            cerr << "Error retrieving attribute descriptor for attribute: " << attrName << endl;
            return status;
        }
    }
    
    // Step 2: Convert filter value to binary representation if needed
    const char *convertedFilter = attrValue;
    char buffer[sizeof(float)]; // Buffer to hold binary representation

    if (!attrName.empty()) {
        if (type == INTEGER) {
            int intValue = atoi(attrValue); // Convert string to integer
            memcpy(buffer, &intValue, sizeof(int));
            convertedFilter = buffer; // Use binary representation
        } else if (type == FLOAT) {
            float floatValue = atof(attrValue); // Convert string to float
            memcpy(buffer, &floatValue, sizeof(float));
            convertedFilter = buffer; // Use binary representation
        }
    }

    // Step 3: Initialize HeapFileScan
    HeapFileScan hfs(relation, status);
    if (status != OK) {
        cerr << "Error opening HeapFileScan for relation: " << relation << endl;
        return status;
    }

    // Step 4: Start scan with or without filter
    if (!attrName.empty()) {
        status = hfs.startScan(attrDesc.attrOffset, attrDesc.attrLen, type, convertedFilter, op);
        if (status != OK) {
            cerr << "Error starting scan with filter for attribute: " << attrName << endl;
            return status;
        }
    } else {
        status = hfs.startScan(0, 0, STRING, nullptr, EQ);
        if (status != OK) {
            cerr << "Error starting unfiltered scan for relation: " << relation << endl;
            return status;
        }
    }

    // Step 5: Delete records matching the filter
    RID rid;
    int deletedCount = 0;
    while (hfs.scanNext(rid) == OK) {
        status = hfs.deleteRecord();
        if (status != OK) {
            cerr << "Error deleting record with RID: " << rid.pageNo << ", " << rid.slotNo << endl;
            return status;
        }
        deletedCount++;
    }

    // Step 6: End the scan
    hfs.endScan();

    // Log the number of deleted records
    //cout << "Number of records deleted: " << deletedCount << endl;

    return OK;
}

