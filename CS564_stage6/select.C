#include "catalog.h"
#include "query.h"
#include "stdio.h"
#include "stdlib.h"
#include "heapfile.h"  // To use HeapFileScan
#include "utility.h"   // For helper functions

// forward declaration
const Status ScanSelect(const string & result,
            const int projCnt,
            const AttrDesc projNames[],
            const AttrDesc *attrDesc,
            const Operator op,
            const char *filter,
            const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 *     OK on success
 *     an error code otherwise
 */

const Status QU_Select(const string & result,
               const int projCnt,
               const attrInfo projNames[],
               const attrInfo *attr,
               const Operator op,
               const char *attrValue)
{
    cout << "Doing QU_Select " << endl;

    Status status;

    // Retrieve projection attributes from catalog
    AttrDesc projAtts[projCnt];
    for (int i = 0; i < projCnt; i++) {
        status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, projAtts[i]);
        if (status != OK) {
            cerr << "Error retrieving projection attribute: " << projNames[i].attrName << endl;
            return status;
        }
    }

    // Retrieve filter attribute descriptor, if provided
    AttrDesc filterAttr;
    if (attr != nullptr) {
        status = attrCat->getInfo(attr->relName, attr->attrName, filterAttr);
        if (status != OK) {
            cerr << "Error retrieving filter attribute: " << attr->attrName << endl;
            return status;
        }
    }

    // Compute the record length for the result relation
    int reclen = 0;
    for (int i = 0; i < projCnt; i++) {
        reclen += projAtts[i].attrLen;
    }

    // Call ScanSelect to execute the actual query
    return ScanSelect(result, projCnt, projAtts, attr != nullptr ? &filterAttr : nullptr, op, attrValue, reclen);
}

const Status ScanSelect(const string & result,
            const int projCnt,
            const AttrDesc projNames[],
            const AttrDesc *attrDesc,
            const Operator op,
            const char *filter,
            const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

    Status status;

    // Initialize the heap file scan
    HeapFileScan hfs(projNames[0].relName, status);
    if (status != OK) {
        cerr << "Error initializing HeapFileScan for relation: " << projNames[0].relName << endl;
        return status;
    }

    // Convert `filter` for numeric attributes if needed
    const char *convertedFilter = filter;
    char buffer[sizeof(float)]; // Buffer to hold binary representation

    if (attrDesc != nullptr) {
        if (attrDesc->attrType == INTEGER) {
            int intValue = atoi(filter); // Convert string to integer
            memcpy(buffer, &intValue, sizeof(int));
            convertedFilter = buffer; // Use binary representation
        } else if (attrDesc->attrType == FLOAT) {
            float floatValue = atof(filter); // Convert string to float
            memcpy(buffer, &floatValue, sizeof(float));
            convertedFilter = buffer; // Use binary representation
        }
    }

    // Apply the filter if attrDesc is not null
    if (attrDesc != nullptr) {
        status = hfs.startScan(attrDesc->attrOffset, attrDesc->attrLen, static_cast<Datatype>(attrDesc->attrType), convertedFilter, op);
        if (status != OK) {
            cerr << "Error starting scan with filter for attribute: " << attrDesc->attrName << endl;
            return status;
        }
    } else {
        status = hfs.startScan(0, 0, STRING, nullptr, EQ);
        if (status != OK) {
            cerr << "Error starting unfiltered scan" << endl;
            return status;
        }
    }

    // Perform the scan and projection
    Record record;
    RID rid;
    while (hfs.scanNext(rid) == OK) {
        // Retrieve the actual record for the current RID
        status = hfs.getRecord(record);
        if (status != OK) {
            cerr << "Error retrieving record for RID" << endl;
            return status;
        }

        // Allocate memory for the projected record
        char *newRecord = new char[reclen];
        int offset = 0;

        // Perform projection
        for (int i = 0; i < projCnt; i++) {
            memcpy(newRecord + offset,
                   static_cast<char*>(record.data) + projNames[i].attrOffset,
                   projNames[i].attrLen);
            offset += projNames[i].attrLen; // Increment offset properly
        }

        // Insert projected record into the result relation
        RID newRid;
        Record newRec;
        newRec.data = newRecord;
        newRec.length = reclen;

        InsertFileScan resultFile(result, status);
        if (status != OK) {
            delete[] newRecord;
            cerr << "Error opening result file: " << result << endl;
            return status;
        }

        status = resultFile.insertRecord(newRec, newRid);
        if (status != OK) {
            delete[] newRecord;
            cerr << "Error inserting record into result file" << endl;
            return status;
        }

        delete[] newRecord; // Free allocated memory after use
    }

    hfs.endScan();
    return OK;
}

