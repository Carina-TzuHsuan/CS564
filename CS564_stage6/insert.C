#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
    cout << "Doing QU_Insert " << endl;
    
    // Step 1: Validate relation existence
    RelDesc relDesc;
    Status status = relCat->getInfo(relation, relDesc);
    if (status !=OK){
        cerr << "Relation" << relation << "does not exist." << endl;
        return status;
    }
    
    // Step 2: Validate attribites against schema
    AttrDesc *attrs;
    int numAttrs;
    status = attrCat->getRelInfo(relation, numAttrs, attrs);
    if(status != OK){
        cerr << "Error fetching attribute info for relation" << relation <<endl;
        return status;
    }
    if(attrCnt != numAttrs){
        cerr << "Attribute count mismatch for relation" << relation << endl;
        return ATTRNOTFOUND;
    }
    
    // Step 3: Prepare record data
    int recordLen = 0;
    for (int i =0; i< numAttrs; i++){
        recordLen += attrs[i].attrLen;
    }
    char* recordData = new char[recordLen];
    memset(recordData, 0, recordLen);
    
    for(int i =0; i< attrCnt; i++){
        bool found = false;
        for(int j=0; j < numAttrs; j++){
            if (strcmp(attrList[i].attrName, attrs[j].attrName) == 0) {
                // Handle type conversion
                if (attrs[j].attrType == INTEGER) {
                    int intValue = atoi(static_cast<const char*>(attrList[i].attrValue));
                    memcpy(recordData + attrs[j].attrOffset, &intValue, sizeof(int));
                } else if (attrs[j].attrType == FLOAT) {
                    float floatValue = atof(static_cast<const char*>(attrList[i].attrValue));
                    memcpy(recordData + attrs[j].attrOffset, &floatValue, sizeof(float));
                } else if (attrs[j].attrType == STRING) {
                    strncpy(recordData + attrs[j].attrOffset, static_cast<const char*>(attrList[i].attrValue), attrs[j].attrLen);
                } else {
                    cerr << "Unsupported attribute type for attribute: " << attrList[i].attrName << endl;
                    delete[] recordData;
                    delete[] attrs;
                    return ATTRNOTFOUND;
                }

                found = true;
                break;
            }
        }
        if(!found){
            delete[] recordData;
            delete[] attrs;
            return ATTRNOTFOUND;
        }
    }
    
    // Step 4: Insert record
    InsertFileScan insertFile(relation, status);
    if(status != OK){
        delete[] recordData;
        delete[] attrs;
        return status;
    }
    
    RID rid;
    Record rec = {recordData, recordLen};
    status = insertFile.insertRecord(rec, rid);
    
    // Clean up
    delete[] recordData;
    delete[] attrs;
    
    return status;
    
}

