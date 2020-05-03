/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.service.sql;

import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author jonathan
 */
public class Operations {
    
    public String fetchLatestDocument;
    public String fetchDocument;
    public String fetchChildren;
    public String createDocument;
    public String createVersion;
    public String createNode;
    public String createFolder;
    public String createLink;
    public String deleteObject;
    public String deleteDocumentById;
    public String fetchPathToId;
    public String fetchLastNameLike;
    public String fetchChildByName;
    public String updateLink;
    public String updateFolder;
    public String updateDocument;
    public String copyFolder;
    public String copyLink;
    public String lockVersions;
    public String unlockVersions;
    public String copyNode;
    public String publishNode;
    public String publishLink;

    public void setCreateDocument(String sql) {
        createDocument = sql;
    }

    public void setCreateVersion(String sql) {
        createVersion = sql;
    }

    public void setCreateFolder(String sql) {
        createFolder = sql;
    }

    public void setCreateLink(String sql) {
        createLink = sql;
    }

    public void setCreateNode(String sql) {
        createNode = sql;
    }

    public void setUpdateLink(String sql) {
        updateLink = sql;
    }

    public void setUpdateDocument(String sql) {
        updateDocument = sql;
    }

    public void setUpdateFolder(String sql) {
        updateFolder = sql;
    }

    public void setFetchPathToId(String sql) {
        fetchPathToId = sql;
    }

    public void setFetchLastNameLike(String sql) {
        fetchLastNameLike = sql;
    }

    public void setFetchChildren(String sql) {
        fetchChildren = sql;
    }
    
    public void setCopyFolder(String sql) {
        copyFolder = sql;
    }

    public void setCopyLink(String sql) {
        copyLink = sql;
    }
    
    public void setDeleteObject(String sql) {
        deleteObject = sql;
    }

    public void setDeleteDocumentById(String sql) {
        deleteDocumentById = sql;
    }    

    public void setUnlockVersions(String sql) {
        unlockVersions = sql;
    }
    
    public void setLockVersions(String sql) {
        lockVersions = sql;
    }
    
    public void setCopyNode(String sql) {
        copyNode = sql;
    }
    
    public void setPublishNode(String sql) {
        publishNode = sql;
    }
    
    public void setPublishLink(String sql) {
        publishLink = sql;
    }
        
    @Autowired
    public Operations(Templates templates) {
        fetchLatestDocument = templates.substitute(templates.fetchDocument, "FROM VIEW_DOCUMENTS WHERE DOCUMENT_ID=? AND LATEST=TRUE");
        fetchDocument = templates.substitute(templates.fetchDocument, "FROM VIEW_DOCUMENTS WHERE DOCUMENT_ID=? AND VERSION_ID=?");
    }
}
