/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.softwareplumbers.dms.service.sql;

import com.softwareplumbers.common.abstractpattern.Pattern;
import com.softwareplumbers.common.abstractpattern.parsers.Token;
import com.softwareplumbers.common.abstractpattern.parsers.Tokenizer;
import com.softwareplumbers.dms.RepositoryPath;
import com.softwareplumbers.dms.RepositoryPath.VersionId;
import com.softwareplumbers.dms.RepositoryPath.VersionName;

/**
 *
 * @author jonat
 */
public class RepositoryPathParser {
    
    public static class InvalidRepositoryPath extends RuntimeException {
        public InvalidRepositoryPath(String reason, String status) {
            super(reason + " at " + status);                    
        }
    }
    
    
    private static RepositoryPath.Version parseVersion(Tokenizer tokenizer) {
        if (!tokenizer.hasNext()) throw new InvalidRepositoryPath("Unexpected end of input", tokenizer.getStatus());
        Token token = tokenizer.current();
        if (token.type == Token.Type.OPERATOR) {
            if ("~".contentEquals(token.data)) {
                tokenizer.next();
                token = tokenizer.current();
                if (token.type != Token.Type.CHAR_SEQUENCE) throw new InvalidRepositoryPath("Unexpected operator", tokenizer.getStatus());
                String id = token.data.toString();
                tokenizer.next();
                return new VersionId(id);
            } else {
                throw new InvalidRepositoryPath("Unexpected operator", tokenizer.getStatus());
            }
        } else {
            String name = token.data.toString();
            tokenizer.next();
            return new VersionName(name);        
        }
    }
    
    private static RepositoryPath.NamedElement parseName(Tokenizer tokenizer) {
        if (!tokenizer.hasNext()) throw new InvalidRepositoryPath("Unexpected end of input", tokenizer.getStatus());
        Token token = tokenizer.current();
        if (token.type != Token.Type.CHAR_SEQUENCE) throw new InvalidRepositoryPath("Unexpected operator", tokenizer.getStatus());
        Pattern pattern = Pattern.of(token.data.toString());
        tokenizer.next();
        RepositoryPath.Version version = RepositoryPath.Version.NONE;
        if (tokenizer.hasNext()) {
            token = tokenizer.current();        
            if (token.type == Token.Type.OPERATOR) {
                if ("@".contentEquals(token.data)) {
                    tokenizer.next();
                    version = parseVersion(tokenizer);
                }
            }
        }
        return new RepositoryPath.NamedElement(RepositoryPath.ElementType.NAME, pattern, version);
    }
    
    private static RepositoryPath.Element parseId(Tokenizer tokenizer) {
        if (!tokenizer.hasNext()) throw new InvalidRepositoryPath("Unexpected end of input", tokenizer.getStatus());
        Token token = tokenizer.current();
        if (token.type != Token.Type.CHAR_SEQUENCE) throw new InvalidRepositoryPath("Unexpected character " + token.data, tokenizer.getStatus());
        CharSequence id = token.data;
        tokenizer.next();
        RepositoryPath.Version version = RepositoryPath.Version.NONE;
        if (tokenizer.hasNext()) {
            token = tokenizer.current();
            if (token.type == Token.Type.OPERATOR) {
                if ("@".contentEquals(token.data)) {
                    tokenizer.next();
                    version = parseVersion(tokenizer);
                }
            }
        }
        return new RepositoryPath.IdElement(RepositoryPath.ElementType.ID, id.toString(), version);
    }
    
    private static RepositoryPath.Element parsePathElement(Tokenizer tokenizer) {
        Token token = tokenizer.current();
        if (token.type == Token.Type.OPERATOR) {
            if ("~".contentEquals(token.data)) {
                tokenizer.next();
                return parseId(tokenizer);
            } 
        }
        return parseName(tokenizer);
    }
    
    public static RepositoryPath parsePath(Tokenizer tokenizer) {
        RepositoryPath result = RepositoryPath.ROOT;
        while (tokenizer.hasNext()) {
            Token token = tokenizer.current();
            if (token.type == Token.Type.OPERATOR) {
                if ("/".contentEquals(token.data)) {
                    tokenizer.next();                    
                    continue;
                } else if ("@".contentEquals(token.data)) {
                    throw new InvalidRepositoryPath("Unexpected symbol `@`", tokenizer.getStatus());
                }
                // other operators must be part of the path element
            }
            result = result.add(parsePathElement(tokenizer));
        } 
        return result;
    }    
}
