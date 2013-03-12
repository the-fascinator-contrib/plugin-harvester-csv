/* 
 * The Fascinator - Plugin - Harvester - CSV
 * Copyright (C) 2009 University of Southern Queensland
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
package com.googlecode.fascinator.harvester.csv;

import java.io.File;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.fascinator.api.PluginManager;
import com.googlecode.fascinator.api.harvester.Harvester;
import com.googlecode.fascinator.api.harvester.HarvesterException;
import com.googlecode.fascinator.api.storage.Storage;

/**
 * Unit tests for the CSV harvester plugin.
 * 
 * @author Linda Octalina
 */
public class CSVHarvesterTest {

    /** Logging */
    private Logger log = LoggerFactory.getLogger(CSVHarvesterTest.class);

    /** In memory storage */
    private Storage ram;

    /**
     * Sets the "test.dir" and "test.cache.dir" system property for use in the
     * JSON configuration.
     * 
     * @throws Exception if any error occurred
     */
    @Before
    public void setup() throws Exception {
        File baseDir = new File(CSVHarvesterTest.class.getResource("/").toURI());
        System.setProperty("test.dir", baseDir.getAbsolutePath());
        ram = PluginManager.getStorage("ram");
        ram.init("{}");
    }

    /**
     * Test getting a list of object identifiers.
     * 
     * @throws Exception if any error occurred
     */
    @Test
    public void simple() throws Exception {
        CSVHarvester csvHarvester = getHarvester("/simple.json");
        Set<String> idList = csvHarvester.getObjectIdList();
        Assert.assertEquals(4, idList.size());
    }
    

    /**
     * Test getting a list of object identifiers.
     *
     * @throws Exception if any error occurred
     */
    @Test
    public void simpleBatch() throws Exception {
        CSVHarvester csvHarvester = getHarvester("/simple-batch.json");
        int count = 0;
        while (csvHarvester.hasMoreObjects()) {
            Set<String> idList = csvHarvester.getObjectIdList();
            count += idList.size();
        }
        Assert.assertEquals(4, count);
    }

    /**
     * Test getting a list of object identifiers with no header row.
     * 
     * @throws Exception if any error occurred
     */
    @Test
    public void simpleNoHeader() throws Exception {
        CSVHarvester csvHarvester = getHarvester("/geonames.json");
        Set<String> idList = csvHarvester.getObjectIdList();
        Assert.assertEquals(5, idList.size());
    }

    /**
     * Test getting a list of object identifiers with a slightly more complex
     * csv file.
     *
     * @throws Exception if any error occurred
     */
    @Test
    public void complex() throws Exception {
        CSVHarvester csvHarvester = getHarvester("/complex.json");
        Set<String> idList = csvHarvester.getObjectIdList();
        for (String oid : idList) {
            log.debug("{}", oid);
        }
        Assert.assertEquals(4, idList.size());
    }
    
    /**
     * Test a field with multiple values
     *
     * @throws Exception if any error occurred
     */
    @Test
    public void multi() throws Exception {
        CSVHarvester csvHarvester = getHarvester("/multi.json");
        Set<String> idList = csvHarvester.getObjectIdList();
        log.debug("Testing a multiple value field");
        for (String oid : idList) {
            log.debug("{}", oid);
        }
        Assert.assertEquals(5, idList.size());
    }
    
    /**
     * Test a filter on a single value
     * @throws Exception if any error occurred
     */
    @Test
    public void simpleFilter() throws Exception {
        CSVHarvester csvHarvester = getHarvester("/simple-filter.json");
        Set<String> idList = csvHarvester.getObjectIdList();
        Assert.assertEquals(2, idList.size());
    }
    
    /**
     * Test a field with multiple values being filtered
     * @throws Exception if any error occurred
     */
    @Test
    public void multiFilter() throws Exception {
        CSVHarvester csvHarvester = getHarvester("/multi-filter.json");
        Set<String> idList = csvHarvester.getObjectIdList();
        log.debug("Testing a multiple value field with a filter");
        for (String oid : idList) {
            log.debug("{}", oid);
        }
        Assert.assertEquals(2, idList.size());
    }
    
    /**
     * Test a field with multiple values being filtered
     * @throws Exception if any error occurred
     */
    @Test
    public void multiFilterAll() throws Exception {
        CSVHarvester csvHarvester = getHarvester("/multi-filter-all.json");
        Set<String> idList = csvHarvester.getObjectIdList();
        log.debug("Testing a multiple value field with a filter");
        for (String oid : idList) {
            log.debug("{}", oid);
        }
        Assert.assertEquals(1, idList.size());
    }
    
    /**
     * Tests that an exception is thrown when the same delimiter is set for
     * the delimiter and multiValueFieldDelimiter params
     * @throws Exception
     */
    @Test(expected=HarvesterException.class)
    public void checkdelimiter() throws Exception {
    	log.debug("Testing that the same delimiter can't be used for the CSV and multi-value fields");
    	CSVHarvester csvHarvester = getHarvester("/checkdelimiter.json");
    	Set<String> idList = csvHarvester.getObjectIdList();
        Assert.assertEquals(4, idList.size());
    }

    /**
     * Test a filter on blank values
     * @throws Exception if any error occurred
     */
    @Test
    public void blankValueFilter() throws Exception {
    	log.debug("Testing a blank value (empty string) filter");
        CSVHarvester csvHarvester = getHarvester("/blank-entry-filter.json");
        Set<String> idList = csvHarvester.getObjectIdList();
        Assert.assertEquals(1, idList.size());
    }
    
    /**
     * Gets a CSV harvester instance and initialises it with the specified
     * configuration file.
     *
     * @param configFile JSON configuration file
     * @return CSV harvester
     * @throws Exception if any error occurred
     */
    private CSVHarvester getHarvester(String configFile) throws Exception {
        Harvester csvHarvester = PluginManager.getHarvester("csv", ram);
        csvHarvester.init(new File(getClass().getResource(configFile).toURI()));
        return (CSVHarvester) csvHarvester;
    }
}
