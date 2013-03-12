/*
 * The Fascinator - Plugin - Harvester - CSV
 * Copyright (C) 2010-2011 University of Southern Queensland
 * Copyright (C) 2011 Queensland Cyber Infrastructure Foundation (http://www.qcif.edu.au/)
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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.json.simple.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;

import com.googlecode.fascinator.api.harvester.HarvesterException;
import com.googlecode.fascinator.api.storage.DigitalObject;
import com.googlecode.fascinator.api.storage.Payload;
import com.googlecode.fascinator.api.storage.StorageException;
import com.googlecode.fascinator.common.JsonObject;
import com.googlecode.fascinator.common.JsonSimple;
import com.googlecode.fascinator.common.harvester.impl.GenericHarvester;
import com.googlecode.fascinator.common.storage.StorageUtils;

/**
 * Harvester for CSV files.
 * <p>
 * Configuration options:
 * <ul>
 * <li>fileLocation: The location of the csv file (required)</li>
 * <li>idColumn: the column holding the primary key. 
 * 	If not provided, the row number will be used.</li>
 * <li>recordIDPrefix: Adds a prefix to the value found in the ID column.
 *	For example, setting this as "http://id.example.com/" with an ID value of "453"
 *	will result in http://id.example.com/453 as the ID. 
 * <li>delimiter: The csv delimiter. Comma (,) is the default (optional)</li>
 * <li>ignoredFields: An array of fields (columns) ignored by the harvest.</li>
 * <li>includedFields: An array of fields (columns) included by the harvest.</li>
 * <li>multiValueFields: An array of fields (columns) that contain several values (optional)</li>
 * <li>multiValueFieldDelimiter: The delimiter for multi-value fields. Semi-colon (;) is the default (optional)</li>
 * <li>payloadId: The payload identifier used to store the JSON data (defaults to "metadata.json")</li>
 * <li>batchSize: The number of rows in the CSV file to process, before being harvested (defaults to 50)</li>
 * <li>maxRows: The number of rows to process where -1 means harvest all (defaults to -1), note that if filters are in place the maxRows applies to all rows not just the ones that pass the filter</li>
 * <li>filters: If a filter is provided it must match for a row to be imported. The filters are defined as an array of a map of the following
 * <ul>
 * 	<li>field: Name of the field (column), note that this field can be one that's being ignored</li>
 *  <li>multi: Only applies to multi-value fields, can have values of ANY or ALL, ANY is the default (optional)</li>
 *  <li>regex: The case sensitive regex that must match part of the value in the given column. To do an entire match surround with ^ and $
 * </ul></li>
 * </ul>
 * <p>
 * You can select to harvest all columns (fields) or be selective:
 * <ul>
 * <li>If you don't set ignoredFields or includedFields: all fields are harvested.</li>
 * <li>ignoredFields has precedence over includedFields: 
 * 	If you have the same field in both lists, it will get ignored.</li>
 * <li>If you only provide fields in ignoredFields (and leave includedFields blank), all other fields will be harvested</li>
 * <li>If you only provide fields in includedFields (and leave ignoredFields blank), only these fields will be harvested</li>
 * </ul>
 * <p>
 * Fields with repeated names result in only the first value being stored.
 * <p>
 * Based on Greg Pendlebury's CallistaHarvester.
 * 
 * @author Duncan Dickinson
 * 
 * <p>2011 - 2012 - QCIF undertakes maintenance work.</p>
 * <p>2012 - innodev.com.au addition for filtering</p>
 * 
 * @author Greg Pendlebury
 * @author Duncan Dickinson
 */
public class CSVHarvester extends GenericHarvester {

	/**
	 * How a match should behave for a multi-value field
	 */
	private enum MultiMatchType {
		/**
		 * In a multi-value field, only 1 value has to match the provided pattern.
		 */
		ANY,
		/**
		 * In a multi-value field, all values have to match the provided pattern. An empty set of values is considered to fail the ALL condition.
		 */
		ALL;
	}
	
	/**
	 * Pass in a single or multiple values to see if it passes the filter.
	 * @author James Andrews
	 */
	private class Filter {
		
		private String field;
		private MultiMatchType type;
		private Pattern regex;
	
		/**
		 * Creates a filter from a JSON object
		 * @param json The json to extract "field","operation" (optional) and "regex" from
		 * @throws HarvesterException if
		 * <ul><li>field is missing</li>
		 * <li>operation is not a valid operation</li>
		 * <li>regex is missing or not a valid regex string</li>
		 * </ul>
		 */
		public Filter(JsonSimple json) throws HarvesterException {
			field = json.getString(null, "field");
			if (field == null) {
				throw new HarvesterException("In a filter definition, missing the mandatory attribute 'field'");
			}
			String matchTypeStr = json.getString("ANY", "multi");
			try {
				type = MultiMatchType.valueOf(matchTypeStr);
			} catch (Exception e) {
				throw new HarvesterException("In a filter definition, invalid filter match type '" + matchTypeStr + "', valid values are " + MultiMatchType.values());
			}
			String regexStr = json.getString(null, "regex");
			if (regexStr == null) {
				throw new HarvesterException("In a filter definition, missing the mandatory attribute 'regex'");
			}
			try {
				regex = Pattern.compile(regexStr);
//				regex = Pattern.compile(regexStr,Pattern.MULTILINE);
			} catch (PatternSyntaxException e) {
				throw new HarvesterException("In a filter definition, provided regex was invalid " + e.getMessage(),e);
			}
		}
		
		public String getField() {
			return field;
		}
		
		/**
		 * Returns if the one string value matches this filter.
		 * @param str String we're testing against
		 * @return If the parameter matches our filter
		 */
		public boolean matches(String str) {
			return regex.matcher(str).find();
		}
		
		/**
		 * Returns if the multi-values pass out filter based on the filter and the MultiMatchType.
		 * @param strs An array of values
		 * @return if the array of values passes the filter
		 */
		public boolean matches(String[] strs) {
			if (strs == null || strs.length == 0) {
				return false;
			}
			for(String str : strs) {
				if (matches(str)) {
					if (type == MultiMatchType.ANY) {
						return true;
					}
				} else if (type == MultiMatchType.ALL) {
					return false;
				}
			}
			return type == MultiMatchType.ALL;
		}
		
		@Override
		public String toString() {
			return "Filter (" + type + "," + regex.pattern() + ")"; 
		}
	}
	
    /** Default column delimiter */
    private static final char DEFAULT_DELIMITER = ',';

    /** Default payload ID */
    private static final String DEFAULT_PAYLOAD_ID = "metadata.json";

    /** Default batch size */
    private static final int DEFAULT_BATCH_SIZE = 50;
    
	private static final char DEFAULT_MULTI_VALUE_FIELD_DELIMITER = ';';

    /** Logging */
    private Logger log = LoggerFactory.getLogger(CSVHarvester.class);

    /** Field names (columns) */
    private List<String> dataFields;

    /** Ignored field names (column) */
    private List<String> ignoredFields;

    /** Included field names (column) */
    private List<String> includedFields;
    
    /** Fields containing more than 1 value. */
    private List<String> multiValueFields;

    /** The name of the column holding the ID */
    private String idColumn;

    /** A prefix for generating the object's ID */
    private String idPrefix;

    /** The delimiter used in the CSV */
    private char delimiter;
    
    /** The delimiter used in fields with multiple values */
    private char multiValueFieldDelimiter;
    
    /** Debugging limit */
    private long maxRows;

    /** Payload ID */
    private String payloadId;

    /** Batch size */
    private int batchSize;

    /** Current row */
    private long currentRow;

    /** Whether or not there are more files to harvest */
    private boolean hasMore;

    /** CSV Reader */
    private CSVReader csvReader;

    /** File name */
    private String filename;

    /** A list of filters by field name */
    private Map<String,List<Filter>> filters;
    
    /**
     * Constructs the CSV harvester plugin.
     */
    public CSVHarvester() {
        super("csv", "CSV Harvester");
    }

    /**
     * Initialise the CSV harvester plugin.
     *
     * @throws HarvesterException if an error occurred
     */
    @Override
    public void init() throws HarvesterException {
        JsonSimple options = new JsonSimple(getJsonConfig().getObject("harvester", "csv"));

        String filePath = options.getString(null, "fileLocation");
        if (filePath == null) {
            throw new HarvesterException("No data file provided!");
        }
        File csvDataFile = new File(filePath);
        if (csvDataFile == null || !csvDataFile.exists()) {
            throw new HarvesterException("Could not find CSV file '" + filePath + "'");
        }
        filename = csvDataFile.getName();

        idPrefix = options.getString("", "recordIDPrefix");
        maxRows = options.getInteger(-1, "maxRows");
        delimiter = options.getString(String.valueOf(DEFAULT_DELIMITER), "delimiter").charAt(0);
        ignoredFields = getStringList(options, "ignoreFields");
        includedFields = getStringList(options, "includedFields");
        multiValueFields = getStringList(options, "multiValueFields");
        multiValueFieldDelimiter = options.getString(String.valueOf(DEFAULT_MULTI_VALUE_FIELD_DELIMITER), "multiValueFieldDelimiter").charAt(0);
        payloadId = options.getString(DEFAULT_PAYLOAD_ID, "payloadId");
        batchSize = options.getInteger(DEFAULT_BATCH_SIZE, "batchSize");
        hasMore = true;
                
        if (delimiter == multiValueFieldDelimiter) {
            throw new HarvesterException("Cannot parse CSV: The requested delimiters for the CSV and multivalue fields are the same: " + delimiter);
        }
        
        try {
            // open the CSV file for reading
            Reader fileReader = new InputStreamReader(new FileInputStream(csvDataFile), "UTF-8");
            csvReader = new CSVReader(fileReader, delimiter);

            // configure the data fields
            if (options.getBoolean(true, "headerRow")) {
                dataFields = Arrays.asList(csvReader.readNext());
            } else {
                dataFields = getStringList(options, "headerList");
            }

            // check that the specified id column is valid
            idColumn = options.getString(null, "idColumn");
            if (idColumn != null && !dataFields.contains(idColumn)) {
                throw new HarvesterException("ID column '" + idColumn + "' was invalid or not found in the data!");
            }
            
            // load filters, all filters must pass for the row to be considered
            filters = new HashMap<String,List<Filter>>();
            List<JsonSimple> filterConfig = options.getJsonSimpleList("filters");
            if (filterConfig != null) {
            	for(JsonSimple singleFilterConfig : filterConfig) {
            		Filter filter = new Filter(singleFilterConfig);
            		String field = filter.getField();
            		if (!dataFields.contains(field)) {
            			throw new HarvesterException("Filter column '" + field + "' was not found in the data");
            		}
            		List<Filter> existingFilters = filters.get(field);
            		if (existingFilters == null) {
            			existingFilters = new ArrayList<Filter>();
            			filters.put(field, existingFilters);
            		}
            		existingFilters.add(filter);
            	}
            }
            
            
        } catch (IOException ioe) {
            throw new HarvesterException(ioe);
        }
    }

    /**
     * Gets a string list from a JsonSimple object. Convenience method to return
     * an empty list instead of null if the node was not found.
     *
     * @param json a JsonSimple object
     * @param path path to the node
     * @return string list found at node, or empty if not found
     */
    private List<String> getStringList(JsonSimple json, Object... path) {
        List<String> list = json.getStringList(path);
        if (list == null) {
            list = Collections.emptyList();
        }
        return list;
    }

    /**
     * Shutdown the plugin.
     * 
     * @throws HarvesterException if an error occurred
     */
    @Override
    public void shutdown() throws HarvesterException {
        if (csvReader != null) {
            try {
                csvReader.close();
            } catch (IOException ioe) {
                log.warn("Failed to close CSVReader!", ioe);
            }
            csvReader = null;
        }
    }

    /**
     * Check if there are more objects to harvest.
     *
     * @return <code>true</code> if there are more, <code>false</code> otherwise
     */
    @Override
    public boolean hasMoreObjects() {
        return hasMore;
    }

    /**
     * Harvest the next batch of rows and return their object IDs.
     *
     * @return the set of object IDs just harvested
     * @throws HarvesterException if an error occurred
     */
    @Override
    public Set<String> getObjectIdList() throws HarvesterException {
        Set<String> objectIdList = new HashSet<String>();
        try {
            String[] row = null;
            int rowCount = 0;
            boolean done = false;
            while (!done && (row = csvReader.readNext()) != null) {
                rowCount++;
                currentRow++;
                
                String recordId = createRecord(row);
                if (recordId != null) {
	                objectIdList.add(recordId);
                }
                if (rowCount % batchSize == 0) {
                    log.debug("Batch size reached at row {}", currentRow);
                    break;
                }
                done = (maxRows > 0) && (currentRow < maxRows);
            }
            hasMore = (row != null);
        } catch (IOException ioe) {
            throw new HarvesterException(ioe);
        }
        if (objectIdList.size() > 0) {
            log.debug("Created {} objects", objectIdList.size());
        }
        return objectIdList;
    }

    /**
     * Create an Object in storage from this record.
     *
     * @param columns an Array of Strings containing column data
     * @return String the OID of the stored Object or null if the record was filtered out
     * @throws HarvesterException if an error occurs
     */
	private String createRecord(String[] columns) throws HarvesterException {
        // by default use the row number as the ID
        String recordId = Long.toString(currentRow);

        // create data
        JsonObject data = new JsonObject();
        for (int index = 0; index < columns.length; index++) {
            String field = dataFields.get(index);
            String value = columns[index];
            // respect fields to be included and ignored
            boolean include = includedFields.contains(field) && !ignoredFields.contains(field);
            boolean hasFilters = filters.containsKey(field);
            List<Filter> fieldFilters = filters.get(field);
            
            // we don't want to include or filter based on this column
            if (!include && !hasFilters) {
            	continue;
            } else {
            	if (multiValueFields.contains(field)){
                	log.debug("Processing a multi-value field: " + field + " with value: " + value);
            		try {
            			CSVReader multi = new CSVReader(new StringReader(value), multiValueFieldDelimiter);
            			String[] values = multi.readNext();
            			multi.close();
            			
            			if (hasFilters) {
            				for(Filter f : fieldFilters) {
            					if (!f.matches(values)) {
            						log.debug("multi-value field '" + field + "' with value '" + value + "' failed filter " + f);
            						return null;
            					}
            				}
            			}
            			if (include) {
	            			JSONArray list = new JSONArray();
	            			if (values != null) {
		            			for (String item : values) {
		            				log.debug(" Individual value:" + item);
		            				list.add(item);
		            			}
	            			}
	            			
	            			data.put(field, list);
            			}
            		} catch (IOException ioe) {
                        throw new HarvesterException(ioe);
                    }
                } else {
                	if (hasFilters) {
                		for(Filter f : fieldFilters) {
        					if (!f.matches(value)) {
        						log.debug("field '" + field + "' with value '" + value + "' failed filter " + f);
        						return null;
        					}
        				}
                	}
                	if (include) {
                		data.put(field, value);
                	}
                }
            }
            if (field.equals(idColumn)) {
                recordId = value;
            }
        }
        log.debug(data.toJSONString());
        // create metadata
        JsonObject meta = new JsonObject();
        meta.put("dc.identifier", idPrefix + recordId);

        // What should the OID be?
        String oid = DigestUtils.md5Hex(filename + idPrefix + recordId);
        // This will throw any exceptions if errors occur
        storeJsonInObject(data, meta, oid);
        return oid;
    }

    /**
     * Store the processed data and metadata in the system
     *
     * @param dataJson an instantiated JSON object containing data to store
     * @param metaJson an instantiated JSON object containing metadata to store
     * @throws HarvesterException if an error occurs
     */
    private void storeJsonInObject(JsonObject dataJson, JsonObject metaJson,
            String oid) throws HarvesterException {
        // Does the object already exist?
        DigitalObject object = null;
        try {
            object = getStorage().getObject(oid);
            storeJsonInPayload(dataJson, metaJson, object);

        } catch (StorageException ex) {
            // This is going to be brand new
            try {
                object = StorageUtils.getDigitalObject(getStorage(), oid);
                storeJsonInPayload(dataJson, metaJson, object);
            } catch (StorageException ex2) {
                throw new HarvesterException(
                        "Error creating new digital object: ", ex2);
            }
        }

        // Set the pending flag
        if (object != null) {
            try {
                object.getMetadata().setProperty("render-pending", "true");
                object.close();
            } catch (Exception ex) {
                log.error("Error setting 'render-pending' flag: ", ex);
            }
        }
    }

    /**
     * Store the processed data and metadata in a payload
     *
     * @param dataJson an instantiated JSON object containing data to store
     * @param metaJson an instantiated JSON object containing metadata to store
     * @param object the object to put our payload in
     * @throws HarvesterException if an error occurs
     */
    private void storeJsonInPayload(JsonObject dataJson, JsonObject metaJson,
            DigitalObject object) throws HarvesterException {

        Payload payload = null;
        JsonSimple json = new JsonSimple();
        try {
            // New payloads
            payload = object.getPayload(payloadId);
            //log.debug("Updating existing payload: '{}' => '{}'",
            //        object.getId(), payloadId);

            // Get the old JSON to merge
            try {
                json = new JsonSimple(payload.open());
            } catch (IOException ex) {
                log.error("Error parsing existing JSON: '{}' => '{}'",
                    object.getId(), payloadId);
                throw new HarvesterException(
                        "Error parsing existing JSON: ", ex);
            } finally {
                payload.close();
            }

            // Update storage
            try {
                InputStream in = streamMergedJson(dataJson, metaJson, json);
                object.updatePayload(payloadId, in);

            } catch (IOException ex2) {
                throw new HarvesterException(
                        "Error processing JSON data: ", ex2);
            } catch (StorageException ex2) {
                throw new HarvesterException(
                        "Error updating payload: ", ex2);
            }

        } catch (StorageException ex) {
            // Create a new Payload
            try {
                //log.debug("Creating new payload: '{}' => '{}'",
                //        object.getId(), payloadId);
                InputStream in = streamMergedJson(dataJson, metaJson, json);
                payload = object.createStoredPayload(payloadId, in);

            } catch (IOException ex2) {
                throw new HarvesterException(
                        "Error parsing JSON encoding: ", ex2);
            } catch (StorageException ex2) {
                throw new HarvesterException(
                        "Error creating new payload: ", ex2);
            }
        }

        // Tidy up before we finish
        if (payload != null) {
            try {
                payload.setContentType("application/json");
                payload.close();
            } catch (Exception ex) {
                log.error("Error setting Payload MIME type and closing: ", ex);
            }
        }
    }

    /**
     * Merge the newly processed data with an (possible) existing data already
     * present, also convert the completed JSON merge into a Stream for storage.
     *
     * @param dataJson an instantiated JSON object containing data to store
     * @param metaJson an instantiated JSON object containing metadata to store
     * @param existing an instantiated JsonSimple object with any existing data
     * @throws IOException if any character encoding issues effect the Stream
     */
    private InputStream streamMergedJson(JsonObject dataJson,
            JsonObject metaJson, JsonSimple existing) throws IOException {
        // Overwrite and/or create only nodes we consider new data
        existing.getJsonObject().put("recordIDPrefix", idPrefix);
        JsonObject existingData = existing.writeObject("data");
        existingData.putAll(dataJson);
        JsonObject existingMeta = existing.writeObject("metadata");
        existingMeta.putAll(metaJson);

        // Turn into a stream to return
        String jsonString = existing.toString(true);
        return IOUtils.toInputStream(jsonString, "UTF-8");
    }
}
