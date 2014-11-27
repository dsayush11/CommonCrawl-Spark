/**
 * Copyright (C) 2004-2014 Synerzip. 
 * 
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.synerzip.analytics.commoncrawl.googleads.parser;

/**
 * Google Ad Parser. Reads the Google Ad JavaScript Parses the values for GoogleAdsinfo
 * @author Rohit Ghatol
 *
 */
public interface GoogleAdParser {

	/**
	 * 
	 * @param attribute The Google Ad Attribute
	 * @return The value for given attribute
	 */
	String getAttribute(String attribute);
}
