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

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * 
 * Parser to read the Google Ad JavaScript and infer configuration attributes
 * for the same.
 * 
 * @author Rohit Ghatol
 *
 */
public class DefaultParser implements GoogleAdParser {

	private static final Map<String, String> map = new HashMap<String, String>();

	/**
	 * Create a Parser from given googleAdScript
	 */
	public DefaultParser(String googleAdScript) {

		// FIXME - Instead of regular expression, lets us JavaScript Interpreter
		// and pass some predefined values for window.width, window.height etc

		String substr = googleAdScript
				.substring(5, googleAdScript.length() - 5).replaceAll(
						"(?://.*)|(/\\*(?:.|[\\n\\r])*?\\*/)", "");

		StringTokenizer tokenizer = new StringTokenizer(substr, ";");
		while (tokenizer.hasMoreTokens()) {
			String subToken = tokenizer.nextToken().replaceAll("(\\r|\\n)", "")
					.replaceAll("\"", "");
			

			StringTokenizer subTokenizer = new StringTokenizer(subToken, "=");

			String key = null;
			String value = null;
			if (subTokenizer.hasMoreTokens()) {
				key = subTokenizer.nextToken().trim();
			}
			if (subTokenizer.hasMoreTokens()) {
				value = subTokenizer.nextToken().trim();
			}

			map.put(key, value);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.synerzip.analytics.commoncrawl.googleads.parser.GoogleAdParser#
	 * getAttribute(java.lang.String)
	 */
	@Override
	public String getAttribute(String attribute) {

		return map.get(attribute);
	}

}
