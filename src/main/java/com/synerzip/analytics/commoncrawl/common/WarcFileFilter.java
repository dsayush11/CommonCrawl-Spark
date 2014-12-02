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
package com.synerzip.analytics.commoncrawl.common;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * Hadoop FileSystem PathFilter for TextData files, allowing users to limit the
 * number of files processed.
 */
//FIXME - Bad class uses Static methods
public class WarcFileFilter implements PathFilter {

	private static int count = 0;
	private static long max = -1;
	private static String filter = "";

	public static void setFilter(String filter) {
		WarcFileFilter.filter = filter;
	}

	public static void setMax(long newmax) {
		max = newmax;
	}

	public boolean accept(Path path) {

		if (!path.getName().contains(filter)) {
			return false;
		}

		if (max < 0) {
			return true;
		}

		if (max < ++count) {
			return false;
		}

		return true;
	}
}
