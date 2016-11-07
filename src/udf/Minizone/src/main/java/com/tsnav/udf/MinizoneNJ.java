package com.tsnav.udf;

import com.aliyun.odps.udf.UDF;

public class MinizoneNJ extends UDF {
	// 银城西堤国际+万科光明城市
	private int[] yw_lon;
	private int[] yw_lat;

	public MinizoneNJ() {
		yw_lon = new int[] {118742263,118754049,118746072,118736676  };
		yw_lat = new int[] {32014739 ,32008555, 31999981, 32006763 };
	}
	public Long whichZone(Double lon, Double lat) {
		Long zone = null;
		int lon_new = (int) (lon * 1000000);
		int lat_new = (int) (lat * 1000000);

		if(this.contains(yw_lon, yw_lat, lon_new, lat_new)) {
			zone = 1L;
		}
		return zone;
	}

	public boolean contains(int[] xpoints, int[] ypoints, int x, int y) {
		int npoints = xpoints.length;

		if (npoints <= 2) {
			return false;
		}

		int bounds_min_x = Integer.MAX_VALUE;
		int bounds_min_y = Integer.MAX_VALUE;
		int bounds_max_x = Integer.MIN_VALUE;
		int bounds_max_y = Integer.MIN_VALUE;

		for (int i = 0; i < npoints; i++) {
			int tmp_x = xpoints[i];
			bounds_min_x = Math.min(bounds_min_x, tmp_x);
			bounds_max_x = Math.max(bounds_max_x, tmp_x);
			int tmp_y = ypoints[i];
			bounds_min_y = Math.min(bounds_min_y, tmp_y);
			bounds_max_y = Math.max(bounds_max_y, tmp_y);
		}

		if (!(x >= bounds_min_x && y >= bounds_min_y && x < bounds_max_x && y < bounds_max_y)) {
			return false;
		}

		int hits = 0;

		int lastx = xpoints[npoints - 1];
		int lasty = ypoints[npoints - 1];
		int curx, cury;

		// Walk the edges of the polygon
		for (int i = 0; i < npoints; lastx = curx, lasty = cury, i++) {
			curx = xpoints[i];
			cury = ypoints[i];

			if (cury == lasty) {
				continue;
			}

			int leftx;
			if (curx < lastx) {
				if (x >= lastx) {
					continue;
				}
				leftx = curx;
			} else {
				if (x >= curx) {
					continue;
				}
				leftx = lastx;
			}

			double test1, test2;
			if (cury < lasty) {
				if (y < cury || y >= lasty) {
					continue;
				}
				if (x < leftx) {
					hits++;
					continue;
				}
				test1 = x - curx;
				test2 = y - cury;
			} else {
				if (y < lasty || y >= cury) {
					continue;
				}
				if (x < leftx) {
					hits++;
					continue;
				}
				test1 = x - lastx;
				test2 = y - lasty;
			}

			if (test1 < (test2 / (lasty - cury) * (lastx - curx))) {
				hits++;
			}
		}

		return ((hits & 1) != 0);
	}

	public Long evaluate(Double lon, Double lat) {
		if (lon == null || lat == null) {
			return null;
		}
		return this.whichZone(lon, lat);
	}
}