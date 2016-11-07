package com.tsnav.udf;

public class App {

	// zi yu shan zhuang
	private int[] zysz_lon;
	private int[] zysz_lat;

	// zi yu hua fu
	private int[] zyhf_lon;
	private int[] zyhf_lat;

	// yi he yuan zhu
	private int[] yhyz_lon;
	private int[] yhyz_lat;

	// wan cheng hua fu
	private int[] wchf_lon;
	private int[] wchf_lat;

	// guang qu jin mao fu
	private int[] jmf_lon;
	private int[] jmf_lat;

	// fan hai guo ji
	private int[] fhgj_lon;
	private int[] fhgj_lat;

	// diao yu tai qi hao yuan
	private int[] qhy_lon;
	private int[] qhy_lat;

	public App() {
		zysz_lon = new int[] { 116414820, 116414721, 116422518, 116422069 };
		zysz_lat = new int[] { 40021519, 40015924, 40015779, 40021066 };

		zyhf_lon = new int[] { 116415687, 116417906, 116417578, 116414142 };
		zyhf_lat = new int[] { 40048757, 40048543, 40045998, 40046706 };

		yhyz_lon = new int[] { 116290355, 116290589, 116297389, 116297443, 116295035 };
		yhyz_lat = new int[] { 40004329, 40003099, 40003044, 40003825, 40004716 };

		wchf_lon = new int[] { 116297829, 116300758, 116299850, 116301261, 116300663, 116295889 };
		wchf_lat = new int[] { 39973811, 39974267, 39975615, 39975974, 39978106, 39977357 };

		jmf_lon = new int[] { 116489902, 116489789, 116495781, 116496046 };
		jmf_lat = new int[] { 39902819, 39899446, 39899484, 39903006 };

		fhgj_lon = new int[] { 116501512, 116507693, 116508124, 116501728 };
		fhgj_lat = new int[] { 39946632, 39946577, 39942428, 39940485 };

		qhy_lon = new int[] { 116325978, 116326194, 116327973, 116328071, 116331988, 116332150 };
		qhy_lat = new int[] { 39925486, 39926372, 39926489, 39926019, 39926455, 39925217 };
	}

	public static void main(String[] args) {

		Double lon1 = 116.414820;
		Double lat1 = 40.015924;

		App app = new App();
		System.out.println("zone to be 1: " + app.whichZone2(lon1, lat1));

		Double lon2 = 116.414820;
		Double lat2 = 40.014721;
		System.out.println("zone to be null: " + app.whichZone2(lon2, lat2));

		Double lon3 = 116.417578;
		Double lat3 = 40.046706;
		System.out.println("zone to be 2: " + app.whichZone2(lon3, lat3));

		Double lon4 = 116.417578;
		Double lat4 = 40.045999;
		System.out.println("zone to be 2: " + app.whichZone2(lon4, lat4));

		// Test on the vertex
		Double lon5 = 116.417578;
		Double lat5 = 40.045998;
		System.out.println("zone to be null: " + app.whichZone2(lon5, lat5));
	}

	public Long whichZone2(Double lon, Double lat) {
		Long zone = null;
		int lon_new = (int) (lon * 1000000);
		int lat_new = (int) (lat * 1000000);

		if (this.contains(zysz_lon, zysz_lat, lon_new, lat_new)) {
			zone = 1L;
		} else if (this.contains(zyhf_lon, zyhf_lat, lon_new, lat_new)) {
			zone = 2L;
		} else if (this.contains(yhyz_lon, yhyz_lat, lon_new, lat_new)) {
			zone = 3L;
		} else if (this.contains(wchf_lon, wchf_lat, lon_new, lat_new)) {
			zone = 4L;
		} else if (this.contains(jmf_lon, jmf_lat, lon_new, lat_new)) {
			zone = 5L;
		} else if (this.contains(fhgj_lon, fhgj_lat, lon_new, lat_new)) {
			zone = 6L;
		} else if (this.contains(qhy_lon, qhy_lat, lon_new, lat_new)) {
			zone = 7L;
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
}
