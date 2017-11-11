package com.lpy.spark_applog;

import java.io.Serializable;

import scala.math.Ordered;

public class AccessLogSortKey implements Ordered<AccessLogSortKey>, Serializable {

	private static final long serialVersionUID = 1L;

	private long timestamp;
	private long upTraffic;
	private long downTraffic;

	
	
	
	public AccessLogSortKey() {
	}

	public AccessLogSortKey(long timestamp, long upTraffic, long downTraffic) {
		this.timestamp = timestamp;
		this.upTraffic = upTraffic;
		this.downTraffic = downTraffic;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public long getUpTraffic() {
		return upTraffic;
	}

	public void setUpTraffic(long upTraffic) {
		this.upTraffic = upTraffic;
	}

	public long getDownTraffic() {
		return downTraffic;
	}

	public void setDownTraffic(long downTraffic) {
		this.downTraffic = downTraffic;
	}

	public boolean $greater(AccessLogSortKey other) {
		if (upTraffic > other.upTraffic) {
			return true;
		} else if (upTraffic == other.upTraffic && downTraffic > other.downTraffic) {
			return true;
		} else if (upTraffic == other.upTraffic && downTraffic == other.downTraffic && timestamp > other.timestamp) {
			return true;
		}
		return false;
	}

	public boolean $greater$eq(AccessLogSortKey other) {
		if ($greater(other)) {
			return true;
		} else if (upTraffic == other.upTraffic && downTraffic == other.downTraffic && timestamp == other.timestamp) {
			return true;
		}
		return false;
	}

	public boolean $less(AccessLogSortKey other) {
		if (upTraffic < other.upTraffic) {
			return true;
		} else if (upTraffic == other.upTraffic && downTraffic < other.downTraffic) {
			return true;
		} else if (upTraffic == other.upTraffic && downTraffic == other.downTraffic && timestamp < other.timestamp) {
			return true;
		}
		return false;
	}

	public boolean $less$eq(AccessLogSortKey other) {
		if ($less(other)) {
			return true;
		} else if (upTraffic == other.upTraffic && downTraffic == other.downTraffic && timestamp == other.timestamp) {
			return true;
		}
		return false;
	}

	public int compare(AccessLogSortKey other) {
		if (upTraffic - other.upTraffic != 0) {
			return (int) (upTraffic - other.upTraffic);
		} else if (downTraffic - other.downTraffic != 0) {
			return (int) (downTraffic - other.downTraffic);
		} else if (timestamp - other.timestamp != 0) {
			return (int) (timestamp - other.timestamp);
		}
		return 0;
	}

	public int compareTo(AccessLogSortKey other) {
		if (upTraffic - other.upTraffic != 0) {
			return (int) (upTraffic - other.upTraffic);
		} else if (downTraffic - other.downTraffic != 0) {
			return (int) (downTraffic - other.downTraffic);
		} else if (timestamp - other.timestamp != 0) {
			return (int) (timestamp - other.timestamp);
		}
		return 0;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (downTraffic ^ (downTraffic >>> 32));
		result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
		result = prime * result + (int) (upTraffic ^ (upTraffic >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AccessLogSortKey other = (AccessLogSortKey) obj;
		if (downTraffic != other.downTraffic)
			return false;
		if (timestamp != other.timestamp)
			return false;
		if (upTraffic != other.upTraffic)
			return false;
		return true;
	}

}
