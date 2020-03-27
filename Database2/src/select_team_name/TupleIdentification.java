package select_team_name;

import java.io.Serializable;

public class TupleIdentification  {
			
	
	int tupleID;
	String pageName;
	
	
	
	public TupleIdentification(int tupleID , String pageName) {
		this.pageName = pageName;
		this.tupleID = tupleID;
	}
	
	
	public String toString() {
		return (this.tupleID + "," + this.pageName);
	}
}
