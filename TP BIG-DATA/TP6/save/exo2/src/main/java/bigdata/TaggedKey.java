package bigdata;

public class TaggedKey {
	public String keyName;
	public int typeKey;
	
	//Type = 0 VILLE
	//Type = 1 REGION
	public TaggedKey() {
		this.keyName = new String("");
	}
	public TaggedKey(String name, int type) {
		this.keyName = name;
		this.typeKey = type;
	}
}
