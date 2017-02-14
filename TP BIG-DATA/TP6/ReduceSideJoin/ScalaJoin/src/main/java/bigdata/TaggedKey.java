package bigdata;

public class TaggedKey {
	public String keyName;
	public Integer typeKey;
	
	//Type = 0 VILLE
	//Type = 1 REGION
	public TaggedKey() {
		this.keyName = new String("");
	}
	public TaggedKey(String name, Integer type) {
		this.keyName = name;
		this.typeKey = type;
	}
	public String getName() {
		return this.keyName.toLowerCase();
	}
	
}
