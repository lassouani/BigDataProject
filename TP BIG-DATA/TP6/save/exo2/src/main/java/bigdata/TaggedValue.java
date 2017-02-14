package bigdata;

public class TaggedValue {
	public String name;
	public int typeValue;
	
	//Type = 0 VILLE
	//Type = 1 REGION
	public TaggedValue() {
		this.name= new String();
	}
	public TaggedValue(String name, int type) {
		this.name = name;
		this.typeValue = type;
	}
}
