package bigdata;

public class TaggedValue {
	public String name;
	public Integer typeValue;
	
	//Type = 0 VILLE
	//Type = 1 REGION
	public TaggedValue() {
		this.name= new String();
	}
	public TaggedValue(String name, Integer type) {
		this.name = name;
		this.typeValue = type;
	}
}
