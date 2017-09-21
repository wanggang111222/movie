package oracle.demo.oow.bd.util;

public class TableInfo{
	private String value;
	private int value1;
	public String family;
    public String qualifier;
	public int getValue1() {
		return value1;
	}
	public void setValue1(int value1) {
		this.value1 = value1;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public String getFamily() {
		return family;
	}
	public void setFamily(String family) {
		this.family = family;
	}
	public String getQualifier() {
		return qualifier;
	}
	public void setQualifier(String qualifier) {
		this.qualifier = qualifier;
	}
	public TableInfo(String family, String qualifier,String value) {
		super();
		this.value = value;
		this.family = family;
		this.qualifier = qualifier;
	}
	public TableInfo(String family, String qualifier,int value1) {
		super();
		this.value1 = value1;
		this.family = family;
		this.qualifier = qualifier;
	}
	public TableInfo(String family, String qualifier) {
		super();
		this.family = family;
		this.qualifier = qualifier;
	}
	

    
}
