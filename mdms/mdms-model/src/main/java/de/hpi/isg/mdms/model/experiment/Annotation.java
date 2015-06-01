package de.hpi.isg.mdms.model.experiment;

public class Annotation{
	private final String tag;
	private final String text;
	
	public Annotation(String left, String right){
		this.tag = left;
		this.text = right;
	}
	
	public String getTag(){
		return this.tag;
	}
	
	public String getText(){
		return this.text;
	}
}


