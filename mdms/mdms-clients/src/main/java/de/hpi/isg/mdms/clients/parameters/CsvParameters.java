package de.hpi.isg.mdms.clients.parameters;

import com.beust.jcommander.Parameter;

import java.util.Objects;

public class CsvParameters {

    public static final char NO_QUOTE_CHAR = '\0';

    @Parameter(names = { "--field-separator" }, description = "the delimiter of fields in each line of the output (and input) file (semicolon, comma, tab)")
    public String fieldSeparatorName = "semicolon";

    @Parameter(names = { "--quote-char" }, description = "the quote of fields in each line of the input (and output) file (none, single, double)")
    public String quoteCharName = "double";

	@Parameter(names = { "--null-string"}, description = "string representation of null", required = false)
	public String nullString = null;
    
    private boolean isInitialized;
    
    private char fieldSeparatorChar;
    
    private char quoteChar;
    
    /**
     * This constructor is to be used when the parameters are configured via the {@link Parameter} annotated fields.
     */
    public CsvParameters() {
    	this.isInitialized = false;
    }

    
    /**
     * This constructor can be used to programmatically create CSV parameters.
     * @param fieldSeparatorChar is a character for separating fields in a CSV file.
     * @param quoteChar is a single or double quote or {@link #NO_QUOTE_CHAR}.
     */
    public CsvParameters(char fieldSeparatorChar, char quoteChar) {
		this.fieldSeparatorChar = fieldSeparatorChar;
		this.quoteChar = quoteChar;
		this.isInitialized = true;
	}



	private void initialize() {
    	// Initialize field separator char.
    	if ("comma".equalsIgnoreCase(this.fieldSeparatorName)) {
    		this.fieldSeparatorChar = ',';
        } else if ("semicolon".equalsIgnoreCase(this.fieldSeparatorName)) {
        	this.fieldSeparatorChar = ';';
        } else if ("tab".equalsIgnoreCase(this.fieldSeparatorName)) {
        	this.fieldSeparatorChar = '\t';
        } else {
            throw new IllegalArgumentException(String.format(
                    "Unkown field separator: \"%s\".", this.fieldSeparatorName));
        }
    	
    	// Initialize quote char.
    	if ("none".equalsIgnoreCase(this.quoteCharName)) {
            this.quoteChar = NO_QUOTE_CHAR;
        } else if ("single".equalsIgnoreCase(this.quoteCharName)) {
        	this.quoteChar = '\'';
        } else if ("double".equalsIgnoreCase(this.quoteCharName)) {
        	this.quoteChar = '"';
        } else {
            throw new IllegalArgumentException(String.format(
                    "Unkown quote type: \"%s\".", this.quoteCharName));
        }
    	
    }

    public char getFieldSeparatorChar() {
        if (!this.isInitialized) {
        	initialize();
        }
        
        return this.fieldSeparatorChar;
    }

    public char getQuoteChar() {
    	if (!this.isInitialized) {
    		initialize();
    	}
    	
    	return this.quoteChar;
    }

	public String getNullString() {
		return this.nullString;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + getFieldSeparatorChar();
		result = prime * result + getQuoteChar();
		result = prime * result + Objects.hashCode(this.nullString);
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
		CsvParameters other = (CsvParameters) obj;
		if (getFieldSeparatorChar() != other.getFieldSeparatorChar())
			return false;
		if (getQuoteChar() != other.getQuoteChar())
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "CsvParameters [separator=" + getFieldSeparatorChar() + "quote=" + getQuoteChar() + "]";
	}
	
	
    
}
