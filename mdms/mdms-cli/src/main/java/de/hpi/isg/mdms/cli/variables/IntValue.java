package de.hpi.isg.mdms.cli.variables;


import de.hpi.isg.mdms.cli.exceptions.CliException;

/**
 * This value represents an {@code int}.
 */
public class IntValue extends ContextValue<Integer> {

    private final Integer value;

    public IntValue(int value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return String.format("%s[value=%d]", getClass().getSimpleName(), this.value);
    }

    @Override
    public String toParseableString() {
        return String.valueOf(this.value);
    }

    @Override
    public String toReadableString() {
        return toParseableString();
    }

    @Override
    public Integer getValue() {
        return this.value;
    }


    /**
     * Creates a new instance by parsing the given code.
     * @param code represents an int
     * @return the parsed {@link IntValue}
     * @throws CliException if the parsing fails
     */
    public static IntValue fromCode(CharSequence code) throws CliException {
        try {
            return new IntValue(Integer.parseInt(code.toString()));
        } catch (NumberFormatException e) {
            throw new CliException("Could not parse " + code, e);
        }
    }
}
