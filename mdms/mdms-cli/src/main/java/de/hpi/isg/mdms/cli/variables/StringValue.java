package de.hpi.isg.mdms.cli.variables;


import de.hpi.isg.mdms.cli.exceptions.CliException;
import it.unimi.dsi.fastutil.chars.Char2CharMap;
import it.unimi.dsi.fastutil.chars.Char2CharOpenHashMap;
import org.apache.commons.lang3.Validate;

import java.util.regex.Pattern;

/**
 * {@link ContextObject} representation of {@link String}.
 */
public class StringValue extends ContextValue<String> {

    private static final Char2CharMap escapeChars = new Char2CharOpenHashMap();

    static {
        escapeChars.put('\t', 't');
        escapeChars.put('\n', 'n');
        escapeChars.put('\r', 'r');
        escapeChars.defaultReturnValue('\0');
    }

    private static final Char2CharMap escapableChars = new Char2CharOpenHashMap();

    static {
        escapableChars.put('"', '"');
        escapableChars.put('$', '$');
        escapableChars.put('\\', '\\');
        escapableChars.put('t', '\t');
        escapableChars.put('r', '\r');
        escapableChars.put('n', '\n');
        escapableChars.defaultReturnValue('\0');
    }

    private static final Pattern VARIABLE_PATTERN = Pattern.compile(
            "(?:^|\\s)" +
                    "((?:\\$[a-zA-Z][a-zA-Z\\d]*)|" +
                    "(?:\\$\\{[a-zA-Z][a-zA-Z\\d]*(?:\\.[a-zA-Z][a-zA-Z\\d]*)*\\}))" +
                    "(?:$|\\s)");

    private final String value;

    public StringValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return String.format("%s[value=%s]", getClass().getSimpleName(), this.value);
    }

    @Override
    public String toParseableString() {
        return escape(this.value);
    }

    @Override
    public String toReadableString() {
        return this.value;
    }

    @Override
    public String getValue() {
        return this.value;
    }

    @Override
    public boolean isValue() {
        return true;
    }

    @Override
    public boolean isReference() {
        return false;
    }

//    /**
//     * Resolves all variables within the given namespace.
//     * @param str
//     * @param globalNamespace
//     * @return
//     */
//    public static String resolveVariables(CharSequence str, Namespace globalNamespace) {
//        CharSequence returnValue = str;
//        Matcher matcher = VARIABLE_PATTERN.matcher(returnValue);
//        while (matcher.matches()) {
//            final String variableCode = matcher.group(1);
//            String replacementValue = parseAndResolve(variableCode, globalNamespace).toReadableString();
//            int matchStart = matcher.start(1);
//            CharSequence prefix = str.subSequence(0, matchStart);
//            int matchEnd = matcher.end(1);
//            final CharSequence suffix = str.subSequence(matchEnd, str.length());
//            returnValue = prefix + replacementValue + suffix;
//            matcher = VARIABLE_PATTERN.matcher(returnValue);
//        }
//        return returnValue.toString();
//    }

    /**
     * Turns the given characters into a parseable format.
     *
     * @param str the characters to escape
     * @return the escaped {@link String}
     * @see #parse(CharSequence,Namespace)
     */
    public static String escape(CharSequence str) {
        StringBuilder sb = new StringBuilder(str.length() * 2);
        sb.append('"');
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            char escape = escapeChars.get(c);
            if (escape != '\0') {
                sb.append('\\').append(escape);
            } else {
                sb.append(c);
            }
        }
        sb.append('"');
        return sb.toString();
    }

    /**
     * Parses the given characters.
     *
     * @param str characters to parse
     * @param namespace provides values for variables
     * @return the parsed {@link String}
     * @see #escape(CharSequence)
     */
    public static String parse(CharSequence str, Namespace namespace) throws CliException {
        boolean isQuotedString = str.charAt(0) == '"';
        StringBuilder sb = new StringBuilder(isQuotedString ? str.length() - 2 : 0);
        int len = str.length();
        for (int i = isQuotedString ? 1 : 0; i < (isQuotedString ? len - 1 : len); i++) {
            char c = str.charAt(i);
            if (c == '\\') {
                // Handle escape characters.
                c = str.charAt(++i);
                c = escapableChars.get(c);
                if (c == '\0') {
                    throw new IllegalArgumentException("Illegal escape sequence.");
                }
                sb.append(c);

            } else if (c == '$') {
                int variableStart = i;
                // Handle variables.
                c = str.charAt(++i);
                i++;
                if (c == '{') {
                    while (str.charAt(i) != '}') {
                        i++;
                    }
                } else {
                    while (i < str.length() && !Character.isSpaceChar(str.charAt(i)) && str.charAt(i) != '"') {
                        i++;
                    }
                    i--;
                }
                CharSequence variableCode = str.subSequence(variableStart, i + 1);
                final ContextObject variable = ContextObjects.parseAndResolve(variableCode, namespace);
                sb.append(variable.toReadableString());
            } else {
                sb.append(c);
            }
        }
        Validate.isTrue(!isQuotedString || str.charAt(len - 1) == '"');
        return sb.toString();
    }

    public static StringValue fromCode(CharSequence code, Namespace globalNamespace) throws CliException {
        return new StringValue(parse(code, globalNamespace));
    }
}
