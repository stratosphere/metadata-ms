grammar Sodap;

options {
  language = Java ;
}

commandline
  : command arg* ;

command
  : IDENTIFIER;


arg
  : referenceArg | variableArg | numberArg | stringArg
  ;

referenceArg
  : REFERENCE
  ;

variableArg
  : VARIABLE
  ;

numberArg
  : number
  ;

stringArg
  : STRING
  | IDENTIFIER
  | WORD
  ;


number
  : NUMBER
  ;

string
  : STRING
  ;

json:   object
    |   array
    ;

object
    :   '{' pair (',' pair)* '}'
    |   '{' '}' // empty object
    ;
pair:   STRING ':' value ;

array
    :   '[' value (',' value)* ']'
    |   '[' ']' // empty array
    ;

value
    :   STRING
    |   NUMBER
    |   REFERENCE
    |   VARIABLE
//    |   object  // recursion
//    |   array   // recursion
    |   'true'  // keywords
    |   'false'
    |   'null'
    ;

NULL : 'Null' ;

STRING :  '"' (ESC | VARIABLE | ~[$"\\])* '"' ;

fragment ESC :   '\\' ([$"\\bfnrt] | UNICODE) ;
fragment UNICODE : 'u' HEX HEX HEX HEX ;
fragment HEX : [0-9a-fA-F] ;
fragment CHAR : [a-zA-Z] ;
fragment DIGIT : [0-9] ;

NUMBER
    :   '-'? INT '.' [0-9]+ EXP? // 1.35, 1.35E-9, 0.3, -4.5
    |   '-'? INT EXP             // 1e10 -3e4
    |   '-'? INT                 // -3, 45
    ;

fragment INT :   '0' | [1-9] [0-9]* ; // no leading zeros
fragment EXP :   [Ee] [+\-]? INT ; // \- since - means "range" inside [...]

WS  :   [ \t\n\r]+ -> skip ;


REFERENCE
  : '*' IDENTIFIER
  | '*{' IDENTIFIER ( '.' IDENTIFIER )* '}' ;

VARIABLE
  : '$' IDENTIFIER
  | '${' IDENTIFIER ( '.' IDENTIFIER )* '}';

IDENTIFIER
  : CHAR (CHAR | DIGIT)* ;


WORD: (~[ \t\n\r])+ ;
