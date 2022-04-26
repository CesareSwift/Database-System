# Database-System
This is a lightweight in-memory database system from coursework of the course Advanced Database System.
The first part of this project minimize conjunction queries.
The second part of this project translate conjunction queries, evaluates and implements the most common relational operators, and outputs the correct query answers.

##### Logic for extracting Join conditions:

Step1.Detect whether the two joined relations have common variables. if have, only join the tuple which the values of the common variables in two relations are the same.

Furthermore, make sure that the new generate relation after join only has the common variables once.

e.g. for a query body R(x, y, z) Q(x, s, t), make sure R.x = Q.x before join, and return (x, y, z, s, t)

Step2.Detect whether the body have expressions which the former element is in one of the relation and the latter elements is in the other. if have, when joining, only join the two relations when the tuple of the two relation staisfied the expression.

e.g. for a query body R(x, y, z) Q(r, s, t) x = s , only do join operation when x = s.

Step3.If none of step1 and step2 is staisfied, do a cross product.

##### Other Information

1.As for query parse, I push down all the select Operator (if have) upside the join Operator (if have) to save the cost time of join. I only do a complete cross product if there are no other options.

2.To implement Group-By aggregation, I extend QueryParser.java and Query.java to make head contain information about SUM and AVG.
