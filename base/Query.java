package ed.inf.adbs.minibase.base;

import ed.inf.adbs.minibase.Utils;

import java.util.List;

public class Query {
    private RelationalAtom head;
    private List<Atom> body;
    private String agg;
    public Query(RelationalAtom head, List<Atom> body, String agg) {
        this.head = head;
        this.body = body;
        this.agg = agg;
    }

    public RelationalAtom getHead() {
        return head;
    }

    public List<Atom> getBody() {
        return body;
    }

    public String getAgg() {
        return agg;
    }

    @Override
    public String toString() {
        return head + " :- " + Utils.join(body, ", ");
    }
}
