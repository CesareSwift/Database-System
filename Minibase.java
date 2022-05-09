package ed.inf.adbs.minibase;

//import com.sun.org.apache.bcel.internal.generic.Select;
import ed.inf.adbs.minibase.base.Atom;
import ed.inf.adbs.minibase.base.Query;
import ed.inf.adbs.minibase.base.RelationalAtom;
import ed.inf.adbs.minibase.parser.QueryParser;
//import jdk.incubator.vector.VectorOperators;

//import javax.swing.*;
import java.io.*;
import java.nio.file.Paths;
import java.util.*;

/**
 * In-memory database system
 *
 */
public class Minibase {

    public static void main(String[] args) throws IOException {

//        if (args.length != 3) {
//            System.err.println("Usage: Minibase database_dir input_file output_file");
//            return;
//        }

//        String databaseDir = args[0];
//        String inputFile = args[1];
//        String outputFile = args[2];

//        evaluateCQ(databaseDir, inputFile, outputFile);

//        //IDE test example:
        String databaseDir = "C:\\Users\\cesar\\Desktop\\ADBS\\Minibase\\data\\evaluation\\db";
        String inputFile = "C:\\Users\\cesar\\Desktop\\ADBS\\Minibase\\data\\evaluation\\input\\query8.txt";
        String outputFile = "C:\\Users\\cesar\\Desktop\\ADBS\\Minibase\\data\\evaluation\\output\\query8.csv";
        evaluateCQ(databaseDir, inputFile, outputFile);
        parsingExample(inputFile);

//        parsingExample(inputFile);
    }

    public static void evaluateCQ(String databaseDir, String inputFile, String outputFile) throws IOException {
        // TODO: add your implementation
        String input = read_file(inputFile);
        Query parsed_query = QueryParser.parse(input);
        RelationalAtom head = parsed_query.getHead();
        List<String> head_list = new ArrayList<>();
        List<String> body_list = new ArrayList<>();
        String head_string = String.valueOf(head);
        String temph = "";
        //parse the head by '(', ')' and ','; e.g. Q(x,y) -> List[x, y]
        for(int n1 = 0; n1 < head_string.length(); n1++) {
            char cur = head_string.charAt(n1);
            if(cur!='('&&cur!=')'&&cur!=',') {
                temph += cur;
            }
            else {
                if(cur!='(') {
                    head_list.add(temph);
                    System.out.println(temph);
                }
                temph = "";
            }
            if(cur==',') {
                n1++;
            }
        }
        List<Atom> body = parsed_query.getBody();
        //store the List<Atom> body in List<String> body_list using String.valueof();
        for(int n2 = 0; n2 < body.size(); n2++) {
            body_list.add(String.valueOf(body.get(n2)));
            System.out.println(String.valueOf(body.get(n2)));
        }
//
//        parse_query(head_list, body_list,databaseDir,outputFile);

//        DatabaseCatalog cur_db = DatabaseCatalog.getInstance();
//        cur_db.setDatabaseDir(databaseDir);
//        ScanOperator s1 = new ScanOperator("R");
//        ScanOperator s2 = new ScanOperator("S");
//        JoinOperator j = new JoinOperator(s1,s2,body_list);
//        j.setfirst_left();
//        List<String> new_l = new ArrayList<>();
//        List<String> new_h = new ArrayList<>();
//        new_l.add("M(x, y, z, w, t)");
//        new_h.add("y");
//        ProjectOperator p = new ProjectOperator(j,new_h,new_l);
//        AvgOperator avg = new AvgOperator(p,0);
//        avg.getNextTuple();
//        sum.dump();

//        Query new_query = QueryParser.parse(input);
//        RelationalAtom head1 = new_query.
//        List<Atom> body1 = parsed_query.getBody();

    }

    /**
     * read input from inputFile;
     */
    public static String read_file(String path) throws IOException {
        File inputFile_name = new File(path);
        InputStreamReader reader = new InputStreamReader(
                new FileInputStream(inputFile_name));
        BufferedReader br = new BufferedReader(reader);
        String input_query = "";
        input_query = br .readLine();
//        System.out.println(input_query);
        return input_query;
    }

    /**
     * write the CQ evaluate output in the path outputFile;
     */
    public static void store_file(String path, List<String> output) throws IOException {
        File outputFile_name = new File(path);
//        System.out.println("output"+ path);
        outputFile_name.createNewFile();
        BufferedWriter out = new BufferedWriter(new FileWriter(path));
        Set<String> set = new HashSet<>();
        for(String str: output) {
            if(set.contains(str))
                continue;
            set.add(str);
            if(str==null)
                continue;
            String [] temp = str.split(" ");
            String new_line = "";
            for(int i = 0; i < temp.length; i++) {
                new_line += temp[i];
                if(i!=temp.length-1)
                    new_line += ",";
            }
            out.write(new_line);
            out.newLine();
        }
        out.flush();
        out.close();
    }
    public static  void parse_query(List<String> head, List<String> body, String databaseDir, String outputFile) {
        DatabaseCatalog cur_db = DatabaseCatalog.getInstance();
        cur_db.setDatabaseDir(databaseDir);
        Operator rootOperator;
        int head_size = head.size();
        List<String> database_name = new ArrayList<>();
        List<String> relation = new ArrayList<>();
        int relation_num = 0;
        int selection_num = 0;
        int relation_atom = 0;
        for(int i = 0; i < body.size(); i++) {
            String cur_atom = body.get(i);
            if(!evaluate_select_conditions.isExpression(cur_atom)) {
                relation_num++;
                String temp1 = "";
                for(int k = 0; k < cur_atom.length(); k++) {
                    char cur = cur_atom.charAt(k);
                    if (cur != '(' && cur != ')' && cur != ',') {
                        temp1 += cur;
                    }
                    else {
                        if(cur!='(') {
                            if(!relation.contains(temp1)) {
                                relation.add(temp1);
                            }
                        }
                        else {
                            database_name.add(temp1);
                        }
                        temp1 = "";
                    }
                    if (cur == ',') {
                        k++;
                    }
                }
            }
        }
        relation_atom = relation.size();
        boolean need_projection = false;
        boolean need_join = false;
        boolean need_selection = false;
        ArrayList<ScanOperator> scan_list= new ArrayList<ScanOperator>();
        for(int i = 0; i < relation_num; i++) {
            scan_list.add(new ScanOperator(database_name.get(i)));
        }
        HashMap<Integer,List<String>> select_map = new HashMap<>();
        HashMap<Integer,List<String>> join_map = new HashMap<>();
        System.out.println(relation_num);
        for(int i = relation_num; i < body.size(); i++) {
            System.out.println(body.get(i));
            String cur_expression = body.get(i);
            String[] express_Atom;
            String former = "";
            String latter = "";
            if (cur_expression.indexOf(" = ") > 0 || cur_expression.indexOf(" != ") > 0 ||
                    cur_expression.indexOf(" > ") > 0 || cur_expression.indexOf(" < ") > 0 ||
                    cur_expression.indexOf(" >= ") > 0 || cur_expression.indexOf(" <= ") > 0
            ) {
//                System.out.println("expression: " +cur_expression);
                if(cur_expression.indexOf(" = ") > 0) {
                    express_Atom = cur_expression.split(" = ");
                    former = express_Atom[0];
                    latter = express_Atom[1];
                }
                else if(cur_expression.indexOf(" != ") > 0) {
                    express_Atom = cur_expression.split(" != ");
                    former = express_Atom[0];
                    latter = express_Atom[1];
                }
                else if(cur_expression.indexOf(" > ") > 0) {
                    express_Atom = cur_expression.split(" > ");
                    former = express_Atom[0];
                    latter = express_Atom[1];
                }
                else if(cur_expression.indexOf(" < ") > 0) {
                    express_Atom = cur_expression.split(" < ");
                    former = express_Atom[0];
                    latter = express_Atom[1];
                }
                else if(cur_expression.indexOf(" >= ") > 0) {
                    express_Atom = cur_expression.split(" >= ");
                    former = express_Atom[0];
                    latter = express_Atom[1];
                }
                else if(cur_expression.indexOf(" <= ") > 0) {
                    express_Atom = cur_expression.split(" <= ");
                    former = express_Atom[0];
                    latter = express_Atom[1];
                }
                int position_f = relation.indexOf(former);
                if (isPositive(latter) || latter.charAt(0) == '\'') {
                    need_selection = true;
                    for (int k = 0; k < relation_num; k++) {
                        String cur_relation = body.get(k);
                        String temp2 = "";
                        List<String> single_relation = new ArrayList<>();
                        for (int k1 = 0; k1 < cur_relation.length(); k1++) {
                            char cur1 = cur_relation.charAt(k1);
                            if (cur1 != '(' && cur1 != ')' && cur1 != ',') {
                                temp2 += cur1;
                            } else {
                                if (cur1 != '(') {
                                    if (!single_relation.contains(temp2.trim())) {
                                        single_relation.add(temp2.trim());
//                                        System.out.println("test: "+ temp2);
                                    }
                                }
                                temp2 = "";
                            }
                            if (cur1 == ',') {
                                k1++;
                            }
                        }
                        // question
//                        System.out.println("test: "+ single_relation.contains(former));
                        if (single_relation.contains(former)) {
                            if (select_map.get(k) != null) {
                                List<String> condition_list = select_map.get(k);
                                condition_list.add(cur_expression);
                                select_map.put(k, condition_list);
                            } else {
                                List<String> new_con = new ArrayList<>();
                                new_con.add(cur_expression);
                                select_map.put(k, new_con);
                            }
                        }
                    }
                }
                else {
                    int tag = 0;
                    for (int k = 0; k < relation_num; k++) {
                        String cur_relation = body.get(k);
                        String temp2 = "";
                        List<String> single_relation = new ArrayList<>();
                        for (int k1 = 0; k1 < cur_relation.length(); k1++) {
                            char cur1 = cur_relation.charAt(k1);
                            if (cur1 != '(' && cur1 != ')' && cur1 != ',') {
                                temp2 += cur1;
                            } else {
                                if (cur1 != '(') {
                                    if (!single_relation.contains(temp2)) {
                                        single_relation.add(temp2);
                                    }
                                }
                                temp2 = "";
                            }
                            if (cur1 == ',') {
                                k1++;
                            }
                        }
                        if (single_relation.contains(former) || single_relation.contains(latter)) {
                            tag = k;
                        }
                    }
                    if (join_map.get(tag) != null) {
                        List<String> condition_list = join_map.get(tag);
                        condition_list.add(cur_expression);
                        join_map.put(tag, condition_list);
                    } else {
                        List<String> new_con = new ArrayList<>();
                        new_con.add(cur_expression);
                        join_map.put(tag, new_con);
                    }
                }
            }
        }

        if(head_size < relation_atom) {
            need_projection = true;
        }
        if(relation_num > 1)
            need_join = true;
        int select_num = select_map.size();
        List<Integer> have_select = new ArrayList<>();
        ArrayList<SelectOperator> select_list= new ArrayList<SelectOperator>();
//        System.out.println(need_selection);
        if(need_selection) {
            Iterator it = select_map.keySet().iterator();
//            System.out.println(select_map.size());
            while(it.hasNext()) {
                Object key = it.next();
                List<String> val = select_map.get(key);
                int a = Integer.valueOf(String.valueOf(key));
//                System.out.println("a" + a);
                have_select.add(a);
                List<String> conditions = new ArrayList<>();
                conditions.add(body.get(a));
                for(int j = 0; j < val.size(); j++)
                    conditions.add(val.get(j));
                select_list.add(new SelectOperator(scan_list.get(a),conditions));
            }
        }
        int join_num = relation_num - 1;
        ArrayList<JoinOperator> join_list= new ArrayList<JoinOperator>();
        if(need_join) {
            int index_select = 0;
            for(int i = 0; i < join_num; i++) {
                if(i == 0) {
                    List<String> join_con = new ArrayList<>();
                    join_con.add(body.get(0));
                    join_con.add(body.get(1));
                    if(join_map.get(i+1)!=null) {
                        List<String> temp = join_map.get(i+1);
                        for(int j = 0; j < temp.size(); j++) {
                            join_con.add(temp.get(j));
                        }
                    }
                    if(have_select.contains(0)&&have_select.contains(1)) {
                        join_list.add(new JoinOperator(select_list.get(0),select_list.get(1),join_con));
                        index_select += 2;
                    }
                    else if(!have_select.contains(0)&&have_select.contains(1)) {
                        join_list.add(new JoinOperator(scan_list.get(0),select_list.get(0),join_con));
                        index_select += 1;
                    }
                    else if(have_select.contains(0)&&!have_select.contains(1)) {
                        join_list.add(new JoinOperator(select_list.get(0),scan_list.get(1),join_con));
                        index_select += 1;
                    }
                    else if(!have_select.contains(0)&&!have_select.contains(1)) {
                        join_list.add(new JoinOperator(scan_list.get(0),scan_list.get(1),join_con));
                    }
                }
                else {
                    List<String> join_con = new ArrayList<>();
                    List<String> sub_relation = new ArrayList<>();
                    for(int j = 0; j <= i; j++) {
                        String cur_atom = body.get(j);
                        String temp1 = "";
                        for(int k = 0; k < cur_atom.length(); k++) {
                            char cur = cur_atom.charAt(k);
                            if (cur != '(' && cur != ')' && cur != ',') {
                                temp1 += cur;
                            }
                            else {
                                if(cur!='(') {
                                    if(!sub_relation.contains(temp1)) {
                                        sub_relation.add(temp1);
                                    }
                                }
                                temp1 = "";
                            }
                            if (cur == ',') {
                                k++;
                            }
                        }
                    }
                    String new_contain = "M(";
                    for(int m = 0; m < sub_relation.size(); m++) {
                        new_contain += sub_relation.get(m);
                        if(m!=sub_relation.size()-1) {
                            new_contain += ", ";
                        }
                        else
                            new_contain += ")";
                    }
                    join_con.add(new_contain);
                    join_con.add(body.get(i+1));
                    if(join_map.get(i+1)!=null) {
                        List<String> temp = join_map.get(i+1);
                        for(int j = 0; j < temp.size(); j++) {
                            join_con.add(temp.get(j));
                        }
                    }
                    if(have_select.contains(i+1)) {
                        join_list.add(new JoinOperator(join_list.get(i-1),select_list.get(index_select),join_con));
                        index_select += 1;
                    }
                    else {
                        join_list.add(new JoinOperator(join_list.get(i-1),select_list.get(i+1),join_con));
                    }
                }
            }
        }


        System.out.println("join: "  + need_join);
        System.out.println("select: "  + need_selection);
        System.out.println("project: "  + need_projection);
        List<String> result = new ArrayList<>();
        if(need_join==false && need_selection==false && need_projection==false) {
            rootOperator = scan_list.get(0);
            result = rootOperator.dump();
        }

        if(need_join==false && need_selection==false && need_projection==true) {
            ProjectOperator p = new ProjectOperator(scan_list.get(0), head, body);
            rootOperator = p;
            result = rootOperator.dump();
        }

        if(need_join==false && need_selection==true && need_projection==false) {
            rootOperator = select_list.get(0);
            result = rootOperator.dump();
        }

        if(need_join==false && need_selection==true && need_projection==true) {
            ProjectOperator p = new ProjectOperator(select_list.get(0), head, body);
            rootOperator = p;
            result = rootOperator.dump();
        }

        if(need_join==true  && need_projection==false) {
            for(int i = 0; i < join_list.size(); i++) {
                join_list.get(i).setfirst_left();
            }
            rootOperator = join_list.get(join_list.size()-1);
            result = rootOperator.dump();
        }



        if(need_join==true && need_selection==true && need_projection==true) {
            String temp_body = "M(";
            for(int j = 0; j<relation.size(); j++) {
                temp_body += relation.get(j);
                if(j!=relation.size()-1) {
                    temp_body += ", ";
                }
                else
                    temp_body += ")";
            }
            List<String> new_body = new ArrayList<>();
            new_body.add(temp_body);
            for(int i = 0; i < join_list.size(); i++) {
                join_list.get(i).setfirst_left();
            }
            ProjectOperator p = new ProjectOperator(join_list.get(join_list.size()-1), head , new_body);
            rootOperator = p;
            result = rootOperator.dump();
        }
        try {
            store_file(outputFile, result);
        }catch (IOException e) {
            System.out.println("wrong when writing file");
        }
    }

    /**
     * judge whether the string is positive int;
     * @param s is a String
     * @return  if is positive int, return true, else return false;
     */
    public static boolean isPositive(String s) {
        for(int i = 0; i < s.length(); i++) {
            if(!Character.isDigit(s.charAt(i)))
                return false;
        }
        return true;
    }
    /**
     * Example method for getting started with the parser.
     * Reads CQ from a file and prints it to screen, then extracts Head and Body
     * from the query and prints them to screen.
     */

    public static void parsingExample(String filename) {
        try {
//            Query query = QueryParser.parse(filename);
            Query query = QueryParser.parse(Paths.get(filename));
//            Query query = QueryParser.parse("Q(x, y) :- R(x, z), S(y, z, w), z < w");
//            Query query = QueryParser.parse("Q(x, w) :- R(x, 'z'), S(4, z, w), 4 < 'test string' ");

            System.out.println("Entire query: " + query);
            RelationalAtom head = query.getHead();
            System.out.println("Head: " + head);
            List<Atom> body = query.getBody();
            System.out.println("Body: " + body);
        }
        catch (Exception e)
        {
            System.err.println("Exception occurred during parsing");
            e.printStackTrace();
        }
    }

}


abstract class Operator {

    public abstract String getNextTuple();

    public abstract void reset();

//test dump(), dump all the output in the screen
//    abstract void dump();
//    public void dump() {
//        String cur_tuple = getNextTuple();
//        do{
//            System.out.println(cur_tuple);
//            cur_tuple = getNextTuple();
//        }
//        while(cur_tuple!=null);
//    }

    public List<String> dump() {
        String cur_tuple = getNextTuple();
        List<String> result = new ArrayList<>();
        do{
            System.out.println("cur_tuple:" + cur_tuple);
            result.add(cur_tuple);
            cur_tuple = getNextTuple();

        }
        while(cur_tuple!=null);
        return  result;
    }

}

class ScanOperator extends Operator{
    private String file_name;
    private String file_path;
    private FileReader f_reader;
    private BufferedReader b_reader;
    private List<String> schema_list;

    public ScanOperator(String file_name) {
        this.file_name = file_name;
        //file_name
        System.out.println(file_name);
        file_path = DatabaseCatalog.getInstance().get_filepath(file_name);
        HashMap<String, List<String>> file_type_map = new HashMap<>();

        try {
        f_reader = new FileReader(file_path);
        b_reader = new BufferedReader(f_reader);
        file_type_map  = DatabaseCatalog.getInstance().read_schema();
        schema_list = file_type_map.get(file_name);
        //schema_list;
//        for(int i = 0; i < schema_list.size(); i++)
//            System.out.println(schema_list.get(i));
        }
        catch (Exception e) {
            System.err.println("Failed to open file");
            e.printStackTrace();
        }
    }

    @Override
    public String getNextTuple() {
        try {
            String next_tuple = b_reader.readLine();
            if (next_tuple != null) {
//                return next_tuple;
                String[] parsed_tuple_list = next_tuple.split(",");
                String parsed_tuple = "";
//                System.out.println(next_tuple);
                for (int i = 0; i < parsed_tuple_list.length; i++) {
                    parsed_tuple += parsed_tuple_list[i].trim();
                    if(i!= parsed_tuple_list.length-1){
                        parsed_tuple += " ";

                    }
                }
//                    System.out.println(parsed_tuple);
                    return parsed_tuple;

            }
            else {
                return null;
            }
        }

            catch (Exception e) {
            System.err.println("Failed to open file");
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void reset() {
        try{
            f_reader = new FileReader(file_path);
            b_reader = new BufferedReader(f_reader);
        }
        catch (Exception e)
        {
            System.err.println("Failed to open file");
            e.printStackTrace();
        }
    }

//    public List<String> readfile(String path) throws IOException {
//        File inputFile_name = new File(path);
//        InputStreamReader reader = new InputStreamReader(
//                new FileInputStream(inputFile_name));
//        BufferedReader br = new BufferedReader(reader);
//        String line = "";
//        List file_data = new ArrayList<>();
//            while ((line = br.readLine()) != null) {
//                System.out.println(line);
//                file_data.add(line);
//        }
//        return  file_data;
//    }
}

class SelectOperator extends Operator {
    private List<String> condition_list;
//    private List<String> schema;
    Operator child;

    public SelectOperator(Operator child, List<String> condition_list) {
        this.child = child;
        this.condition_list= condition_list;
//        this.schema = schema;
    }
    @Override
    public String getNextTuple() {
        String next_tuple;
        while((next_tuple = child.getNextTuple())!=null) {
            if(evaluate_select_conditions.judge(next_tuple, condition_list)) {
//                System.out.println(next_tuple);
                return next_tuple;
            }
        }
        return null;
    }
    @Override
    public void reset() {
        child.reset();
    }

}

class evaluate_select_conditions {
//    private List<String> condition_list;
//    private String next_tuple;
    public static boolean judge(String next_tuple, List<String> condition_list) {

        List<String> relation = new ArrayList<>();
        String [] tuple_string;
        tuple_string  = next_tuple.split(" ");
        for(int i = 0; i < condition_list.size(); i++) {
            String cur_atom = condition_list.get(i);
            if(isExpression(cur_atom)) {
                String []express_Atom;
                String former;
                String latter;
//                System.out.println("tuple: "+next_tuple);
//                String [] tuple_string;
//                tuple_string  = next_tuple.split(" ");
//                System.out.println("tuple list: "+tuple_string[2]);
                if(cur_atom.indexOf(" = ") > 0) {
                    express_Atom = cur_atom.split(" = ");
                    former = express_Atom[0];
                    latter = express_Atom[1];
//                    System.out.println("former: "+ former);
//                    System.out.println("latter: "+ latter);
                    int position_f = relation.indexOf(former);
//                    System.out.println("postion: "+position_f);
//                    System.out.println("atom: "+tuple_string[position_f]);
                    if(isPositive(latter)) {
//                        System.out.println("positive: ");
                        if(Integer.valueOf(tuple_string[position_f])!=Integer.valueOf(latter)){
//                            System.out.println(Integer.valueOf(tuple_string[position_f]));
//                            System.out.println(Integer.valueOf(latter));
                            return false;
                        }
                    }
                    else if (latter.charAt(0)=='\'') {
                        if(!tuple_string[position_f].equals(latter)) {
//                            System.out.println(tuple_string[position_f]);
//                            System.out.println(latter);
                            return false;
                        }
                    }
                    else {
                        int position_l = relation.indexOf(latter);
//                        System.out.println("length: " +  tuple_string.length);
//                        System.out.println(tuple_string[0]+" "+tuple_string[1]+" "+tuple_string[2]);
//                        System.out.println(position_l);
                        if(!tuple_string[position_f].equals(tuple_string[position_l])) {
                            return false;
                        }
                    }
                }
                else if(cur_atom.indexOf(" != ") > 0) {
                    express_Atom = cur_atom.split(" != ");
                    former = express_Atom[0];
                    latter = express_Atom[1];
                    int position_f = relation.indexOf(former);
                    if(isPositive(latter)) {
                        if(Integer.valueOf(tuple_string[position_f])==Integer.valueOf(latter))
                            return false;
                    }
                    else if (latter.charAt(0)=='\'') {
                        if(tuple_string[position_f].equals(latter)) {
                            return false;
                        }
                    }
                    else {
                        int position_l = relation.indexOf(latter);
                        if(!(tuple_string[position_f]!=tuple_string[position_l])) {
                            return false;
                        }
                    }
                }
                else if(cur_atom.indexOf(" >= ") > 0) {
                    express_Atom = cur_atom.split(" >= ");
                    former = express_Atom[0];
                    latter = express_Atom[1];
                    int position_f = relation.indexOf(former);
                    if(isPositive(latter)) {
                        if(Integer.valueOf(tuple_string[position_f])<Integer.valueOf(latter))
                            return false;
                    }
                }
                else if(cur_atom.indexOf(" <= ") > 0) {
                    express_Atom = cur_atom.split(" <= ");
                    former = express_Atom[0];
                    latter = express_Atom[1];
                    int position_f = relation.indexOf(former);
                    if(isPositive(latter)) {
                        if(Integer.valueOf(tuple_string[position_f])>Integer.valueOf(latter))
                            return false;
                    }
                }
                else if(cur_atom.indexOf(" > ") > 0) {
                    express_Atom = cur_atom.split(" > ");
                    former = express_Atom[0];
                    latter = express_Atom[1];
                    int position_f = relation.indexOf(former);
                    if(isPositive(latter)) {
                        if(Integer.valueOf(tuple_string[position_f])<=Integer.valueOf(latter))
                            return false;
                    }
                }
                else if(cur_atom.indexOf(" < ") > 0) {
                    express_Atom = cur_atom.split(" < ");
                    former = express_Atom[0];
                    latter = express_Atom[1];
                    int position_f = relation.indexOf(former);
                    if (isPositive(latter)) {
                        if (Integer.valueOf(tuple_string[position_f]) >= Integer.valueOf(latter))
                            return false;
                    }
                }

            }
            else {
                String temp1 = "";
                for(int k = 0; k < cur_atom.length(); k++) {
                    char cur = cur_atom.charAt(k);
                    if (cur != '(' && cur != ')' && cur != ',') {
                        temp1 += cur;
                    } else {
                        if(cur!='(') {
                            if(!relation.contains(temp1))
                                relation.add(temp1);
                            if(isPositive(temp1)) {
                                int position_int = relation.indexOf(temp1);
                                if(!tuple_string[position_int].equals(temp1))
                                    return false;
                            }
                        }
                        temp1 = "";
                    }
                    if (cur == ',') {
                        k++;
                    }
                }
            }
        }

        return true;
    }
    public static boolean isExpression(String body_atom) {
        if(body_atom.indexOf("(") >= 0)
            return false;
        else
            return true;
    }



    public static boolean isPositive(String s) {
        for(int i = 0; i < s.length(); i++) {
            if(!Character.isDigit(s.charAt(i)))
                return false;
        }
        return true;
    }
}

class ProjectOperator extends Operator {
    private List<String> head_list;
    private List<String> body_list;
    Operator child;

    public ProjectOperator(Operator child, List<String> head_list, List<String> body_list) {
        this.child = child;
        this.head_list = head_list;
        this.body_list = body_list;
    }

    @Override
    public String getNextTuple() {
        String next_tuple;
        while ((next_tuple = child.getNextTuple()) != null) {
//                System.out.println("old:" +next_tuple);
                next_tuple = project_tuple(next_tuple,head_list,body_list);
//                System.out.println("new:" +next_tuple);
                return next_tuple;
        }
        return null;
    }

    @Override
    public void reset() {
        child.reset();
    }

    public static String project_tuple(String next_tuple, List<String> head, List<String> body) {
        String[] tuple_list = next_tuple.split(" ");
        List<String> relation = new ArrayList<>();
        String temp1 = "";
        for (int i = 0; i < body.size(); i++) {
            String cur_atom = body.get(i);
            if (!evaluate_select_conditions.isExpression(cur_atom)) {
                for (int k = 0; k < cur_atom.length(); k++) {
                    char cur = cur_atom.charAt(k);
                    if (cur != '(' && cur != ')' && cur != ',') {
                        temp1 += cur;
                    } else {
                        if (cur != '(') {
                            if (!relation.contains(temp1))
                                relation.add(temp1);
                        }
                        temp1 = "";
                    }
                    if (cur == ',') {
                        k++;
                    }
                }
            }

        }
        String project_tuple = "";
        for(int i = 0; i < head.size(); i++) {
            int position = relation.indexOf(head.get(i));
            project_tuple += tuple_list[position];
            if(i!= head.size()-1)
                project_tuple += " ";
        }
        return project_tuple;
    }
}


class JoinOperator extends Operator {
    Operator left_child;
    Operator right_child;
    List<String> body_list;
    String left_tuple;
    public JoinOperator(Operator left_child, Operator right_child, List<String> body_list) {
        this.left_child = left_child;
        this.right_child = right_child;
        this.body_list = body_list;
    }

    public void setfirst_left() {
        this.left_tuple = left_child.getNextTuple();
        System.out.println("init: ");
    }

    @Override
    public String getNextTuple() {
        String right_tuple;
        while(left_tuple!=null) {
//            System.out.println("left: " +left_tuple);
            while ((right_tuple = right_child.getNextTuple()) != null) {
//                System.out.println("right: " +right_tuple);
                String new_tuple = join_process(body_list, left_tuple ,right_tuple);
//                System.out.println("new:" + new_tuple);
                if(new_tuple!=null) {
//                    System.out.println(new_tuple);
                    return new_tuple;
                }
            }
            right_child.reset();
            left_tuple = left_child.getNextTuple();
        }
        return null;
    }
    @Override
    public void reset() {
        left_child.reset();
        right_child.reset();
    }
    public static String join_process(List<String> body, String left, String right) {
        List<String> relation = new ArrayList<>();
        String [] left_l = left.split(" ");
        String [] right_l = right.split(" ");
        String new_tuple = "";
        for(int i = 0; i < body.size(); i++) {
            String cur_atom = body.get(i);
            String [] express_Atom;
            String former;
            String latter;
            if(evaluate_select_conditions.isExpression(cur_atom)) {
                String[] new_l = new_tuple.split(" ");
//                System.out.println("new:" + new_tuple);
                if (cur_atom.indexOf(" = ") > 0) {
                    express_Atom = cur_atom.split(" = ");
                    former = express_Atom[0];
                    latter = express_Atom[1];
                    int position_f = relation.indexOf(former);
                    int position_l = relation.indexOf(latter);
//                    System.out.println(position_f);
//                    System.out.println(position_l);
                    if (!new_l[position_f].equals(new_l[position_l])) {
                        return null;
                    }
                } else if (cur_atom.indexOf(" != ") > 0) {
                    express_Atom = cur_atom.split(" != ");
                    former = express_Atom[0];
                    latter = express_Atom[1];
                    int position_f = relation.indexOf(former);
                    int position_l = relation.indexOf(latter);
                    if (!new_l[position_f].equals(new_l[position_l])) {
                        return null;
                    }
                } else if (cur_atom.indexOf(" >= ") > 0) {
                    express_Atom = cur_atom.split(" >= ");
                    former = express_Atom[0];
                    latter = express_Atom[1];
                    int position_f = relation.indexOf(former);
                    int position_l = relation.indexOf(latter);
                    if (Integer.valueOf(new_l[position_f]) < Integer.valueOf(new_l[position_l]))
                        return null;

                } else if (cur_atom.indexOf(" <= ") > 0) {
                    express_Atom = cur_atom.split(" <= ");
                    former = express_Atom[0];
                    latter = express_Atom[1];
                    int position_f = relation.indexOf(former);
                    int position_l = relation.indexOf(latter);
                    if (Integer.valueOf(new_l[position_f]) > Integer.valueOf(new_l[position_l]))
                        return null;
                } else if (cur_atom.indexOf(" > ") > 0) {
                    express_Atom = cur_atom.split(" > ");
                    former = express_Atom[0];
                    latter = express_Atom[1];
                    int position_f = relation.indexOf(former);
                    int position_l = relation.indexOf(latter);
                    if (Integer.valueOf(new_l[position_f]) <= Integer.valueOf(new_l[position_l]))
                        return null;
                }
                else if (cur_atom.indexOf(" < ") > 0) {
                    express_Atom = cur_atom.split(" < ");
                    former = express_Atom[0];
                    latter = express_Atom[1];
                    int position_f = relation.indexOf(former);
                    int position_l = relation.indexOf(latter);
                    if (Integer.valueOf(new_l[position_f]) >= Integer.valueOf(new_l[position_l]))
                        return null;
                }
            }
            else {
                String temp1 = "";
                int count = 0;
                for(int k = 0; k < cur_atom.length(); k++) {
                    char cur = cur_atom.charAt(k);
                    if (cur != '(' && cur != ')' && cur != ',') {
                        temp1 += cur;
                    }
                    else {
                        if(cur!='(') {
                            if(!relation.contains(temp1)) {
                                relation.add(temp1);
                                if(i==0) {
                                    new_tuple += left_l[count];
                                    new_tuple += " ";
                                }
                                if(i==1) {
                                    new_tuple += right_l[count];
                                    new_tuple += " ";
                                }
                            }
                            else {
                                int position_l = relation.indexOf(temp1);
                                int position_r = count;
                                if(!left_l[position_l].equals(right_l[position_r]))
                                    return null;
                            }
                            count++;
                        }
                        temp1 = "";
//                        count++;
//                        System.out.println(count);

                    }
                    if (cur == ',') {
                        k++;
                    }
                }
            }
        }
        return new_tuple;
    }
    public static List<String> new_body(List<String> body) {
        List<String> new_list = new ArrayList<>();
        List<String> relation = new ArrayList<>();
        String news = "";
        for(int i = 0; i < 2; i++) {
            String cur_atom = body.get(i);
            String temp1 = "M(";
            for (int k = 0; k < cur_atom.length(); k++) {
                char cur = cur_atom.charAt(k);
                if (cur != '(' && cur != ')' && cur != ',') {
                    temp1 += cur;
                } else {
                    if (cur != '(') {
                        if (!relation.contains(temp1)) {
                            relation.add(temp1);
                            news += temp1;
                            if(i==1&&k==cur_atom.length()-1){
                                news += ")";
                            }
                            else {
                                news += ", ";
                            }
                        }
                    }
                    temp1 = "";

                }
                if (cur == ',') {
                    k++;
                }
            }
        }
        new_list.add(news);
        return new_list;
    }
}

class SumOperator extends Operator{
    Operator child;
    private List<String> sum_atom;
    private Integer sum_index;
    public SumOperator(Operator child, Integer sum_index) {
        this.child = child;
        this.sum_index = sum_index;
        List<String> temp = new ArrayList<>();
        temp = child.dump();
//        List<String> distinct = new ArrayList<>();
//        for(int i = 0; i < temp.size(); i++) {
//            if(distinct.contains(temp.get(i)))
//                continue;
//            distinct.add(temp.get(i));
//        }
        this.sum_atom = temp;
    }
    @Override
    public String getNextTuple() {
        while(!sum_atom.isEmpty()) {
            String next_tuple = sum(sum_atom, sum_index);
            System.out.println("next_tuple: " + next_tuple);
            if(getNewList(sum_atom, sum_index)!=null)
                sum_atom = getNewList(sum_atom, sum_index);
            else
                sum_atom.clear();
            if(next_tuple!=null);
                return next_tuple;
        }
        return null;
    }
    @Override
    public void reset() {
        child.reset();
    }
    public static String sum(List<String> atom, Integer index) {
        int sum = 0;
        System.out.println("size:" + atom.get(0).length());
//        System.out.println("size:" + atom);
        if(atom.get(0).length()==1) {
            for(int i = 0; i < atom.size(); i++) {
                sum += Integer.valueOf(atom.get(i));
            }
            return String.valueOf(sum);
        }
        else {
            String cur = atom.get(0);
            String [] cur_list = cur.split(" ");
            sum += Integer.valueOf(cur_list[index]);
            for(int i = 1; i < atom.size(); i++){
                String temp = atom.get(i);
                String [] temp_list = temp.split(" ");
                boolean can_sum = true;
                for(int j = 0; j< temp_list.length; j++) {
                    if(j!=index) {
                        if(!temp_list[j].equals(cur_list[j])) {
                            can_sum = false;
                            break;
                        }
                    }
                }
                if(can_sum)
                    sum += Integer.valueOf(temp_list[index]);
            }
            String new_output = "";
            for(int i = 0; i < cur_list.length; i++) {
                if(i==index) {
                    new_output += String.valueOf(sum);
                }
                else
                    new_output += cur_list[i];
                if(i!= cur_list.length-1) {
                    new_output += " ";
                }

            }
            return new_output;
        }
    }
    public static List<String> getNewList(List<String> atom, Integer index) {
        if(atom.get(0).length()==1) {
            return null;
        }
        else {
            String cur = atom.get(0);
            String[] cur_list = cur.split(" ");
            List<String> new_list = new ArrayList<>();
            for (int i = 1; i < atom.size(); i++) {
                String temp = atom.get(i);
                String[] temp_list = temp.split(" ");
                boolean can_sum = true;
                for (int j = 0; j < temp_list.length; j++) {
                    if (j != index) {
                        if (!temp_list[j].equals(cur_list[j])) {
                            can_sum = false;
                            break;
                        }
                    }
                }
                if (!can_sum)
                    new_list.add(temp);
            }
            return new_list;
        }
    }
}

class AvgOperator extends Operator{
    Operator child;
    private List<String> avg_atom;
    private Integer avg_atom_index;
    public AvgOperator(Operator child, Integer avg_atom_index) {
        this.child = child;
        this.avg_atom_index = avg_atom_index;
        List<String> temp = new ArrayList<>();
        temp = child.dump();
        this.avg_atom = temp;
    }
    @Override
    public String getNextTuple() {
        while(!avg_atom.isEmpty()) {
            String next_tuple = sum(avg_atom, avg_atom_index);
            if(getAvgList(avg_atom, avg_atom_index)!=null)
                avg_atom = getAvgList(avg_atom, avg_atom_index);
            else
                avg_atom.clear();
            if(next_tuple!=null) {
                System.out.println("next_tuple: " +next_tuple);
                return next_tuple;
            }
        }
        return null;
    }
    @Override
    public void reset() {
        child.reset();
    }
    public static String sum(List<String> atom, Integer index) {
        int sum = 0;
        int count = 0;
        int avg = 0;
        if(atom.get(0).length()==1) {
            for(int i = 0; i < atom.size(); i++) {
                sum += Integer.valueOf(atom.get(i));
                count ++;
            }
            avg = sum/count;
            return String.valueOf(avg);
        }
        else {
            String cur = atom.get(0);
            String [] cur_list = cur.split(" ");
            sum += Integer.valueOf(cur_list[index]);
            for(int i = 1; i < atom.size(); i++){
                String temp = atom.get(i);
                String [] temp_list = temp.split(" ");
                boolean can_sum = true;
                for(int j = 0; j< temp_list.length; j++) {
                    if(j!=index) {
                        if(!temp_list[j].equals(cur_list[j])) {
                            can_sum = false;
                            break;
                        }
                    }
                }
                if(can_sum) {
                    sum += Integer.valueOf(temp_list[index]);
                    count++;
                }
            }
            String new_output = "";
            for(int i = 0; i < cur_list.length; i++) {
                if(i==index) {
                    avg = sum/count;
                    new_output += String.valueOf(avg);
                }
                else
                    new_output += cur_list[i];
                if(i!= cur_list.length-1) {
                    new_output += " ";
                }

            }
            return new_output;
        }
    }
    public static List<String> getAvgList(List<String> atom, Integer index) {
        if(atom.get(0).length()==1) {
            return null;
        }
        else {
            String cur = atom.get(0);
            String[] cur_list = cur.split(" ");
            List<String> new_list = new ArrayList<>();
            for (int i = 1; i < atom.size(); i++) {
                String temp = atom.get(i);
                String[] temp_list = temp.split(" ");
                boolean can_sum = true;
                for (int j = 0; j < temp_list.length; j++) {
                    if (j != index) {
                        if (!temp_list[j].equals(cur_list[j])) {
                            can_sum = false;
                            break;
                        }
                    }
                }
                if (!can_sum)
                    new_list.add(temp);
            }
            return new_list;
        }
    }}

class DatabaseCatalog {
    private String databaseDir;
    private String schemaDir;
    private  String fileDir;
    private HashMap<String, List<String>> file_type_map;
    private List<String> filename_list;
//    private  List<String> file_data;
    private  static DatabaseCatalog instance = new DatabaseCatalog();
    private DatabaseCatalog () {}
    public static  DatabaseCatalog getInstance() {
        return instance;
    }
    public void setDatabaseDir(String databaseDir) {
        this.databaseDir = databaseDir;
    }

    public String get_schema_path(){
        schemaDir = databaseDir + File.separator + "schema.txt";
        return schemaDir;
    }

    public HashMap<String, List<String>> read_schema() throws IOException {
        file_type_map = new HashMap<>();
        filename_list = new ArrayList<>();
        schemaDir = get_schema_path();
//        System.out.println("output: " + schemaDir);
        File inputFile_name = new File(schemaDir);
        InputStreamReader reader = new InputStreamReader(
                new FileInputStream(inputFile_name));
        BufferedReader br = new BufferedReader(reader);
        String line= "";
        String[] line_list;
        while((line = br.readLine())!=null) {
            line_list = line.split(" ");
            filename_list.add(line_list[0]);
            List<String> types = new ArrayList<>();
            int i = 0;
            while(i!=(line_list.length-1)){
                if(i != 0) {
                    types.add(line_list[i]);
                }
                i++;
            }
            file_type_map.put(line_list[0],types);
        }
        return file_type_map;
    }

    public  String get_filepath(String filename) {
        fileDir = databaseDir + File.separator + "files" + File.separator + filename + ".csv";
        return fileDir;
    }

//    public  List<String> get_filedata(String filename) throws IOException {
//        fileDir = get_filepath(filename);
//        File inputFile_name = new File(fileDir);
//        InputStreamReader reader = new InputStreamReader(
//                new FileInputStream(inputFile_name));
//        BufferedReader br = new BufferedReader(reader);
//        String line = "";
//        file_data = new ArrayList<>();
//        while ((line = br.readLine()) != null) {
//            System.out.println(line);
//            file_data.add(line);
//            }
//        return file_data;
//    }
}

