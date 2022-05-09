package ed.inf.adbs.minibase;

import ed.inf.adbs.minibase.base.Atom;
import ed.inf.adbs.minibase.base.Query;
import ed.inf.adbs.minibase.base.RelationalAtom;
import ed.inf.adbs.minibase.parser.QueryParser;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * Minimization of conjunctive queries
 *
 */
public class CQMinimizer {

    public static void main(String[] args) throws IOException {
//        System.out.println(args.length);
//        if (args.length != 2) {
//            System.err.println("Usage: CQMinimizer input_file output_file");
//            return;
//        }
//
//        String inputFile = args[0];
//        String outputFile = args[1];
//
//        minimizeCQ(inputFile, outputFile);
////        System.out.println(outputFile);
//        parsingExample(inputFile);

        /**
         * IDE test example
         */
        String a = "C:\\Users\\cesar\\Desktop\\ADBS\\Minibase\\data\\minimization\\input\\query3.txt";
        String b = "C:\\Users\\cesar\\Desktop\\ADBS\\Minibase\\data\\minimization\\output\\query3.txt";
        System.out.println(a);
        parsingExample(a);
//        System.out.println("mini: ");
        minimizeCQ(a, b);
    }

    /**
     * CQ minimization procedure
     *
     * Assume the body of the query from inputFile has no comparison atoms
     * but could potentially have constants in its relational atoms.
     *
     */
    public static void minimizeCQ(String inputFile, String outputFile) throws IOException {

//        // TODO: add your implementation
        // get the parsed_query
        String input = read_file(inputFile);
        Query parsed_query = QueryParser.parse(input);
        RelationalAtom head = parsed_query.getHead();
        //create two lists to store head and body
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
        }
        /**
         * using nested loop to compare each atom in the body
         *
         */
        //outer loop
        for(int i = 0; i < body.size(); i++) {
            if(body.get(i) == null)
                continue;
            //store current outer loop atom with String first_q
            String first_q = String.valueOf(body.get(i));
            List<String> first_s = new ArrayList<>();
            String temp1 = "";
            /**
             * parse the outer atom and store it in String List
             * e.g. R(x, y) -> List[R, x, y]
             */
            for (int n = 0; n < first_q.length(); n++) {
                char cur = first_q.charAt(n);
                if (cur != '(' && cur != ')' && cur != ',') {
                    temp1 += cur;
                } else {
                    first_s.add(temp1);
                    temp1 = "";
                }
                if (cur == ',') {
                    n++;
                }
            }
            //inner loop
            for (int j = i+1; j < body.size(); j++) {
                    if (body.get(j) == null)
                        continue;
                    //store current inner loop atom with String second_q
                    String second_q = String.valueOf(body.get(j));
                    String temp2 = "";
                    List<String> second_s = new ArrayList<>();
                    //parse the inner atom
                    for (int m = 0; m < second_q.length(); m++) {
                        char cur = second_q.charAt(m);
                        if (cur != '(' && cur != ')' && cur != ',') {
                            temp2 += cur;
                        } else {
                            second_s.add(temp2);
                            temp2 = "";
                        }
                        if (cur == ',') {
                            m++;
                        }
                    }
                    //if the first element in the two lists is equal, then judge two atom whether
                    //they are homo or not
                    if (first_s.get(0).equals(second_s.get(0))) {
                        body_list = judge_homo(head_list, body_list, first_s, second_s, i, j);
                    } else {
                        continue;
                    }

            }
        }
        /**
         * generate the output String
         */
        //output body
        String output_body = "";
        //create a new list to store the list removing all the empty atom
        List<String> new_list = new ArrayList<>();
        for(int i = 0; i < body_list.size(); i++) {
            if(body_list.get(i)!=null)
                new_list.add(body_list.get(i));
        }
        //link the new_list to finish the output_body
        for(int i = 0; i < new_list.size(); i++) {
            output_body += new_list.get(i);
            if(i != new_list.size()-1)
                output_body += ", ";
        }
        //finish the whole output (head and body)
        String output = String.valueOf(head) + " :- " + output_body;
        store_file(outputFile, output);
    }

    /**
     * read input from inputFile
     * @param path is the input file path
     */
    public static String read_file(String path) throws IOException {
        File inputFile_name = new File(path);
        InputStreamReader reader = new InputStreamReader(
                new FileInputStream(inputFile_name));
        BufferedReader br = new BufferedReader(reader);
        String input_query = br .readLine();
        return input_query;
    }

    /**
     * write the minimized output in the path outputFile;
     * @param path is the out file path
     * @param output is the output CQ query after minimized
     */
    public static void store_file(String path, String output) throws IOException {
        File outputFile_name = new File(path);
        outputFile_name.createNewFile();
        BufferedWriter out = new BufferedWriter(new FileWriter(path));
        out.write(output);
        out.flush();
        out.close();
    }
    /**
     * Judge whether the two atoms of the body are homo or not, if homo, do minimization
     * @params head_list and body_list are the two Lists store the head and the body of the CQ query
     * @params first_s and second_s are the two Lists store the first atom and the second atom
     * @params i is the index of the first atom, j is the index of the second atom;
     * @return CQ query after minimization
     */
    public static List<String> judge_homo(List<String> head_list, List<String> body_list, List<String> first_s, List<String> second_s, int i, int j) {
        //create two lists to store the rank of each element in every atom
        List<Integer> rank_f = new ArrayList<>();
        List<Integer> rank_s = new ArrayList<>();
        //create two booleans to mark the relationship between first atom and the second
        boolean f_homo_s = true;
        boolean s_homo_f = true;
        //because the first element of the list (e.g. List[R, x, y]) is not the inner element
        //this loop starts from 1 not 0
        for (int n = 1; n < first_s.size(); n++) {
            String cur = first_s.get(n);
            //if the element is in the head or the element is constant
            //mark the rank as 2
            if (head_list.contains(cur) || cur.charAt(0) == '\'' || isPositive(cur))
                rank_f.add(2);
            //otherwise, mark the rank as 1
            else
                rank_f.add(1);
        }
        //do the same process to the second atom
        for (int m = 1; m < second_s.size(); m++) {
            String cur = second_s.get(m);
            if (head_list.contains(cur) || cur.charAt(0) == '\'' || isPositive(cur))
                rank_s.add(2);
            else
                rank_s.add(1);
        }
        //compare two rank lists to find out the relationship
        for (int k = 0; k < rank_f.size(); k++) {
            if (rank_f.get(k) > rank_s.get(k))
                f_homo_s = false;
            if (rank_f.get(k) < rank_s.get(k))
                s_homo_f = false;
            if (rank_f.get(k) == rank_s.get(k) && rank_f.get(k) != 1 && !first_s.get(k + 1).equals(second_s.get(k + 1))) {
                f_homo_s = false;
                s_homo_f = false;
            }
        }
        //create a map to store the value before mini as key, after mini as value
        Map<String, String> changed = new HashMap<>();
        //a flag to judge whether this mini is legal
        boolean change_tag = true;
        //if they are each other's homo, check whether the map relation of them has a circle
        //if having, keep both, if not, keep one of them randomly (in this code is the first)
        if (f_homo_s && s_homo_f) {
            List<String> key = new ArrayList<>();
            List<String> value = new ArrayList<>();
            //check whether a circle exists, if exists, turn change_tag false
            for (int k = 1; k < first_s.size(); k++) {
                if (!first_s.get(k).equals(second_s.get(k))) {
                    key.add(first_s.get(k));
                    value.add(second_s.get(k));
                    if(value.contains(first_s.get(k))||key.contains(second_s.get(k)))
                        change_tag = false;
                }
            }
            if(change_tag)
                body_list.set(j, null);
            return body_list;
        }
        /**
         * if the first is the second's homo, mini the first to the second,
         * then set the second to null to reduce computations.
         */
        if (f_homo_s) {
            for (int k = 1; k < first_s.size(); k++) {
                //store the key and value of the map relation to mini the query in the Map
                if (!first_s.get(k).equals(second_s.get(k))) {
                    changed.put(first_s.get(k), second_s.get(k));
                    //check whether other atoms (except the current first and second)
                    //have the key of the map relation
                    for (int k1 = 0; k1 < body_list.size(); k1++) {
                        String this_k = String.valueOf(body_list.get(k1));
                        if(k1!=j&&k1!=i) {
                            List<String> cur_list = new ArrayList<>();
                            String temp = "";
                            for (int n1 = 0; n1 < this_k.length(); n1++) {
                                char cur = this_k.charAt(n1);
                                if (cur != '(' && cur != ')' && cur != ',') {
                                    temp += cur;
                                } else {
                                    cur_list.add(temp);
                                    temp = "";
                                }
                                if (cur == ',') {
                                    n1++;
                                }
                            }
                            //check whether this atom contains constant or the element in the head
                            //if so, this mini process is not legal, then set change_tag to false
                            //make the query remain the same
                            if(cur_list.contains(first_s.get(k))&&!body_list.get(i).equals(body_list.get(k1))) {
                                for (int o = 1; o < cur_list.size(); o++) {
                                    if (head_list.contains(cur_list.get(o)) || isPositive(cur_list.get(o)) || cur_list.get(o).charAt(0) == '\'' )
                                        change_tag = false;
                                }
                            }

                        }
                    }
                }
            }
            if(change_tag) {
                body_list.set(i, body_list.get(j));
                body_list.set(j, null);
            }
        }
        /**
        *if the second is the first's homo, mini the second to the first,
        * then set the second to null to reduce computations.
        */
        if (s_homo_f) {
            for (int k = 1; k < first_s.size(); k++) {
                //store the key and value of the map relation to mini the query in the Map
                if (!first_s.get(k).equals(second_s.get(k))) {
                    changed.put(second_s.get(k), first_s.get(k));
                    for (int k1 = 0; k1 < body_list.size(); k1++) {
                        String this_k = String.valueOf(body_list.get(k1));
                        if(k1!=j&&k1!=i) {
                            List<String> cur_list = new ArrayList<>();
                            String temp = "";
                            for (int n1 = 0; n1 < this_k.length(); n1++) {
                                char cur = this_k.charAt(n1);
                                if (cur != '(' && cur != ')' && cur != ',') {
                                    temp += cur;
                                } else {
                                    cur_list.add(temp);
                                    temp = "";
                                }
                                if (cur == ',') {
                                    n1++;
                                }
                            }
                            //check whether this atom contains constant or the element in the head
                            //if so, this mini process is not legal, then set change_tag to false
                            //make the query remain the same
                            if(cur_list.contains(second_s.get(k))&&!body_list.get(j).equals(body_list.get(k1))) {
                                for (int o = 1; o < cur_list.size(); o++) {
                                    if (head_list.contains(cur_list.get(o)) || isPositive(cur_list.get(o)) || cur_list.get(o).charAt(0) == '\'' )
                                        change_tag = false;
                                }
                            }
                        }
                    }
                }
            }
            if(change_tag) {
                body_list.set(j, null);
            }
        }
            return body_list;
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
//            Query query = QueryParser.parse("Q(x, y) :- R(x, z), S(y, z, w)");
//            Query query = QueryParser.parse("Q() :- R(x, 'z'), S(4, z, w)");
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
