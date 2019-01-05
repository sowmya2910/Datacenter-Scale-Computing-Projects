import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WordMapper extends MapReduceBase 
		implements  Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter r)
			throws IOException {
		
		List<String> dictionaryList = new ArrayList<String>();
		
		dictionaryList.add("a");dictionaryList.add("about");dictionaryList.add("above");dictionaryList.add("after");	
		dictionaryList.add("again");dictionaryList.add("against");dictionaryList.add("all");dictionaryList.add("am");
		dictionaryList.add("an");dictionaryList.add("and");dictionaryList.add("any");dictionaryList.add("are");
		dictionaryList.add("arent");dictionaryList.add("as");dictionaryList.add("at");dictionaryList.add("be");
		dictionaryList.add("because");dictionaryList.add("been");dictionaryList.add("before");dictionaryList.add("being");
		dictionaryList.add("below");dictionaryList.add("between");dictionaryList.add("both");dictionaryList.add("but");
		dictionaryList.add("by");dictionaryList.add("cant");dictionaryList.add("cannot");dictionaryList.add("could");
		dictionaryList.add("couldnt");dictionaryList.add("did");dictionaryList.add("didnt");dictionaryList.add("do");
		dictionaryList.add("does");dictionaryList.add("doesnt");dictionaryList.add("doing");dictionaryList.add("dont");
		dictionaryList.add("down");dictionaryList.add("during");dictionaryList.add("each");dictionaryList.add("few");
		dictionaryList.add("for");dictionaryList.add("from");dictionaryList.add("further");dictionaryList.add("had");
		dictionaryList.add("hadnt");dictionaryList.add("has");dictionaryList.add("hasnt");dictionaryList.add("have");		
		dictionaryList.add("havent");dictionaryList.add("having");dictionaryList.add("he");dictionaryList.add("hed");
		dictionaryList.add("hell");dictionaryList.add("hes");dictionaryList.add("her");dictionaryList.add("here");
		dictionaryList.add("heres");dictionaryList.add("hers");dictionaryList.add("herself");dictionaryList.add("him");
		dictionaryList.add("himself");dictionaryList.add("his");dictionaryList.add("how");dictionaryList.add("hows");
		dictionaryList.add("i");dictionaryList.add("id");dictionaryList.add("ill");dictionaryList.add("im");
		dictionaryList.add("ive");dictionaryList.add("if");dictionaryList.add("in");dictionaryList.add("into");		
		dictionaryList.add("is");dictionaryList.add("isnt");dictionaryList.add("it");dictionaryList.add("its");
		dictionaryList.add("itself");dictionaryList.add("lets");dictionaryList.add("me");dictionaryList.add("more");
		dictionaryList.add("most");dictionaryList.add("mustnt");dictionaryList.add("my");dictionaryList.add("myself");
		dictionaryList.add("no");dictionaryList.add("nor");dictionaryList.add("not");dictionaryList.add("of");
		dictionaryList.add("off");dictionaryList.add("on");dictionaryList.add("once");dictionaryList.add("only");
		dictionaryList.add("or");dictionaryList.add("other");dictionaryList.add("ought");dictionaryList.add("ours");
		dictionaryList.add("ourselves");dictionaryList.add("out");dictionaryList.add("over");dictionaryList.add("own");
		dictionaryList.add("same");dictionaryList.add("shant");dictionaryList.add("she");dictionaryList.add("shed");
		dictionaryList.add("shell");dictionaryList.add("shes");dictionaryList.add("should");dictionaryList.add("shouldnt");
		dictionaryList.add("so");dictionaryList.add("some");dictionaryList.add("such");dictionaryList.add("than");
		dictionaryList.add("that");dictionaryList.add("thats");dictionaryList.add("the");dictionaryList.add("their");
		dictionaryList.add("theirs");dictionaryList.add("them");dictionaryList.add("themselves");dictionaryList.add("then");
		dictionaryList.add("there");dictionaryList.add("theres");dictionaryList.add("these");dictionaryList.add("they");
		dictionaryList.add("theyd");dictionaryList.add("theyll");dictionaryList.add("theyre");dictionaryList.add("theyve");
		dictionaryList.add("this");dictionaryList.add("those");dictionaryList.add("through");dictionaryList.add("to");
		dictionaryList.add("too");dictionaryList.add("under");dictionaryList.add("until");dictionaryList.add("up");
		dictionaryList.add("very");dictionaryList.add("was");dictionaryList.add("wasnt");dictionaryList.add("we");
		dictionaryList.add("wed");dictionaryList.add("well");dictionaryList.add("were");dictionaryList.add("weve");
		dictionaryList.add("werent");dictionaryList.add("what");dictionaryList.add("whats");dictionaryList.add("whens");
		dictionaryList.add("where");dictionaryList.add("wheres");dictionaryList.add("which");dictionaryList.add("while");
		dictionaryList.add("who");dictionaryList.add("whos");dictionaryList.add("whom");dictionaryList.add("why");
		dictionaryList.add("whys");dictionaryList.add("with");dictionaryList.add("wont");dictionaryList.add("would");
		dictionaryList.add("wouldnt");dictionaryList.add("you");dictionaryList.add("youd");dictionaryList.add("youll");
		dictionaryList.add("youre");dictionaryList.add("youve");dictionaryList.add("your");dictionaryList.add("yours");
		dictionaryList.add("yourself");dictionaryList.add("yourselves");dictionaryList.add("our");
		dictionaryList.add("when");
		
		
		
		
		
		String s = value.toString();
		s = s.replaceAll("[^a-zA-Z\\s]", "");
		s = s.toLowerCase();
		for(String word:s.split(" ")) {
			int flag_not_stopword = 1;
			if(word.length()>0) {				
				if(dictionaryList.contains(word)){
					flag_not_stopword = 0;
			    }
				if(flag_not_stopword==1) {
					output.collect(new Text(word), new IntWritable(1));
				}
			}
		}
	}

}
