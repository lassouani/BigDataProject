/**
* Squelette minimal d'une application Hadoop
* A exporter dans un jar sans les librairies externes
* A ex�cuter avec la commande ./hadoop jar NOMDUFICHER.jar ARGUMENTS....
*/

package bigdata;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MonApplication {
    public static class MonProg extends Configured implements Tool {
        public int run(String[] args) throws Exception {
            System.out.println("Hello World");
            return 0;
        }
    }
    
    public static class Help extends Configured implements Tool {
        public int run(String[] args) throws Exception {
            System.out.println("Voici la liste des commandes disponibles:");
            System.out.println("$hadoop jar <jarname> MonProg (Affiche un message Hello World)");
            System.out.println("$hadoop jar <jarname> Help (Affiche cette aide)");
            System.out.println("$hadoop jar <jarname> CopyFromLocal <Path Input> <HDFS Path Output> (Permet de transférer un fichier local sur un HDFS)");
            System.out.println("$hadoop jar <jarname> MergeFromLocal <Path Input file 1> [<Path Input file n>]* <HDFS Path Output> (Permet de concaténer plusieurs fichiers locaux sur un HDFS)");
            System.out.println("$hadoop jar <jarname> GenerateWord <NumberSyllabes> <HDFS Path Output> (Permet de génerer un mot aléatoire avec un certains nombre de syllabes. Ce fichier sera transférer sur le HDFS)");
            return 0;
        }
    }
    
    public static class CopyFromLocal extends Configured implements Tool {
        public int run(String[] args) throws Exception {
        	
        	if(args.length != 2) {
        		System.out.println("Cette fonction exige deux arguments, plus d'info avec la commande Help");
        		return 1;
        	}
        	
            String localInputPath = args[0];
            URI uri = new URI(args[1]);
            uri = uri.normalize();
            Configuration conf = getConf();
            FileSystem fs = FileSystem.get(uri, conf, "hadoop");
            Path outputPath = new Path(uri.getPath());
            OutputStream os = fs.create(outputPath);
            InputStream is = new BufferedInputStream(new FileInputStream(localInputPath));
            IOUtils.copyBytes(is, os, conf);
            os.close();
            is.close();
            return 0;
        }
    }
    
    public static class MergeFromLocal extends Configured implements Tool {
        public int run(String[] args) throws Exception {
        	
        	if(args.length < 2) {
        		System.out.println("Cette fonction exige au minimum deux arguments, plus d'info avec la commande Help");
        		return 1;
        	}
        	
            URI uri = new URI(args[args.length - 1]);
            uri = uri.normalize();
            Configuration conf = getConf();
            FileSystem fs = FileSystem.get(uri, conf, "hadoop");
            Path outputPath = new Path(uri.getPath());
            OutputStream os = fs.create(outputPath);
            for(int i=0; i < args.length - 1; ++i) {
                InputStream is = new BufferedInputStream(new FileInputStream(args[1]));
                IOUtils.copyBytes(is, os, conf, false);
                is.close();
            }
            os.close();
            return 0;
        }
    }
    
    /* GenerateWord 10 hdfs... */
    public static class GenerateWord extends Configured implements Tool {
        public int run(String[] args) throws Exception {
        	
        	if(args.length != 2) {
        		System.out.println("Cette fonction exige deux arguments, plus d'info avec la commande Help");
        		return 1;
        	}
        	
            Integer syllabeNumber = Integer.parseInt(args[0]);
            URI uri = new URI(args[1]);
            uri = uri.normalize();
            Configuration conf = getConf();
            FileSystem fs = FileSystem.get(uri, conf, "hadoop");
            Path outputPath = new Path(uri.getPath());
            OutputStream os = fs.create(outputPath);
            List<String> syllabes = new ArrayList<String>(Arrays.asList("ba", "ta", "via", "me", "pot", "clap", "dot", "po", "ma","de", "te"));
            Integer numberSyllabesList = syllabes.size();
            String wordGenerated = "";
            for(int i=0; i<syllabeNumber; i++) {
                wordGenerated += syllabes.get((int)(Math.random()*numberSyllabesList));
            }
            InputStream is = new ByteArrayInputStream(wordGenerated.getBytes(StandardCharsets.UTF_8));
            IOUtils.copyBytes(is, os, conf);
            os.close();
            is.close();
            return 0;
        }
    }
    public static void main( String[] args ) throws Exception {
    	String command = args[0];
    	String[] arguments = Arrays.copyOfRange(args, 1, args.length);
    	String commandsAvailable[] = {"MonProg", "Help", "CopyFromLocal", "MergeFromLocal", "GenerateWord"};
    	int returnCode;
    	if(Arrays.asList(commandsAvailable).contains(command)) {
    		switch(command) {
    			case "MonProg":
    				returnCode = ToolRunner.run(new MonApplication.MonProg(), arguments);
    				break;
    			case "Help":
    				returnCode = ToolRunner.run(new MonApplication.Help(), arguments);
    				break;
    			case "CopyFromLocal":
    				returnCode = ToolRunner.run(new MonApplication.CopyFromLocal(), arguments);
    				break;
    			case "MergeFromLocal":
    				returnCode = ToolRunner.run(new MonApplication.MergeFromLocal(), arguments);
    				break;
    			case "GenerateWord":
    				returnCode = ToolRunner.run(new MonApplication.GenerateWord(), arguments);
    				break;
    				
    			default:
    				returnCode = 1;
    		}
    		
    		/* Tentative d'usage de reflection, mais cela n'a pas fonctionné */
    		/*Tool runCommand = (Tool)Class.forName("MonApplication."+command).newInstance();
    		returnCode = ToolRunner.run(runCommand, arguments);*/
    		
    	}
    	else {
    		System.out.println("Commande non reconnue, utiliser la commande Help pour avoir la liste des commandes ainsi que leurs arguments ");
    		returnCode = 0;
    	}
    	
		System.exit(returnCode);
    }
}
//=====================================================================