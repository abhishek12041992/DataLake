

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import org.apache.hadoop.conf.Configuration;
//Needed to get the hadoop configuration.

import org.apache.hadoop.fs.*;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
//Needed for HDFS file system operation.
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.html.HtmlParser;
import org.apache.tika.parser.microsoft.ooxml.OOXMLParser;
import org.apache.tika.parser.odf.OpenDocumentParser;
import org.apache.tika.parser.pdf.PDFParser;
import org.apache.tika.parser.pkg.PackageParser;
import org.apache.tika.parser.txt.TXTParser;
import org.apache.tika.parser.xml.XMLParser;
import org.apache.tika.sax.BodyContentHandler;

import org.xml.sax.SAXException;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.io.*;
import javax.swing.JTextField;
import java.awt.Button;

import javax.swing.DefaultListModel;
import javax.swing.DropMode;
import javax.swing.JTextPane;
import javax.swing.JList;
import javax.swing.ListSelectionModel;
import javax.swing.text.BadLocationException;
import javax.swing.text.SimpleAttributeSet;

import java.awt.TextField;
import javax.swing.JMenuBar;
import javax.swing.JMenu;
import javax.swing.GroupLayout;
import javax.swing.GroupLayout.Alignment;
import java.awt.Scrollbar;
import javax.swing.JPopupMenu;
import java.awt.Component;
import java.awt.Dimension;

import javax.swing.JTabbedPane;
import javax.swing.JToolBar;


public class Start  {
	
	  static  FileSystem hdfs;
	static  Path homeDir;
	static int l=0;
	static String home_dir;
	  public static void main(String args[]) throws IOException, URISyntaxException, SAXException, TikaException {
		  Configuration conf = new Configuration();
		//  f.setVisible(true);
		    conf.set("fs.default.name","hdfs://localhost:9000");
		hdfs =FileSystem.get(conf);
		  homeDir=hdfs.getHomeDirectory();
		  home_dir=homeDir+"";
		  
		  JFrame.setDefaultLookAndFeelDecorated(true);
		    JDialog.setDefaultLookAndFeelDecorated(true);
		    JFrame frame1 = new JFrame("Data Lake Viewer And Maintainer");
		    frame1.setSize(800,800);
		    JFrame frame = new JFrame("Data Lake Viewer And Maintainer");
		    frame.setSize(800,800);
		    
		    JTextPane textPane = new JTextPane();
		    textPane.setBounds(499, 59, 311, 512);
		    JTextPane tp = new JTextPane();
		     JTextField j=new JTextField();
		     j.setBounds(630, 560, 150, 25);
	        JPanel p = new JPanel();
	        tp.setBounds( 0, 0, 600, 600 );
	        p.setLayout( null );
	        p.add( tp );
	        p.add(j);
	        p.setPreferredSize( new Dimension( 800, 800 ) );
	        
		    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		    JButton btnUploadFile = new JButton("Upload File");
		    JButton search = new JButton("Search");
		    btnUploadFile.setBounds(54, 557, 130, 25);
		    search.setBounds(650, 600, 130, 25);
		    btnUploadFile.addActionListener(new ActionListener() {
		      public void actionPerformed(ActionEvent ae) {
		        JFileChooser fileChooser = new JFileChooser();
		        int returnValue = fileChooser.showOpenDialog(null);
		        if (returnValue == JFileChooser.APPROVE_OPTION) {
		          File selectedFile = fileChooser.getSelectedFile();
		          //System.out.println(selectedFile.getName());
		          String cur=fileChooser.getCurrentDirectory()+"/"+selectedFile.getName();
		          try {
					upload(homeDir,"",cur);
				} catch (IOException | SAXException | TikaException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		        }
		      }
		    });
		    search.addActionListener(new ActionListener() {
			      public void actionPerformed(ActionEvent ae) {
			         {
			          
			         tp.setText("");
			        	 try {
						String txt=null;
						String toSend=j.getText();
						txt=search(toSend);
						if(txt!=null)
						{
							
							tp.getDocument().insertString(0, txt, SimpleAttributeSet.EMPTY);
						}
						else
						{
							tp.removeAll();
							tp.getDocument().insertString(0, "No match for the query", SimpleAttributeSet.EMPTY);
						}
						} catch (IOException | BadLocationException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
			        	 
			        	 
			        	 
			        }
			      }
			    });
		    
		    Button button_1 = new Button("Home");
		    button_1.setBounds(406, 93, 50, 23);
		    
		    JButton btnExtract = new JButton("Extract");
		    btnExtract.setBounds(366, 215, 117, 25);
		    
		    JButton btnDownload = new JButton("Download");
		    btnDownload.setBounds(366, 308, 117, 25);
		    DefaultListModel listModel;
		    listModel = new DefaultListModel();
		   
		    JList list = new JList(listModel);
		    JScrollPane scrollPane = new JScrollPane();
		    scrollPane.setSize(350, 450);
		    scrollPane.setLocation(10, 59);
		    list.setVisibleRowCount(8);
		    list.setBounds(12, 93, 350,350);
		    list.setValueIsAdjusting(true);
		    list.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		    scrollPane.setViewportView(list);
		    
		    
		    MouseListener mouseListener = new MouseAdapter() {
		        public void mouseClicked(MouseEvent e) {
		            if (e.getClickCount() == 2) {
                     String[] add=null;
                     
		               String selectedItem = (String) list.getSelectedValue();
		               System.out.println(selectedItem);
		               // add selectedItem to your second list.
		              try {
						add=view_dir(selectedItem);
						listModel.clear();
					} catch (FileNotFoundException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} catch (IllegalArgumentException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
		              l=add.length;
		              for (int i=0;i<l;i++)
		              {
		               listModel.addElement(add[i]);
		              }
		             }
		        }
		    };
		    list.addMouseListener(mouseListener);
		    
		    MouseListener mouseListener1 = new MouseAdapter() {
		        public void mouseClicked(MouseEvent e) {
		            if (e.getClickCount() == 1) {
                     String[] add=null;
                     
		               String selectedItem = (String) list.getSelectedValue();
		               System.out.println(selectedItem);
		               // add selectedItem to your second list.
		              try {
		            	  
						listModel.clear();
						listModel.addElement(home_dir);
					} catch (IllegalArgumentException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
		              
		             }
		        }
		    };
		    
		    MouseListener mouseListener2 = new MouseAdapter() {
		        public void mouseClicked(MouseEvent e) {
		            if (e.getClickCount() == 1) {
                     String[] add=null;
                     String path_selected = (String) list.getSelectedValue();
		             
		               System.out.println(path_selected);
		               // add selectedItem to your second list.
		              try {
		            	  copy_h_l(home_dir+"/index/index.in");
		            	  String[] file = path_selected.split("/");
		            	  int len=file.length;
		            	  String choose=file[len-1];
		            	  System.out.println("file choosen:"+choose);
		            	  BufferedReader in = new BufferedReader(new FileReader("/home/abhishek/Desktop/data/index.in"));
		                  String str;
                           textPane.setText("");
		                 String text=null;
		                  while((str = in.readLine()) != null){
		                	  String[] temp = str.split(",,");
		                	  System.out.println(temp[0]);
		                	  System.out.println(path_selected);
		                	  String h=temp[0];
		                	  if(h.compareTo(path_selected)==0)
		                	  {
		                		  System.out.println("wrong");
		                		 text= read_file(temp[1]);
		                		
		                	  }
		                  }
		                  textPane.getDocument().insertString(0, text, SimpleAttributeSet.EMPTY);
					} catch (IllegalArgumentException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} catch (BadLocationException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
		              
		             }
		        }
		    };
		    MouseListener mouseListener3 = new MouseAdapter() {
		        public void mouseClicked(MouseEvent e) {
		            if (e.getClickCount() == 1) {
                     String[] add=null;
                     String path_selected = (String) list.getSelectedValue();
		             
		               System.out.println(path_selected);
		               // add selectedItem to your second list.
		              try {
		            	  copy_h_l(path_selected);
						
					} catch (IllegalArgumentException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
		              
		             }
		        }
		    };
		    btnExtract.addMouseListener(mouseListener2);
		    btnDownload.addMouseListener(mouseListener3);
		    button_1.addMouseListener(mouseListener1);
		    frame.getContentPane().setLayout(null);
		    frame.getContentPane().add(textPane);
		    frame.getContentPane().add(button_1);
		    frame.getContentPane().add(btnUploadFile);
		    frame.getContentPane().add(btnExtract);
		    frame.getContentPane().add(btnDownload);
		    frame.getContentPane().add(scrollPane);
		    p.add(search);
		    frame1.getContentPane().add(p);
		    p.add(tp);
		    Scrollbar scrollbar = new Scrollbar();
		    scrollbar.setBounds(759, 318, 17, 50);
		    frame.getContentPane().add(scrollbar);
		    
		   
	        frame1.setVisible(true);
		    frame.setVisible(true);

		
		
		 
		listModel.addElement(home_dir);
		System.out.println("home folder -" +homeDir);
		//create_dir("index",homeDir);
		//copy_h_l(home_dir+"/index/index.in");
		//read_file("test.txt",homeDir);
		//view_dir("abhishek");
		//upload(homeDir,"" ,"Dw.xls");
		//create_file(homeDir,"index.in");
	    }
	  public static String[] view_dir(String dir) throws FileNotFoundException, IllegalArgumentException, IOException
	  {
		  FileStatus[] fileStatus = hdfs.listStatus(new Path(dir));
		  Path[] paths = FileUtil.stat2Paths(fileStatus);
		  System.out.println("***** Contents of the Directory *****");
		 
		  int i=0;
		    for(Path path : paths)
		    {
		    	
		    	i++;
		      System.out.println(path);
		    }
		    String[] str=new String[i];
		   
		   int j=0;
		    for(Path path : paths)
		    {
		    	
		    	str[j++]=""+path;
		    }
		    return str;
	  }
	  public static String get_metadata(String path,String File) throws IOException, SAXException, TikaException
	  {
		  
		  //copy_h_l(path);
		  String file=path;
		  Parser parser = new AutoDetectParser();
	      BodyContentHandler handler = new BodyContentHandler();
	      Metadata metadata = new Metadata();
	      FileInputStream inputstream = new FileInputStream(file);
	      ParseContext context = new ParseContext();
	      String loc=null;
	      OOXMLParser  msofficeparser = new OOXMLParser (); 
	     if(file.matches("(.*)xls(.*)")||file.matches("(.*)ppt(.*)")||file.matches("(.*)pptx(.*)"))
	     {
	    	 
	    	 msofficeparser.parse(inputstream, handler, metadata,context);
	    
	      String disc=handler.toString();
	      String[] metadataNames = metadata.names();
	      int length=disc.length();
	      
	      String display= disc.substring(0 , Math.min(length,200));
	      System.out.println("--------------------------------------------");
	      System.out.println("Contents of the document:" + display);
	      System.out.println("--------------------------------------------");
	      System.out.println("Metadata of the document:");
	      for(String name : metadataNames) {
		         System.out.println(name + ": " + metadata.get(name));
		         display=display+"\n"+name+":"+metadata.get(name);
		      }
	  	try {
   		 
		      File meta_temp = new File("/home/abhishek/Desktop/data/"+File+"_meta.met");
		      
		      if (meta_temp.createNewFile()){
		    	  System.out.println("--------------------------------------------");
		        System.out.println("File is created!");
		        loc="/home/abhishek/Desktop/data/"+File+"_meta.met";
		        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
		                new FileOutputStream(loc), "utf-8"))) {
		     writer.write(display);
		  }
		      }else{
		    	  System.out.println("--------------------------------------------");
		        System.out.println("File already exists.");
		        loc="/home/abhishek/Desktop/data/"+File+"_meta.met";
		        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
		                new FileOutputStream(loc), "utf-8"))) {
		     writer.write(display);
		  }
		      }
		      
	    	} catch (IOException e) {
		      e.printStackTrace();
		}
	     }
	     
	     else if(file.matches("(.*)pdf(.*)")){
	    	  //parsing the document using PDF parser
	         PDFParser pdfparser = new PDFParser(); 
	         pdfparser.parse(inputstream, handler, metadata,context);
	         
	         String disc=handler.toString();
		      String[] metadataNames = metadata.names();
		      int length=disc.length();
		      
		      String display= disc.substring(0 , Math.min(length,200));		  
		      System.out.println("--------------------------------------------");
		      System.out.println("Contents of the document:" + display);
		      System.out.println("--------------------------------------------");
		      System.out.println("Metadata of the document:");
		      for(String name : metadataNames) {
			         System.out.println(name + ": " + metadata.get(name));
			         display=display+"\n"+name+":"+metadata.get(name);			      
			         }
		      try {
		    		 
			      File meta_temp = new File("/home/abhishek/Desktop/data/"+File+"_meta.met");
			      
			      if (meta_temp.createNewFile()){
			    	  System.out.println("--------------------------------------------");
			        System.out.println("File is created!");
			        loc="/home/abhishek/Desktop/data/"+File+"_meta.met";
			        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
			                new FileOutputStream(loc), "utf-8"))) {
			     writer.write(display);
			  }
			      }else{
			    	  System.out.println("--------------------------------------------");
			        System.out.println("File already exists.");
			        loc="/home/abhishek/Desktop/data/"+File+"_meta.met";
			        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
			                new FileOutputStream(loc), "utf-8"))) {
			     writer.write(display);
			  }
			      }
			      
		    	} catch (IOException e) {
			      e.printStackTrace();
			}
		     }
	 /*    else  if(file.matches("(.*)csv(.*)")){
	    	 
	    	 Scanner scanner = new Scanner(new File(path));
	         
	         //Set the delimiter used in file
	         scanner.useDelimiter(",");
	          String display=null;
	          int i=0;
	         //Get all tokens and store them in some data structure
	         //I am just printing them
	         while (scanner.hasNext()&& i<5) 
	         {
	        	 i++;
	        
	             System.out.print(scanner.next() + "|");
	             display=display+"\n"+scanner.next()+"|";		
	         }
	          
	         //Do not forget to close the scanner  
	         scanner.close();
	    	       
	         
	         try {
	    		 
			      File meta_temp = new File("/home/abhishek/Desktop/data/"+File+"_meta.met");
			      
			      if (meta_temp.createNewFile()){
			    	  System.out.println("--------------------------------------------");
			        System.out.println("File is created!");
			        loc="/home/abhishek/Desktop/data/"+File+"_meta.met";
			        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
			                new FileOutputStream(loc), "utf-8"))) {
			     writer.write(display);
			  }
			      }else{
			    	  System.out.println("--------------------------------------------");
			        System.out.println("File already exists.");
			        loc="/home/abhishek/Desktop/data/"+File+"_meta.met";
			        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
			                new FileOutputStream(loc), "utf-8"))) {
			     writer.write(display);
			  }
			      }
			      
		    	} catch (IOException e) {
			      e.printStackTrace();
			}   
	    	    
		     
	     }*/
	     else     if(file.matches("(.*)ods(.*)")){
	    	 OpenDocumentParser openofficeparser = new OpenDocumentParser (); 
	         openofficeparser.parse(inputstream, handler, metadata,context); 
	         String disc=handler.toString();
		      String[] metadataNames = metadata.names();
		      int length=disc.length();
		      
		      String display= disc.substring(0 , Math.min(length,200));		   
		      System.out.println("--------------------------------------------");
		      System.out.println("Contents of the document:" + display);
		      System.out.println("--------------------------------------------");
		      System.out.println("Metadata of the document:");
		      for(String name : metadataNames) {
			         System.out.println(name + ": " + metadata.get(name));
			         display=display+"\n"+name+":"+metadata.get(name);			      }
		      try {
		    		 
			      File meta_temp = new File("/home/abhishek/Desktop/data/"+File+"_meta.met");
			      
			      if (meta_temp.createNewFile()){
			    	  System.out.println("--------------------------------------------");
			        System.out.println("File is created!");
			        loc="/home/abhishek/Desktop/data/"+File+"_meta.met";
			        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
			                new FileOutputStream(loc), "utf-8"))) {
			     writer.write(display);
			  }
			      }else{
			    	  System.out.println("--------------------------------------------");
			        System.out.println("File already exists.");
			        loc="/home/abhishek/Desktop/data/"+File+"_meta.met";
			        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
			                new FileOutputStream(loc), "utf-8"))) {
			     writer.write(display);
			  }
			      }
			      
		    	} catch (IOException e) {
			      e.printStackTrace();
			}
		     }
	     else  if(file.matches("(.*)html(.*)")){
	    	 //Html parser 
	         HtmlParser htmlparser = new HtmlParser();
	         htmlparser.parse(inputstream, handler, metadata,context);
	         String disc=handler.toString();
		      String[] metadataNames = metadata.names();
		      int length=disc.length();
		      
		      String display= disc.substring(0 , Math.min(length,200));
		      System.out.println("--------------------------------------------");
		      System.out.println("Contents of the document:" + display);
		      System.out.println("--------------------------------------------");
		      System.out.println("Metadata of the document:");
		      for(String name : metadataNames) {
			         System.out.println(name + ": " + metadata.get(name));
			         display=display+"\n"+name+":"+metadata.get(name);			      }
		      try {
		    		 
			      File meta_temp = new File("/home/abhishek/Desktop/data/"+File+"_meta.met");
			      
			      if (meta_temp.createNewFile()){
			    	  System.out.println("--------------------------------------------");
			        System.out.println("File is created!");
			        loc="/home/abhishek/Desktop/data/"+File+"_meta.met";
			        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
			                new FileOutputStream(loc), "utf-8"))) {
			     writer.write(display);
			  }
			      }else{
			    	  System.out.println("--------------------------------------------");
			        System.out.println("File already exists.");
			        loc="/home/abhishek/Desktop/data/"+File+"_meta.met";
			        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
			                new FileOutputStream(loc), "utf-8"))) {
			     writer.write(display);
			  }
			      }
			      
		    	} catch (IOException e) {
			      e.printStackTrace();
			}
	     }
	     else  if(file.matches("(.*)xml(.*)")){
	    	 //Xml parser
	         XMLParser xmlparser = new XMLParser(); 
	         xmlparser.parse(inputstream, handler, metadata, context);
	         String disc=handler.toString();
		      String[] metadataNames = metadata.names();
		      int length=disc.length();
		      
		      String display= disc.substring(0 , Math.min(length,200));
		      System.out.println("--------------------------------------------");
		      System.out.println("Contents of the document:" + display);
		      System.out.println("--------------------------------------------");
		      System.out.println("Metadata of the document:");
		      for(String name : metadataNames) {
			         System.out.println(name + ": " + metadata.get(name));
			         display=display+"\n"+name+":"+metadata.get(name);			      
			         }
		      try {
		    		 
			      File meta_temp = new File("/home/abhishek/Desktop/data/"+File+"_meta.met");
			      
			      if (meta_temp.createNewFile()){
			    	  System.out.println("--------------------------------------------");
			        System.out.println("File is created!");
			        loc="/home/abhishek/Desktop/data/"+File+"_meta.met";
			        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
			                new FileOutputStream(loc), "utf-8"))) {
			     writer.write(display);
			  }
			      }else{
			    	  System.out.println("--------------------------------------------");
			        System.out.println("File already exists.");
			        loc="/home/abhishek/Desktop/data/"+File+"_meta.met";
			        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
			                new FileOutputStream(loc), "utf-8"))) {
			     writer.write(display);
			  }
			      }
			      
		    	} catch (IOException e) {
			      e.printStackTrace();
			}
		     }
	     else    if(file.matches("(.*)txt(.*)")){
		     
		      TXTParser  TexTParser = new TXTParser();
		      TexTParser.parse(inputstream, handler, metadata,context);
		      String disc=handler.toString();
		      String[] metadataNames = metadata.names();
		      int length=disc.length();
		      
		      String display= disc.substring(0 , Math.min(length,200));
		      System.out.println("--------------------------------------------");
		      System.out.println("Contents of the document:" + display);
		      System.out.println("--------------------------------------------");
		      System.out.println("Metadata of the document:");
		      for(String name : metadataNames) {
			         System.out.println(name + ": " + metadata.get(name));
			         display=display+"\n"+name+":"+metadata.get(name);			      
			         }
		      try {
		    		 
			      File meta_temp = new File("/home/abhishek/Desktop/data/"+File+"_meta.met");
			      
			      if (meta_temp.createNewFile()){
			    	  System.out.println("--------------------------------------------");
			        System.out.println("File is created!");
			        loc="/home/abhishek/Desktop/data/"+File+"_meta.met";
			        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
			                new FileOutputStream(loc), "utf-8"))) {
			     writer.write(display);
			  }
			      }else{
			    	  System.out.println("--------------------------------------------");
			        System.out.println("File already exists.");
			        loc="/home/abhishek/Desktop/data/"+File+"_meta.met";
			        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
			                new FileOutputStream(loc), "utf-8"))) {
			     writer.write(display);
			  }
			      }
			      
		    	} catch (IOException e) {
			      e.printStackTrace();
			}
		     }
	     else{
		     
		      Parser  TexTParser = new TXTParser();
		      TexTParser.parse(inputstream, handler, metadata,context);
		      String disc=handler.toString();
		      String[] metadataNames = metadata.names();
		      int length=disc.length();
		      
		      String display= disc.substring(0 , Math.min(length,200));
		      System.out.println("--------------------------------------------");
		      System.out.println("Contents of the document:" + display);
		      System.out.println("--------------------------------------------");
		      System.out.println("Metadata of the document:");
		      for(String name : metadataNames) {
			         System.out.println(name + ": " + metadata.get(name));
			         display=display+"\n"+name+":"+metadata.get(name);			      
			         }
		      try {
		    		 
			      File meta_temp = new File("/home/abhishek/Desktop/data/"+File+"_meta.met");
			      
			      if (meta_temp.createNewFile()){
			    	  System.out.println("--------------------------------------------");
			        System.out.println("File is created!");
			        loc="/home/abhishek/Desktop/data/"+File+"_meta.met";
			        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
			                new FileOutputStream(loc), "utf-8"))) {
			     writer.write(display);
			  }
			      }else{
			    	  System.out.println("--------------------------------------------");
			        System.out.println("File already exists.");
			        loc="/home/abhishek/Desktop/data/"+File+"_meta.met";
			        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
			                new FileOutputStream(loc), "utf-8"))) {
			     writer.write(display);
			  }
			      }
			      
		    	} catch (IOException e) {
			      e.printStackTrace();
			}
		     } 
		return loc;
	 	
	  }
	  public static void upload(Path homeDir,String File_c ,String File) throws IOException, SAXException, TikaException
	  {
		 
		  Path localFilePath = new Path(File);
		  String[] file1 = File.split("/");
    	  int len=file1.length;
    	  String choose=file1[len-1];
		 String temp= get_metadata(File,choose);
		 Path temp_local=new Path(temp);
		  Path hdfsFilePath=new Path((homeDir)+"/"+File_c);
		  Path metafile=new Path((homeDir)+"/meta/"+File_c);
		  Path index=new Path((homeDir)+"/index/"+File_c);
		  Path index_local=new Path("/home/abhishek/Desktop/data/index.in");
		  System.out.println("--------------------------------------------");
		  System.out.println("copied from local directory to HDFS");
			  hdfs.copyFromLocalFile(localFilePath, hdfsFilePath);
			  System.out.println("creating the entry in the index");
			  try {
		    		 
				
				  FileWriter fw = new FileWriter("/home/abhishek/Desktop/data/index.in",true); //the true will append the new data
				    fw.write((homeDir)+"/"+choose+",,"+(homeDir)+"/meta/"+choose+"_meta.met"+"\n");//appends the string to the file
				    fw.close();
			      
		    	} catch (IOException e) {
			      e.printStackTrace();
			}
			
			  hdfs.copyFromLocalFile(temp_local, metafile);
			  java.nio.file.Path p= Paths.get("/home/abhishek/Desktop/data/.index.in.crc");
			  Files.deleteIfExists(p);
			  hdfs.copyFromLocalFile(index_local, index);
			  System.out.println("updated the index in the hdfs");
			  System.out.println("--------------------------------------------");
	  }
	  public static void copy_h_l(String path) throws IOException
	  {
		  
		 Path  localFilePath=new Path("/home/abhishek/Desktop/data/");
		  Path hdfsFilePath=new Path(path);
		  hdfs.copyToLocalFile(hdfsFilePath, localFilePath);
		  System.out.println("copied from HDFS to local directory");
	  }
	  public static void create_file(Path homeDir,String Filename) throws IOException
	  {
		  Path newFilePath=new Path(homeDir+"/index/"+Filename);
		  hdfs.createNewFile(newFilePath);
		  System.out.println("file created...");
	  }
	  public static String read_file(String Filename) throws IOException
	  {
		  
		  
		  
		  Path filepath=new Path(Filename);
		  ContentSummary summary=hdfs.getContentSummary(filepath);
		  
		  BufferedReader bfr=new BufferedReader(new InputStreamReader(hdfs.open(filepath)));
		  String str =null;
		  String str1 =null;
		  while ((str = bfr.readLine())!= null)

		  {
             str1=str1+"\n"+str;   
		  System.out.println(str);

		  }
		  return str1;
	  }
	  public static void create_dir(String Dir,Path workingDir) throws IOException
	  {
		  //Path workingDir=hdfs.getWorkingDirectory();
		  Path newFolderPath= new Path(Dir);
		  String p2=String.valueOf(newFolderPath);
		  String p1=String.valueOf(workingDir);
		  String p1_p2=p1+"/"+p2;
		 // System.out.println(p1_p2);
		  Path newFolderPath_merge =new Path(p1_p2);
         
		  if(hdfs.exists(newFolderPath_merge))

		  {

		  hdfs.delete(newFolderPath_merge, true); //Delete existing Directory

		  }

		  hdfs.mkdirs(newFolderPath_merge);     //Create new Directory
		  System.out.println("Directory created..");
	  }
	  public static  void read_metadata(String File,String Home)
	  {
		  
		  homeDir=hdfs.getHomeDirectory();
		  
		  
		  
		  
		  
		  
		  
		  
	  }
	  
	private static void addPopup(Component component, final JPopupMenu popup) {
		component.addMouseListener(new MouseAdapter() {
			public void mousePressed(MouseEvent e) {
				if (e.isPopupTrigger()) {
					showMenu(e);
				}
			}
			public void mouseReleased(MouseEvent e) {
				if (e.isPopupTrigger()) {
					showMenu(e);
				}
			}
			private void showMenu(MouseEvent e) {
				popup.show(e.getComponent(), e.getX(), e.getY());
			}
		});
	}
	public static String search(String search) throws IOException
	{
		copy_h_l(home_dir+"/index/index.in");
		  BufferedReader in = new BufferedReader(new FileReader("/home/abhishek/Desktop/data/index.in"));
          String str;
          String toDisplay=null;
          String toPrint=null;
         String text=null;
          while((str = in.readLine()) != null){
        	  if (str.matches("(?i:.*"+search+".*)"))
        	  {
        		  String[] temp = str.split(",,");
        		  String[] term =temp[0].split("/");
        		  int len=term.length;
        		  String t=term[len-1];
        		  //System.out.println(t);
        		  toDisplay=toDisplay+t+" file at : "+temp[0]+"\n";
        		  toPrint=t+" file at : "+temp[0];
        		  System.out.println(toPrint);
        		  
        	  }
        	  
        	 
          }
		
		return toDisplay;
		
	}
}
