package edu.upi.cs.tweetmining;

//BETA BETA BETA DO NOT USE

public class SentimentAnalysis {
	
  public void processBeta() {	
	
	/*   input:   file tweet yang sudah diprepro+synStopWord  (harusnya nanti bisa tweet mentah)
	 *   output:     
	 *     
	 *   
	 *   
	 *   
	 */
		
		//diklasifikasikan ke opini atau bukan
		//bagian yang opini diklasifikasi lagi menjadi opini positif dan negatif
		//untuk opini negatif dan positif --> cluster dan ambil label-nya (berapa K?)
		//proses tfidfnya
		
	  //G:\\eksperimen\\corpus_tweet_opini\\perhari\\jadi\\7_prepro_nots_preproSyn_classresult_siap_opini_2_classresult_negatif_siap_opini2.txt
	    
//		TFIDF t = new TFIDF();
//	    t.process("G:\\eksperimen\\corpus_tweet_opini\\perhari\\jadi\\7_prepro_nots_preproSyn_classresult_siap_opini_2_classresult_negatif_siap_opini2.txt","G:\\eksperimen\\corpus_tweet_opini\\gabungan\\catatan_stopwords_weight.txt");
//	    t.stat("G:\\eksperimen\\corpus_tweet_opini\\perhari\\jadi\\7_prepro_nots_preproSyn_classresult_siap_opini_2_classresult_negatif_siap_opini2_tfidf.txt");
//		
	  
		//proses opini negatif
	    //KMeansClustering km = new KMeansClustering();
	    //km.process("G:\\eksperimen\\corpus_tweet_opini\\gabungan\\positif_siap_opini2_telkomsel_tfidf.txt","G:\\eksperimen\\corpus_tweet_opini\\gabungan\\positif_siap_opini2_telkomsel.txt",5);
		
	}
  
    public void processMulti(String dirInput) {
    	
    	int numOfFile=27;  //testing dulu
    	
    	String fileSyn= "G:\\eksperimen\\corpus_tweet_opini\\catatan_kata_sinonim.txt";
        String fileStopWords = "G:\\eksperimen\\corpus_tweet_opini\\catatan_stopwords.txt";
    	for (int i=1; i<=numOfFile; i++) {
    		//prepro dasar
    		GenericPrepro gp = new GenericPrepro();
    	    gp.process(dirInput+i+".txt");  //output : 0_prepro_nots.txt
    	    
    	    //hilangkan stopword dan apply sinonim
    	    PreproSynStopWord ps = new PreproSynStopWord();
    	    ps.process(fileSyn,fileStopWords,dirInput+i+"_prepro_nots.txt");  //otuput: //0_prepro_nots_preproSyn
    	    
    	    //klasifikasi apakah dia opini atau bukan
    	    NaiveBayes nb = new NaiveBayes();   
    	    nb.classify("G:\\eksperimen\\corpus_tweet_opini\\model_opini_2.txt", dirInput+i+"_prepro_nots_preproSyn.txt",dirInput);
    	    //output: 1_prepro_nots_preproSyn_classresult_siap_opini_2.txt  
    	    
    	    //klasisifikasi berdasarkan opini positif dan negatif
    	    NaiveBayes nb2 = new NaiveBayes();   
    	    nb2.classify("G:\\eksperimen\\corpus_tweet_opini\\model_pos_neg_opini_2.txt", dirInput+i+"_prepro_nots_preproSyn_classresult_siap_opini_2.txt",dirInput);
    	}
    	
    }
   
    public static void main(String[] args) {
		//testing
		SentimentAnalysis sa = new SentimentAnalysis(); 
		sa.processBeta();
		//sa.processMulti("G:\\eksperimen\\corpus_tweet_opini\\perhari\\");
 }
	
	
	
	 
}
