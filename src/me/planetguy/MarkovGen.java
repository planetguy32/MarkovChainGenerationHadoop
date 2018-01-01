package me.planetguy;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MarkovGen {
	
	private static class Prob{
		public final String key;
		public final double probability;
		public Prob(String key, double probability){
			this.key=key;
			this.probability=probability;
		}
	}
	
	private static class Element implements Comparable<Element>{
		public final String key;
		public final List<Prob> followers;
		public Element(String[] line){
			key=line[0];
			followers=new ArrayList<>();
			for(int i=1; i<line.length; i++){
				String[] parts=line[i].split("=");
				followers.add(new Prob(parts[0], Double.parseDouble(parts[1])));
			}
		}
		@Override
		public int compareTo(Element other) {
			return key.compareTo(other.key);
		}
	}
	
	public static void main(String[] args) throws Exception{
		File chain=new File(args[0]);
		int wordCount=Integer.parseInt(args[1]);
		
		BufferedReader reader=new BufferedReader(new FileReader(chain));
		Element[] lines=reader.lines()				
				//TODO AARRRRGH IT BurNS  MY EYES!!1!!
				.map(new Function<String, Element>(){

					@Override
					public <V> Function<String, V> andThen(
							Function<? super Element, ? extends V> arg0) {
						return null;
					}

					@Override
					public Element apply(String line) {
						return new Element(line.split("\t"));
					}

					@Override
					public <V> Function<V, Element> compose(
							Function<? super V, ? extends String> arg0) {
						return null;
					}
				})
				
				.sorted()
				
				//TODO is there a way to collect directly to an array?
				.collect(Collectors.toList())
				.toArray(new Element[0]);
		reader.close();
		//TODO this is because java.util.Random is a turd
		Random rand=new Random();
		Element word=lines[rand.nextInt(lines.length)];
		top:
		for(int i=0; i<wordCount; i++){
			System.out.print(word.key);
			System.out.print(" ");
			double d=rand.nextDouble();
			for(Prob p:word.followers) {
				d -= p.probability;
				if(d < 0) {
					word=getElement(p.key, lines);
					continue top;
				}
			}
		}
	}

	private static Element getElement(String key, Element[] lines) {
		Element dummy=new Element(new String[]{key});
		int index=Arrays.binarySearch(lines, dummy);
		return lines[index];
	}
	
	
}
